use anyhow::{anyhow, Result};
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::process::Command;
use tokio::time::{sleep, Duration, Instant};
use regex::Regex;
use chrono;

use crate::global::structs::{AiProgress, SHARED_AI_PROGRESS, SELECTED_SUBTITLE};

const CACHE_DIR: &str = "~/.cache/walker-yt-rs"; 
const DEMUCS_BIN: &str = "~/.local/share/walker-yt/venv/bin/demucs";
const YT_DLP_BIN: &str = "~/.local/bin/yt-dlp";

fn expand_path(path: &str) -> PathBuf {
    PathBuf::from(shellexpand::tilde(path).into_owned())
}

pub fn log(message: &str) {
    if let Ok(mut f) = fs::OpenOptions::new().create(true).append(true).open("/tmp/walker-yt.log") {
        let timestamp = chrono::Local::now().format("%H:%M:%S");
        let _ = writeln!(f, "[{}] {}", timestamp, message);
    }
}

pub async fn get_subtitles(video_id: &str) -> Result<Vec<String>> {
    let yt_dlp = expand_path(YT_DLP_BIN);
    let output = Command::new(yt_dlp)
        .args([
            "--list-subs",
            "--quiet",
            &format!("https://www.youtube.com/watch?v={}", video_id),
        ])
        .output()
        .await?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut subs = Vec::new();
    let mut start_parsing = false;

    for line in stdout.lines() {
        if (line.contains("Available subtitles") || line.contains("Available automatic captions")) && line.contains("Language") {
            start_parsing = true;
            continue;
        }
        if line.contains("Available") && !line.contains("Language") {
            start_parsing = false;
        }
        if start_parsing && !line.trim().is_empty() && !line.contains("Language Name Formats") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 {
                let code = parts[0];
                let name = parts[1..parts.len() - 1].join(" ");
                let name = if name.is_empty() { code } else { &name };
                subs.push(format!("{} ({})", name, code));
            }
        }
    }
    
    subs.sort();
    subs.dedup();
    Ok(subs)
}

async fn walker_dmenu(prompt: &str, lines: Vec<String>) -> Result<String> {
    let mut child = Command::new("walker")
        .args(["-d", "-p", prompt])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;

    let mut stdin = child.stdin.take().ok_or_else(|| anyhow!("Failed to open stdin"))?;
    stdin.write_all(lines.join("\n").as_bytes()).await?;
    drop(stdin);

    let output = child.wait_with_output().await?;
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

pub async fn select_subtitles(video_id: &str) -> Result<Option<String>> {
    let mut subs = get_subtitles(video_id).await?;
    
    if subs.is_empty() {
        return Ok(None);
    }

    subs.insert(0, "🚫 None".to_string());
    let selection = walker_dmenu("Select Subtitles", subs).await?;
    
    if selection.is_empty() || selection.contains("None") {
        *SELECTED_SUBTITLE.lock().unwrap() = None;
        return Ok(None);
    }

    let re = Regex::new(r"\((.*?)\)$").unwrap();
    if let Some(caps) = re.captures(&selection) {
        let code = caps[1].to_string();
        *SELECTED_SUBTITLE.lock().unwrap() = Some(code.clone());
        return Ok(Some(code));
    }
    Ok(None)
}

pub async fn start_ai_separation(video_id: String, mode: String) -> Result<u16> {
    let cache_dir = expand_path(CACHE_DIR);
    let work_dir = cache_dir.join(format!("proc_{}", video_id));
    fs::create_dir_all(&work_dir)?;

    let audio_path = work_dir.join("input.m4a");
    let chunks_dir = work_dir.join("chunks");
    let out_chunks_dir = work_dir.join("out_chunks");
    let playback_file = work_dir.join("live_audio.pcm");

    if playback_file.exists() { fs::remove_file(&playback_file)?; }
    fs::create_dir_all(&chunks_dir)?;
    fs::create_dir_all(&out_chunks_dir)?;

    {
        let mut progress = SHARED_AI_PROGRESS.lock().unwrap();
        progress.active = true;
        progress.label = "Downloading audio...".to_string();
        progress.current_chunk = 0;
        progress.total_chunks = 0;
        progress.eta_seconds = None;
    }

    if !audio_path.exists() {
        let status = Command::new(expand_path(YT_DLP_BIN))
            .args(["-f", "bestaudio[ext=m4a]/bestaudio", "-o", audio_path.to_str().unwrap(), "--no-playlist", "--", &video_id])
            .status()
            .await?;
        if !status.success() { return Err(anyhow!("Download failed")); }
    }

    {
        let mut progress = SHARED_AI_PROGRESS.lock().unwrap();
        progress.label = "Splitting into chunks...".to_string();
    }

    let split_status = Command::new("ffmpeg")
        .args(["-y", "-i", audio_path.to_str().unwrap(), "-f", "segment", "-segment_time", "30", "-c", "copy", chunks_dir.join("chunk_%03d.m4a").to_str().unwrap()])
        .status()
        .await?;
    if !split_status.success() { return Err(anyhow!("FFmpeg split failed")); }

    let mut entries: Vec<_> = fs::read_dir(&chunks_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().map_or(false, |ext| ext == "m4a"))
        .collect();
    entries.sort();

    let total = entries.len();
    let playback_file_clone = playback_file.clone();
    let demucs_bin = expand_path(DEMUCS_BIN);
    let out_chunks_dir_clone = out_chunks_dir.clone();

    tokio::spawn(async move {
        let start_time = Instant::now();
        for (i, chunk_path) in entries.iter().enumerate() {
            let chunk_name = chunk_path.file_stem().unwrap().to_str().unwrap();
            let stem_name = if mode == "vocals" { "vocals.wav" } else { "no_vocals.wav" };
            let separated_wav = out_chunks_dir_clone.join("htdemucs").join(chunk_name).join(stem_name);

            if !separated_wav.exists() {
                let _ = Command::new("systemd-run")
                    .args([
                        "--user", "--scope", "-p", "MemoryMax=10G", "-p", "CPUQuota=400%",
                        "-E", "OMP_NUM_THREADS=8", "-E", "MKL_NUM_THREADS=8",
                        "-E", "OPENBLAS_NUM_THREADS=8", "-E", "VECLIB_MAXIMUM_THREADS=8",
                        demucs_bin.to_str().unwrap(), "-n", "htdemucs", "--two-stems=vocals",
                        "--segment", "7", "--shifts", "0", "--overlap", "0.1", "-d", "cpu", "-j", "1",
                        "-o", out_chunks_dir_clone.to_str().unwrap(), chunk_path.to_str().unwrap()
                    ])
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status()
                    .await;
            }

            if separated_wav.exists() {
                let output = Command::new("ffmpeg")
                    .args(["-y", "-i", separated_wav.to_str().unwrap(), "-f", "s16le", "-acodec", "pcm_s16le", "-ar", "44100", "-ac", "2", "-"])
                    .output()
                    .await;

                if let Ok(output) = output {
                    if let Ok(mut f) = fs::OpenOptions::new().create(true).append(true).open(&playback_file_clone) {
                        let _ = f.write_all(&output.stdout);
                        let _ = f.sync_all();
                    }
                }
            }

            let elapsed = start_time.elapsed().as_secs();
            let avg_time_per_chunk = elapsed as f32 / (i + 1) as f32;
            let remaining_chunks = (total - (i + 1)) as f32;
            let eta = (avg_time_per_chunk * remaining_chunks) as u64;

            {
                let mut progress = SHARED_AI_PROGRESS.lock().unwrap();
                progress.current_chunk = i + 1;
                progress.total_chunks = total;
                progress.eta_seconds = Some(eta);
                progress.label = format!("Separating: Chunk {}/{}", i + 1, total);
            }
        }
        
        {
            let mut progress = SHARED_AI_PROGRESS.lock().unwrap();
            progress.label = "AI Separation Finished".to_string();
            progress.active = false;
        }
    });

    let start_wait = Instant::now();
    loop {
        if playback_file.exists() && fs::metadata(&playback_file)?.len() >= 10_000_000 { break; }
        if start_wait.elapsed() > Duration::from_secs(240) {
            if playback_file.exists() && fs::metadata(&playback_file)?.len() > 0 { break; }
            return Err(anyhow!("Timeout waiting for audio buffer"));
        }
        sleep(Duration::from_secs(1)).await;
    }

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    
    let playback_file_stream = playback_file.clone();
    tokio::spawn(async move {
        if let Ok((mut stream, _)) = listener.accept().await {
            if let Ok(mut f) = fs::File::open(&playback_file_stream) {
                let mut buffer = [0; 16384];
                loop {
                    match f.read(&mut buffer) {
                        Ok(0) => {
                            sleep(Duration::from_millis(500)).await;
                        }
                        Ok(n) => {
                            if stream.write_all(&buffer[..n]).await.is_err() { break; }
                        }
                        Err(_) => break,
                    }
                }
            }
        }
    });

    Ok(port)
}
