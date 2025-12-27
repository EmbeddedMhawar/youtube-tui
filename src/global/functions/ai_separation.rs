use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::fs;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::Stdio;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::process::Command;
use tokio::time::{sleep, Duration, Instant};
use regex::Regex;
use chrono;

use crate::global::structs::{SHARED_AI_PROGRESS, SELECTED_SUBTITLE, SELECTED_QUALITY};

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

pub fn notify(title: &str, body: &str, urgency: &str) {
    let _ = std::process::Command::new("notify-send")
        .args(["-u", urgency, title, body, "-h", "string:x-canonical-private-synchronous:walker-yt"])
        .spawn();
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
        if line.contains("Language") && line.contains("Name") && line.contains("Formats") {
            start_parsing = true;
            continue;
        }
        if start_parsing && line.is_empty() {
            start_parsing = false;
        }
        if start_parsing {
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

pub async fn get_video_qualities(video_id: &str) -> Result<Vec<String>> {
    let yt_dlp = expand_path(YT_DLP_BIN);
    let output = Command::new(yt_dlp)
        .args([
            "--dump-json",
            "--no-playlist",
            &format!("https://www.youtube.com/watch?v={}", video_id),
        ])
        .output()
        .await?;

    let data: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let formats = data["formats"].as_array().ok_or_else(|| anyhow!("No formats found"))?;
    
    let mut quality_map: HashMap<u64, Vec<u64>> = HashMap::new();
    for f in formats {
        if let (Some(h), Some(vcodec)) = (f["height"].as_u64(), f["vcodec"].as_str()) {
            if vcodec != "none" {
                let fps = f["fps"].as_u64().unwrap_or(0);
                quality_map.entry(h).or_default().push(fps);
            }
        }
    }

    let mut options = Vec::new();
    let mut heights: Vec<_> = quality_map.keys().collect();
    heights.sort_by(|a, b| b.cmp(a));

    for h in heights {
        let mut fps_list = quality_map[h].clone();
        fps_list.sort_by(|a, b| b.cmp(a));
        fps_list.dedup();
        for fps in fps_list {
            if fps >= 50 || (fps > 0 && quality_map[h].len() == 1) {
                options.push(format!("{}p{}", h, fps));
            } else if fps == 30 && !quality_map[h].contains(&60) {
                options.push(format!("{}p{}", h, fps));
            } else if fps == 0 {
                options.push(format!("{}p", h));
            }
        }
    }
    Ok(options)
}

pub async fn select_quality(video_id: &str) -> Result<Option<String>> {
    notify("Quality", "Fetching resolutions...", "low");
    let qualities = get_video_qualities(video_id).await?;
    if qualities.is_empty() { return Ok(None); }

    let selection = walker_dmenu("Select Quality", qualities).await?;
    if selection.is_empty() { return Ok(None); }

    let re = Regex::new(r"(\d+)p(\d+)?").unwrap();
    if let Some(caps) = re.captures(&selection) {
        let h = &caps[1];
        let val = if let Some(f) = caps.get(2) {
            format!("bestvideo[height<={}][fps<={}]", h, f.as_str())
        } else {
            format!("bestvideo[height<={}]", h)
        };
        if let Ok(mut guard) = SELECTED_QUALITY.lock() {
            *guard = Some(val.clone());
        }
        return Ok(Some(val));
    }
    Ok(None)
}

pub async fn select_subtitles(video_id: &str) -> Result<Option<String>> {
    let mut subs = get_subtitles(video_id).await?;
    
    if subs.is_empty() {
        return Ok(None);
    }

    subs.insert(0, "🚫 None".to_string());
    let selection = walker_dmenu("Select Subtitles", subs.clone()).await?;
    
    if selection.is_empty() || selection.contains("None") {
        if let Ok(mut guard) = SELECTED_SUBTITLE.lock() {
            *guard = None;
        }
        return Ok(None);
    }

    let re = Regex::new(r"\((.*?)\)$").unwrap();
    if let Some(caps) = re.captures(&selection) {
        let code = caps[1].to_string();
        if let Ok(mut guard) = SELECTED_SUBTITLE.lock() {
            *guard = Some(code.clone());
        }
        return Ok(Some(code));
    }
    Ok(None)
}

pub async fn get_video_metadata_yt_dlp(video_id: &str) -> Result<serde_json::Value> {
    let yt_dlp = expand_path(YT_DLP_BIN);
    let output = Command::new(yt_dlp)
        .args([
            "--dump-json",
            "--no-playlist",
            "--",
            video_id,
        ])
        .output()
        .await?;

    if !output.status.success() {
        return Err(anyhow!("yt-dlp metadata extraction failed"));
    }

    let data: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    Ok(data)
}

pub fn reset_ai_progress() {
    if let Ok(mut progress) = SHARED_AI_PROGRESS.lock() {
        progress.active = false;
        progress.label = String::new();
        progress.current_chunk = 0;
        progress.total_chunks = 0;
        progress.eta_seconds = None;
    }
}

pub async fn start_ai_separation(video_id: String, mode: String) -> Result<u16> {
    log(&format!("AI: Starting separation for {} (mode: {})", video_id, mode));
    reset_ai_progress(); // Ensure clean start
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
        if let Ok(mut progress) = SHARED_AI_PROGRESS.lock() {
            progress.active = true;
            progress.label = "Downloading audio...".to_string();
            progress.current_chunk = 0;
            progress.total_chunks = 0;
            progress.eta_seconds = None;
        }
    }

    if !audio_path.exists() {
        log("AI: Downloading audio file...");
        let status = Command::new(expand_path(YT_DLP_BIN))
            .args(["--quiet", "--no-progress", "-f", "bestaudio[ext=m4a]/bestaudio", "-o", audio_path.to_str().unwrap(), "--no-playlist", "--", &video_id])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await?;
        if !status.success() { 
            log("AI: Download FAILED");
            return Err(anyhow!("Download failed")); 
        }
    } else {
        log("AI: Audio file already cached.");
    }

    {
        if let Ok(mut progress) = SHARED_AI_PROGRESS.lock() {
            progress.label = "Wait for Playback...".to_string();
        }
    }

    log("AI: Splitting audio into 30s chunks...");
    let split_status = Command::new("ffmpeg")
        .args(["-y", "-hide_banner", "-loglevel", "error", "-i", audio_path.to_str().unwrap(), "-f", "segment", "-segment_time", "30", "-c", "copy", chunks_dir.join("chunk_%03d.m4a").to_str().unwrap()])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await?;
    if !split_status.success() { 
        log("AI: FFmpeg split FAILED");
        return Err(anyhow!("FFmpeg split failed")); 
    }

    let mut entries: Vec<_> = fs::read_dir(&chunks_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().map_or(false, |ext| ext == "m4a"))
        .collect();
    entries.sort();

    let total = entries.len();
    log(&format!("AI: Found {} chunks to process.", total));
    let playback_file_clone = playback_file.clone();
    let demucs_bin = expand_path(DEMUCS_BIN);
    let out_chunks_dir_clone = out_chunks_dir.clone();

    tokio::spawn(async move {
        log("WORKER: Started");
        let start_time = Instant::now();
        for (i, chunk_path) in entries.iter().enumerate() {
            let chunk_name = chunk_path.file_stem().unwrap().to_str().unwrap();
            let stem_name = if mode == "vocals" { "vocals.wav" } else { "no_vocals.wav" };
            let separated_wav = out_chunks_dir_clone.join("htdemucs").join(chunk_name).join(stem_name);

            if !separated_wav.exists() {
                log(&format!("WORKER: Separating chunk {}/{} ({})...", i+1, total, chunk_name));
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
                // log(&format!("WORKER: Converting chunk {} to PCM...", i+1));
                let output = Command::new("ffmpeg")
                    .args(["-y", "-hide_banner", "-loglevel", "error", "-i", separated_wav.to_str().unwrap(), "-f", "s16le", "-acodec", "pcm_s16le", "-ar", "44100", "-ac", "2", "-"])
                    .stdin(Stdio::null())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::null())
                    .output()
                    .await;


                if let Ok(output) = output {
                    if let Ok(mut f) = fs::OpenOptions::new().create(true).append(true).open(&playback_file_clone) {
                        let _ = f.write_all(&output.stdout);
                        let _ = f.sync_all();
                    }
                }
            }

            // Update Progress & Time Prediction
            let elapsed = start_time.elapsed().as_secs_f32();
            let avg_time_per_chunk = elapsed / (i + 1) as f32;
            let ratio = 30.0 / avg_time_per_chunk;
            
            let (label, eta) = if i + 1 < 2 {
                let remaining_for_buffer = (2 - (i + 1)) as f32;
                ("Wait for Playback...".to_string(), (avg_time_per_chunk * remaining_for_buffer) as u64)
            } else {
                let remaining_chunks = (total - (i + 1)) as f32;
                ("Separating...".to_string(), (avg_time_per_chunk * remaining_chunks) as u64)
            };

            {
                if let Ok(mut progress) = SHARED_AI_PROGRESS.lock() {
                    progress.current_chunk = i + 1;
                    progress.total_chunks = total;
                    progress.eta_seconds = Some(eta);
                    progress.ratio = Some(ratio);
                    progress.label = label;
                }
            }
        }
        
        reset_ai_progress();
        log("WORKER: Finished");
    });

    log("AI: Waiting for initial audio buffer (2 chunks)...");
    let start_wait = Instant::now();
    loop {
        if playback_file.exists() && fs::metadata(&playback_file)?.len() >= 10_000_000 { 
            log("AI: Initial buffer READY.");
            break; 
        }
        if start_wait.elapsed() > Duration::from_secs(240) {
            log("AI: BUFFER TIMEOUT.");
            if playback_file.exists() && fs::metadata(&playback_file)?.len() > 0 { 
                log("AI: Buffer timeout reached but some data available, proceeding anyway.");
                break; 
            }
            return Err(anyhow!("Timeout waiting for audio buffer"));
        }
        sleep(Duration::from_secs(1)).await;
    }

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    
    let playback_file_stream = playback_file.clone();
    tokio::spawn(async move {
        log(&format!("TCP: Server started on port {}", port));
        if let Ok((mut stream, _)) = listener.accept().await {
            log("TCP: Player connected.");
            if let Ok(mut f) = fs::File::open(&playback_file_stream) {
                let mut buffer = [0; 16384];
                loop {
                    let n = f.read(&mut buffer).unwrap_or(0);
                    if n > 0 {
                        if stream.write_all(&buffer[..n]).await.is_err() { break; }
                    } else {
                        sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        }
        log("TCP: Server closed.");
    });

    Ok(port)
}
