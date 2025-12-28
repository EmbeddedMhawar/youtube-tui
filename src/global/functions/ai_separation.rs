use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::fs;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::Stdio;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::process::Command;
use tokio::time::{sleep, Duration};
use std::time::Instant;
use regex::Regex;
use chrono;

use crate::global::structs::{SHARED_AI_PROGRESS, SELECTED_SUBTITLE, SELECTED_QUALITY};

const CACHE_DIR: &str = "~/.cache/walker-yt-rs"; 
const SHM_DIR: &str = "/dev/shm/walker-yt-rs";
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
        progress.worker_label = String::new();
        progress.current_chunk = 0;
        progress.total_chunks = 0;
        progress.eta_seconds = None;
        progress.last_update = None;
    }
}

pub async fn start_ai_separation(video_id: String, mode: String) -> Result<u16> {
    log(&format!("AI: Starting separation for {} (mode: {})", video_id, mode));
    reset_ai_progress(); 
    
    let cache_root = expand_path(CACHE_DIR);
    let shm_root = PathBuf::from(SHM_DIR);
    
    let perm_dir = cache_root.join(format!("proc_{}", video_id));
    let work_dir = shm_root.join(format!("proc_{}", video_id));
    
    fs::create_dir_all(&perm_dir)?;
    fs::create_dir_all(&work_dir)?;

    let audio_path = perm_dir.join("input.m4a");
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
            progress.last_update = None;
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
        if let Ok(mut progress) = SHARED_AI_PROGRESS.lock() {
            progress.label = "Download complete".to_string();
        }
    }

    log("AI: Splitting audio into 60s chunks...");
    if let Ok(mut progress) = SHARED_AI_PROGRESS.lock() {
        progress.label = "Splitting audio...".to_string();
    }
    let split_status = Command::new("ffmpeg")
        .args(["-y", "-hide_banner", "-loglevel", "error", "-i", audio_path.to_str().unwrap(), "-f", "segment", "-segment_time", "60", "-c", "copy", chunks_dir.join("chunk_%03d.m4a").to_str().unwrap()])
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
    log(&format!("AI: Found {} chunks (60s) to process.", total));
    
    // Set initial progress
    if let Ok(mut progress) = SHARED_AI_PROGRESS.lock() {
        progress.total_chunks = total;
        progress.worker_label = "Initializing worker...".to_string();
    }

    let playback_file_clone = playback_file.clone();
    let demucs_bin = expand_path(DEMUCS_BIN);
    let out_chunks_dir_clone = out_chunks_dir.clone();
    let perm_dir_clone = perm_dir.clone();

    tokio::spawn(async move {
        log("WORKER: Started");
        let mut current_quota = 800; 
        let mut threads_per_job = 4;
        
        let batch_size = 2;
        let mut i = 0;
        
        while i < total {
            let chunk_start = Instant::now();
            let mut batch_chunks = Vec::new();
            
            for j in 0..batch_size {
                if i + j < total {
                    batch_chunks.push(i + j);
                }
            }

            let stem_name = if mode == "vocals" { "vocals.wav" } else { "no_vocals.wav" };
            let mut needs_processing = Vec::new();
            
            for &idx in &batch_chunks {
                let chunk_path = &entries[idx];
                let chunk_name = chunk_path.file_stem().unwrap().to_str().unwrap();
                let perm_wav = perm_dir_clone.join("htdemucs").join(chunk_name).join(stem_name);
                let work_wav = out_chunks_dir_clone.join("htdemucs").join(chunk_name).join(stem_name);
                
                if perm_wav.exists() {
                    log(&format!("WORKER: Chunk {} CACHED. Restoring...", idx + 1));
                    let _ = fs::create_dir_all(work_wav.parent().unwrap());
                    let _ = fs::copy(&perm_wav, &work_wav);
                } else {
                    needs_processing.push(idx);
                }
            }

            if !needs_processing.is_empty() {
                let proc_paths: Vec<_> = needs_processing.iter().map(|&idx| entries[idx].to_str().unwrap()).collect();
                log(&format!("WORKER: Separating batch: {:?} | Quota: {}%", needs_processing, current_quota));
                
                // Update label with detailed status
                if let Ok(mut progress) = SHARED_AI_PROGRESS.lock() {
                    progress.worker_label = format!("Separating Batch ({} cores)...", threads_per_job * 2);
                }

                let cmd_args = [
                    "--user", "--scope", "--quiet",
                    "-p", "MemoryMax=12G", 
                    "-p", &format!("CPUQuota={}%", current_quota),
                    "-E", "OMP_WAIT_POLICY=PASSIVE",
                    "-E", &format!("OMP_NUM_THREADS={}", threads_per_job),
                    "-E", &format!("MKL_NUM_THREADS={}", threads_per_job),
                    "-E", &format!("OPENBLAS_NUM_THREADS={}", threads_per_job),
                    "-E", &format!("VECLIB_MAXIMUM_THREADS={}", threads_per_job),
                    demucs_bin.to_str().unwrap(), "-n", "htdemucs", "--two-stems=vocals",
                    "--segment", "7", "--shifts", "0", "--overlap", "0.1", "-d", "cpu", "-j", "2",
                    "-o", out_chunks_dir_clone.to_str().unwrap()
                ];
                
                let mut full_args = cmd_args.to_vec();
                full_args.extend(proc_paths);

                let output = Command::new("/usr/bin/systemd-run")
                    .args(&full_args)
                    .stdout(Stdio::null())
                    .stderr(Stdio::piped())
                    .output()
                    .await;
                
                if let Ok(out) = output {
                    if !out.status.success() {
                        let err_msg = String::from_utf8_lossy(&out.stderr);
                        log(&format!("WORKER: systemd-run FAILED: {}", err_msg.trim()));
                    }
                }
                
                // Sync to permanent cache
                for &idx in &needs_processing {
                    let chunk_name = entries[idx].file_stem().unwrap().to_str().unwrap();
                    let work_wav = out_chunks_dir_clone.join("htdemucs").join(chunk_name).join(stem_name);
                    let perm_wav = perm_dir_clone.join("htdemucs").join(chunk_name).join(stem_name);
                    if work_wav.exists() {
                        let _ = fs::create_dir_all(perm_wav.parent().unwrap());
                        let _ = fs::copy(&work_wav, &perm_wav);
                    }
                }
            }

            // Post-process batch into playback file
            for &idx in &batch_chunks {
                let chunk_name = entries[idx].file_stem().unwrap().to_str().unwrap();
                let work_wav = out_chunks_dir_clone.join("htdemucs").join(chunk_name).join(stem_name);
                
                if work_wav.exists() {
                    let output = Command::new("ffmpeg")
                        .args(["-y", "-hide_banner", "-loglevel", "error", "-i", work_wav.to_str().unwrap(), "-f", "s16le", "-acodec", "pcm_s16le", "-ar", "44100", "-ac", "2", "-"])
                        .output()
                        .await;

                    if let Ok(output) = output {
                        if let Ok(mut f) = fs::OpenOptions::new().create(true).append(true).open(&playback_file_clone) {
                            let _ = f.write_all(&output.stdout);
                            let _ = f.sync_all();
                        }
                    }
                }
            }

            let was_cached_batch = needs_processing.is_empty();
            let batch_elapsed = chunk_start.elapsed().as_secs_f32();
            
            if !was_cached_batch {
                let r = (batch_chunks.len() as f32 * 60.0) / batch_elapsed;
                if r < 1.40 {
                    current_quota = (current_quota + 100).min(800);
                    threads_per_job = (threads_per_job + 1).min(4);
                } else if r > 1.80 {
                    current_quota = (current_quota - 100).max(400);
                    threads_per_job = (threads_per_job - 1).max(2);
                }

                if let Ok(mut progress) = SHARED_AI_PROGRESS.lock() {
                    progress.current_chunk = i + batch_chunks.len();
                    progress.total_chunks = total;
                    progress.ratio = Some(r);
                    progress.eta_seconds = Some(((total - (i + batch_chunks.len())) as f32 * (60.0 / r)) as u64);
                    progress.last_update = Some(Instant::now());
                    progress.worker_label = "Separating (Batched)...".to_string();
                }
            } else {
                if let Ok(mut progress) = SHARED_AI_PROGRESS.lock() {
                    progress.current_chunk = i + batch_chunks.len();
                    progress.total_chunks = total;
                    progress.worker_label = "Processing (Cached)...".to_string();
                }
            }

            i += batch_size;
        }
        
        reset_ai_progress();
        log("WORKER: Finished");
    });

    log("AI: Waiting for initial audio buffer (4 chunks / 4min)...");
    let start_wait = Instant::now();
    loop {
        let current_size = playback_file.exists().then(|| fs::metadata(&playback_file).ok().map(|m| m.len())).flatten().unwrap_or(0);
        let target_size = 40_000_000; // ~4 chunks of 60s
        
        if current_size >= target_size { 
            log("AI: Initial buffer READY.");
            if let Ok(mut progress) = SHARED_AI_PROGRESS.lock() {
                progress.label = "Playing".to_string();
            }
            break; 
        }

        // Update ETA for the buffer wait
        if let Ok(mut progress) = SHARED_AI_PROGRESS.lock() {
            let remaining_bytes = target_size.saturating_sub(current_size);
            // Assume 1.2x speed if we don't have a ratio yet
            let speed = progress.ratio.unwrap_or(1.2);
            let bytes_per_sec = 176400.0 * speed;
            let wait_eta = (remaining_bytes as f32 / bytes_per_sec) as u64;
            
            progress.label = format!("Buffer: {}% | Wait for 4 chunks...", (current_size * 100 / target_size));
            progress.eta_seconds = Some(wait_eta);
            progress.last_update = Some(Instant::now());
        }

        if start_wait.elapsed() > Duration::from_secs(480) { // 8 minutes timeout
            log("AI: BUFFER TIMEOUT.");
            if current_size > 0 { 
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
