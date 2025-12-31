use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::process::Stdio;
use tokio::io::{AsyncReadExt, AsyncWriteExt, AsyncBufReadExt, AsyncSeekExt};
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

pub fn expand_path(path: &str) -> PathBuf {
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
        progress.split_finished = false;
        progress.worker_finished = false;
        progress.label = String::new();
        progress.worker_label = String::new();
        progress.current_chunk = 0;
        progress.total_chunks = 0;
        progress.eta_seconds = None;
        progress.last_update = Instant::now();
    }
}

pub fn cleanup_cache() -> Result<()> {
    let cache_root = expand_path(CACHE_DIR);
    if !cache_root.exists() { return Ok(()); }

    let mut entries: Vec<_> = fs::read_dir(&cache_root)?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().map_or(false, |ft| ft.is_dir()))
        .collect();

    // Sort by modification time, newest first
    entries.sort_by(|a, b| {
        let ma = a.metadata().and_then(|m| m.modified()).ok();
        let mb = b.metadata().and_then(|m| m.modified()).ok();
        mb.cmp(&ma)
    });

    // Keep only the 5 most recent processed videos
    if entries.len() > 5 {
        for entry in entries.iter().skip(5) {
            log(&format!("AI: Cleaning up old cache: {:?}", entry.path()));
            let _ = fs::remove_dir_all(entry.path());
        }
    }
    Ok(())
}

pub async fn start_ai_separation(video_id: String, mode: String) -> Result<u16> {
    log(&format!("AI: Starting separation for {} (mode: {})", video_id, mode));
    let _ = cleanup_cache();
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

    let metadata = get_video_metadata_yt_dlp(&video_id).await?;
    let duration = metadata["duration"].as_f64().unwrap_or(0.0);
    let total = (duration / 60.0).ceil() as usize;

    {
        if let Ok(mut progress) = SHARED_AI_PROGRESS.lock() {
            progress.active = true;
            progress.total_chunks = total;
            progress.label = "Starting stream...".to_string();
            progress.current_chunk = 0;
            progress.eta_seconds = None;
            progress.last_update = Instant::now();
        }
    }

    let chunks_dir_clone = chunks_dir.clone();
    let video_id_clone = video_id.clone();
    let audio_path_clone = audio_path.clone();

    tokio::spawn(async move {
        let yt_dlp_bin = expand_path(YT_DLP_BIN);
        let mut cmd = if audio_path_clone.exists() {
            let mut c = Command::new("ffmpeg");
            c.args(["-y", "-hide_banner", "-loglevel", "error", "-i", audio_path_clone.to_str().unwrap(), "-f", "segment", "-segment_time", "15", "-c", "copy", chunks_dir_clone.join("chunk_%03d.m4a").to_str().unwrap()]);
            c.stderr(Stdio::piped());
            c
        } else {
            let mut c = Command::new("sh");
            c.arg("-c").arg(format!(
                "{} --quiet --no-progress -f bestaudio[ext=m4a]/bestaudio -o - --no-playlist -- {} | tee {} | ffmpeg -y -hide_banner -loglevel error -i pipe:0 -f segment -segment_time 15 -c copy {}",
                yt_dlp_bin.to_str().unwrap(),
                video_id_clone,
                audio_path_clone.to_str().unwrap(),
                chunks_dir_clone.join("chunk_%03d.m4a").to_str().unwrap()
            ));
            c.stderr(Stdio::piped());
            c
        };

        log("AI: Starting background download/split process (15s chunks)...");
        if let Ok(mut child) = cmd.spawn() {
            if let Some(stderr) = child.stderr.take() {
                tokio::spawn(async move {
                    let mut reader = tokio::io::BufReader::new(stderr);
                    let mut line = String::new();
                    while let Ok(n) = reader.read_line(&mut line).await {
                        if n == 0 { break; }
                        log(&format!("YT-DLP/FFMPEG: {}", line.trim()));
                        line.clear();
                    }
                });
            }
            let _ = child.wait().await;
        }
        if let Ok(mut progress) = SHARED_AI_PROGRESS.lock() {
            progress.split_finished = true;
        }
        log("AI: Split process finished.");
    });

    let playback_file_clone = playback_file.clone();
    let demucs_bin = expand_path(DEMUCS_BIN);
    let out_chunks_dir_clone = out_chunks_dir.clone();
    let perm_dir_clone = perm_dir.clone();
    let chunks_dir_worker = chunks_dir.clone();

    tokio::spawn(async move {
        log("WORKER: Started");
        let mut current_quota = 600; 
        let mut threads_per_job = 3;
        let mut i = 0;
        
        loop {
            {
                if let Ok(progress) = SHARED_AI_PROGRESS.lock() {
                    if !progress.active {
                        log("WORKER: Stopping because active is false");
                        break;
                    }
                }
            }

            // Hybrid batching: 1 chunk for the first one (15s), then 2 chunks (30s) for the rest
            let batch_size = if i == 0 { 1 } else { 2 };
            let mut batch_chunks = Vec::new();
            
            while batch_chunks.len() < batch_size {
                let idx = i + batch_chunks.len();
                let chunk_path = chunks_dir_worker.join(format!("chunk_{:03}.m4a", idx));
                let next_chunk_path = chunks_dir_worker.join(format!("chunk_{:03}.m4a", idx + 1));
                let split_finished = SHARED_AI_PROGRESS.lock().map(|p| p.split_finished).unwrap_or(false);

                if chunk_path.exists() {
                    // If it's the last chunk of the video, or if the next chunk exists, we can process it
                    if next_chunk_path.exists() || split_finished {
                        batch_chunks.push(idx);
                        continue;
                    }
                } else if split_finished {
                    break;
                }
                
                if !batch_chunks.is_empty() { break; }
                sleep(Duration::from_millis(500)).await;
            }

            if batch_chunks.is_empty() {
                if SHARED_AI_PROGRESS.lock().map(|p| p.split_finished).unwrap_or(false) { break; }
                continue;
            }

            let chunk_start = Instant::now();
            let stem_name = if mode == "vocals" { "vocals.wav" } else { "no_vocals.wav" };
            let mut needs_processing = Vec::new();
            
            for &idx in &batch_chunks {
                let chunk_name = format!("chunk_{:03}", idx);
                let perm_wav = perm_dir_clone.join("htdemucs").join(&chunk_name).join(stem_name);
                let work_wav = out_chunks_dir_clone.join("htdemucs").join(&chunk_name).join(stem_name);
                
                if perm_wav.exists() {
                    log(&format!("WORKER: Chunk {} CACHED. Restoring...", idx + 1));
                    let _ = fs::create_dir_all(work_wav.parent().unwrap());
                    let _ = fs::copy(&perm_wav, &work_wav);
                } else {
                    needs_processing.push(idx);
                }
            }

            if !needs_processing.is_empty() {
                let proc_paths: Vec<_> = needs_processing.iter().map(|&idx| chunks_dir_worker.join(format!("chunk_{:03}.m4a", idx)).to_str().unwrap().to_string()).collect();
                log(&format!("WORKER: Separating batch: {:?} | Quota: {}%", needs_processing, current_quota));
                
                if let Ok(mut progress) = SHARED_AI_PROGRESS.lock() {
                    progress.worker_label = format!("Separating Batch ({} cores)...", threads_per_job * 2);
                    progress.last_update = Instant::now();
                }

                let cmd_args = [
                    "--user", "--scope", "--quiet",
                    "-p", "MemoryMax=8G", 
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
                let proc_paths_refs: Vec<&str> = proc_paths.iter().map(|s| s.as_str()).collect();
                full_args.extend(proc_paths_refs);

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
                
                for &idx in &needs_processing {
                    let chunk_name = format!("chunk_{:03}", idx);
                    let work_wav = out_chunks_dir_clone.join("htdemucs").join(&chunk_name).join(stem_name);
                    let perm_wav = perm_dir_clone.join("htdemucs").join(&chunk_name).join(stem_name);
                    if work_wav.exists() {
                        let _ = fs::create_dir_all(perm_wav.parent().unwrap());
                        let _ = fs::copy(&work_wav, &perm_wav);
                    }
                }
            }

            for &idx in &batch_chunks {
                let chunk_name = format!("chunk_{:03}", idx);
                let work_wav = out_chunks_dir_clone.join("htdemucs").join(&chunk_name).join(stem_name);
                
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
                    progress.worker_label = "Separating (Batched)...".to_string();
                    progress.last_update = Instant::now();
                }
            } else {
                if let Ok(mut progress) = SHARED_AI_PROGRESS.lock() {
                    progress.current_chunk = i + batch_chunks.len();
                    progress.total_chunks = total;
                    progress.worker_label = "Processing (Cached)...".to_string();
                    progress.last_update = Instant::now();
                }
            }

            i += batch_chunks.len();
        }
        
        if let Ok(mut progress) = SHARED_AI_PROGRESS.lock() {
            progress.worker_finished = true;
        }
        log("WORKER: Finished");
    });

    log("AI: Waiting for initial audio buffer (15s)...");
    let start_wait = Instant::now();
    loop {
        let current_size = playback_file.exists().then(|| fs::metadata(&playback_file).ok().map(|m| m.len())).flatten().unwrap_or(0);
        let target_size: u64 = 2_500_000; // ~15s of audio
        
        if current_size >= target_size { 
            log("AI: Initial buffer READY.");
            break; 
        }

        if let Ok(progress) = SHARED_AI_PROGRESS.lock() {
            if !progress.active {
                log("AI: Stopping buffer wait because active is false");
                return Err(anyhow!("Cancelled"));
            }
        }

        if let Ok(mut progress) = SHARED_AI_PROGRESS.lock() {
            let remaining_bytes = target_size.saturating_sub(current_size);
            let speed = progress.ratio.unwrap_or(1.2);
            let bytes_per_sec = 176400.0 * speed;
            let wait_eta = (remaining_bytes as f32 / bytes_per_sec) as u64;
            
            progress.label = format!("Buffer: {}% | Wait for initial chunk...", (current_size * 100 / target_size));
            progress.eta_seconds = Some(wait_eta);
            progress.last_update = Instant::now();
        }

        if start_wait.elapsed() > Duration::from_secs(480) { 
            log("AI: BUFFER TIMEOUT.");
            if current_size > 0 { break; }
            return Err(anyhow!("Timeout waiting for audio buffer"));
        }
        sleep(Duration::from_secs(1)).await;
    }
    
    // Clear the high-level label once buffering is done
    if let Ok(mut progress) = SHARED_AI_PROGRESS.lock() {
        progress.label = String::new();
        progress.last_update = Instant::now();
    }

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    
    let playback_file_stream = playback_file.clone();
    tokio::spawn(async move {
        log(&format!("TCP: Server started on port {}", port));
        if let Ok((mut stream, _)) = listener.accept().await {
            log("TCP: Player connected.");
            if let Ok(mut f) = tokio::fs::File::open(&playback_file_stream).await {
                let mut buffer = [0; 32768];
                loop {
                    let mut is_active = false;
                    let mut is_worker_finished = false;
                    if let Ok(progress) = SHARED_AI_PROGRESS.lock() {
                        is_active = progress.active;
                        is_worker_finished = progress.worker_finished;
                    }

                    if !is_active {
                        log("TCP: Stopping because active is false");
                        break;
                    }

                    // Check how much data is available in the file
                    let current_pos = f.stream_position().await.unwrap_or(0);
                    let file_len = fs::metadata(&playback_file_stream).map(|m| m.len()).unwrap_or(0);
                    let available = file_len.saturating_sub(current_pos);

                    // If worker is still running, wait for at least 10 seconds of audio (approx 1.76MB)
                    // to avoid stuttering and force mpv to buffer properly.
                    let threshold = if is_worker_finished { 0 } else { 1_764_000 };

                    if available > threshold || (is_worker_finished && available > 0) {
                        let n = f.read(&mut buffer).await.unwrap_or(0);
                        if n > 0 {
                            if stream.write_all(&buffer[..n]).await.is_err() { break; }
                        }
                    } else {
                        // If worker is finished and we've sent everything, just wait.
                        // DO NOT close the stream, as mpv might still be playing from its internal buffer.
                        // The stream will be closed when progress.active becomes false (user switches video/closes mpv).
                        sleep(Duration::from_millis(500)).await;
                    }
                }
            }
        }
        log("TCP: Server closed.");
    });

    Ok(port)
}
