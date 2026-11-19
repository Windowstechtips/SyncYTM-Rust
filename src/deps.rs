use futures_util::StreamExt;
use std::ffi::OsStr;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

/// Creates a `std::process::Command` configured to never spawn a console window on Windows.
pub fn create_command<S: AsRef<OsStr>>(program: S) -> Command {
    let mut cmd = Command::new(program);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NO_WINDOW: u32 = 0x08000000;
        cmd.creation_flags(CREATE_NO_WINDOW);
    }
    cmd
}

/// Returns the persistent directory where SyncYTM downloads standalone helper binaries (%APPDATA%\SyncYTM\bin).
pub fn app_bin_dir() -> PathBuf {
    if let Ok(appdata) = std::env::var("APPDATA") {
        let dir = PathBuf::from(appdata).join("SyncYTM").join("bin");
        let _ = std::fs::create_dir_all(&dir);
        return dir;
    }
    let fallback = PathBuf::from("bin");
    let _ = std::fs::create_dir_all(&fallback);
    fallback
}

fn test_executable(name: &str, args: &[&str]) -> bool {
    let mut cmd = create_command(name);
    cmd.args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    match cmd.status() {
        Ok(status) => status.success(),
        Err(_) => false,
    }
}

/// Attempts to locate `yt-dlp` executable.
pub fn find_yt_dlp() -> Option<PathBuf> {
    // 1. Next to current executable
    if let Ok(exe_dir) = std::env::current_exe().map(|p| p.parent().unwrap_or(Path::new("")).to_path_buf()) {
        let candidate = exe_dir.join("yt-dlp.exe");
        if candidate.is_file() {
            return Some(candidate);
        }
    }

    // 2. Current working directory
    if Path::new("yt-dlp.exe").is_file() {
        return Some(PathBuf::from("yt-dlp.exe"));
    }

    // 3. AppData bin directory (%APPDATA%\SyncYTM\bin\yt-dlp.exe)
    let appdata_bin = app_bin_dir().join("yt-dlp.exe");
    if appdata_bin.is_file() {
        return Some(appdata_bin);
    }

    // 4. User cargo bin directory
    if let Ok(home) = std::env::var("USERPROFILE") {
        let candidate = PathBuf::from(home).join(".cargo").join("bin").join("yt-dlp.exe");
        if candidate.is_file() {
            return Some(candidate);
        }
    }

    // 5. Test system PATH
    if test_executable("yt-dlp", &["--version"]) {
        return Some(PathBuf::from("yt-dlp"));
    }

    None
}

/// Attempts to locate `ffmpeg` executable.
pub fn find_ffmpeg() -> Option<PathBuf> {
    // 1. FFMPEG_PATH environment variable
    if let Ok(p) = std::env::var("FFMPEG_PATH") {
        let path = PathBuf::from(p);
        if path.is_file() {
            return Some(path);
        }
    }

    // 2. Next to current executable
    if let Ok(exe_dir) = std::env::current_exe().map(|p| p.parent().unwrap_or(Path::new("")).to_path_buf()) {
        let candidate = exe_dir.join("ffmpeg.exe");
        if candidate.is_file() {
            return Some(candidate);
        }
    }

    // 3. Current working directory
    if Path::new("ffmpeg.exe").is_file() {
        return Some(PathBuf::from("ffmpeg.exe"));
    }

    // 4. AppData bin directory (%APPDATA%\SyncYTM\bin\ffmpeg.exe)
    let appdata_bin = app_bin_dir().join("ffmpeg.exe");
    if appdata_bin.is_file() {
        return Some(appdata_bin);
    }

    // 5. Common installation path
    if Path::new("C:\\ffmpeg\\bin\\ffmpeg.exe").is_file() {
        return Some(PathBuf::from("C:\\ffmpeg\\bin\\ffmpeg.exe"));
    }

    // 6. Test system PATH
    if test_executable("ffmpeg", &["-version"]) {
        return Some(PathBuf::from("ffmpeg"));
    }

    None
}

/// Returns (need_ytdlp, need_ffmpeg).
pub fn check_missing() -> (bool, bool) {
    let need_ytdlp = find_yt_dlp().is_none();
    let need_ffmpeg = find_ffmpeg().is_none();
    (need_ytdlp, need_ffmpeg)
}

#[derive(Clone, Debug, PartialEq)]
pub enum DependencyStatus {
    Checking,
    Downloading {
        item: String,
        progress_pct: f32,
        details: String,
    },
    Extracting {
        item: String,
    },
    Ready,
    Error(String),
}

impl DependencyStatus {
    pub fn is_ready(&self) -> bool {
        matches!(self, DependencyStatus::Ready)
    }

    #[allow(dead_code)]
    pub fn is_busy(&self) -> bool {
        matches!(
            self,
            DependencyStatus::Checking
                | DependencyStatus::Downloading { .. }
                | DependencyStatus::Extracting { .. }
        )
    }
}

/// Asynchronously downloads and installs missing dependencies into `%APPDATA%\SyncYTM\bin`.
pub async fn install_dependencies(
    tx: crossbeam_channel::Sender<DependencyStatus>,
    need_ytdlp: bool,
    need_ffmpeg: bool,
) {
    if !need_ytdlp && !need_ffmpeg {
        let _ = tx.send(DependencyStatus::Ready);
        return;
    }

    let client = match reqwest::Client::builder()
        .user_agent("SyncYTM/0.1.0")
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            let _ = tx.send(DependencyStatus::Error(format!("Failed to init HTTP client: {}", e)));
            return;
        }
    };

    let bin_dir = app_bin_dir();

    // 1. Install yt-dlp if needed
    if need_ytdlp {
        let _ = tx.send(DependencyStatus::Downloading {
            item: "yt-dlp".to_string(),
            progress_pct: 0.0,
            details: "Connecting to GitHub...".to_string(),
        });

        let url = "https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp.exe";
        let res = match client.get(url).send().await {
            Ok(r) if r.status().is_success() => r,
            Ok(r) => {
                let _ = tx.send(DependencyStatus::Error(format!(
                    "Failed to download yt-dlp (HTTP {})",
                    r.status()
                )));
                return;
            }
            Err(e) => {
                let _ = tx.send(DependencyStatus::Error(format!("Network error downloading yt-dlp: {}", e)));
                return;
            }
        };

        let total_size = res.content_length().unwrap_or(24 * 1024 * 1024);
        let part_path = bin_dir.join("yt-dlp.exe.part");
        let target_path = bin_dir.join("yt-dlp.exe");

        let mut file = match std::fs::File::create(&part_path) {
            Ok(f) => f,
            Err(e) => {
                let _ = tx.send(DependencyStatus::Error(format!("File create error: {}", e)));
                return;
            }
        };

        let mut downloaded: u64 = 0;
        let mut stream = res.bytes_stream();

        while let Some(chunk_res) = stream.next().await {
            let chunk = match chunk_res {
                Ok(c) => c,
                Err(e) => {
                    let _ = tx.send(DependencyStatus::Error(format!("Download stream error: {}", e)));
                    let _ = std::fs::remove_file(&part_path);
                    return;
                }
            };

            if let Err(e) = file.write_all(&chunk) {
                let _ = tx.send(DependencyStatus::Error(format!("File write error: {}", e)));
                let _ = std::fs::remove_file(&part_path);
                return;
            }

            downloaded += chunk.len() as u64;
            let pct = (downloaded as f32 / total_size as f32).clamp(0.0, 1.0);
            let details = format!(
                "{:.1} MB / {:.1} MB",
                downloaded as f64 / 1_000_000.0,
                total_size as f64 / 1_000_000.0
            );

            let _ = tx.send(DependencyStatus::Downloading {
                item: "yt-dlp".to_string(),
                progress_pct: pct,
                details,
            });
        }

        drop(file);
        if let Err(e) = std::fs::rename(&part_path, &target_path) {
            let _ = tx.send(DependencyStatus::Error(format!("Failed to install yt-dlp: {}", e)));
            return;
        }
    }

    // 2. Install ffmpeg if needed
    if need_ffmpeg {
        let _ = tx.send(DependencyStatus::Downloading {
            item: "ffmpeg".to_string(),
            progress_pct: 0.0,
            details: "Connecting to GitHub...".to_string(),
        });

        let url = "https://github.com/eugeneware/ffmpeg-static/releases/download/b6.1.1/ffmpeg-win32-x64.gz";
        let res = match client.get(url).send().await {
            Ok(r) if r.status().is_success() => r,
            Ok(r) => {
                let _ = tx.send(DependencyStatus::Error(format!(
                    "Failed to download ffmpeg (HTTP {})",
                    r.status()
                )));
                return;
            }
            Err(e) => {
                let _ = tx.send(DependencyStatus::Error(format!("Network error downloading ffmpeg: {}", e)));
                return;
            }
        };

        let total_size = res.content_length().unwrap_or(29 * 1024 * 1024);
        let gz_path = bin_dir.join("ffmpeg.gz");
        let part_path = bin_dir.join("ffmpeg.exe.part");
        let target_path = bin_dir.join("ffmpeg.exe");

        let mut file = match std::fs::File::create(&gz_path) {
            Ok(f) => f,
            Err(e) => {
                let _ = tx.send(DependencyStatus::Error(format!("File create error: {}", e)));
                return;
            }
        };

        let mut downloaded: u64 = 0;
        let mut stream = res.bytes_stream();

        while let Some(chunk_res) = stream.next().await {
            let chunk = match chunk_res {
                Ok(c) => c,
                Err(e) => {
                    let _ = tx.send(DependencyStatus::Error(format!("Download stream error: {}", e)));
                    let _ = std::fs::remove_file(&gz_path);
                    return;
                }
            };

            if let Err(e) = file.write_all(&chunk) {
                let _ = tx.send(DependencyStatus::Error(format!("File write error: {}", e)));
                let _ = std::fs::remove_file(&gz_path);
                return;
            }

            downloaded += chunk.len() as u64;
            let pct = (downloaded as f32 / total_size as f32).clamp(0.0, 1.0);
            let details = format!(
                "{:.1} MB / {:.1} MB",
                downloaded as f64 / 1_000_000.0,
                total_size as f64 / 1_000_000.0
            );

            let _ = tx.send(DependencyStatus::Downloading {
                item: "ffmpeg".to_string(),
                progress_pct: pct,
                details,
            });
        }

        drop(file);

        let _ = tx.send(DependencyStatus::Extracting {
            item: "ffmpeg (extracting executable)...".to_string(),
        });

        // Decompress the .gz archive in a blocking thread
        let gz_clone = gz_path.clone();
        let part_clone = part_path.clone();
        let target_clone = target_path.clone();

        let extract_result = tokio::task::spawn_blocking(move || -> Result<(), String> {
            let gz_file = std::fs::File::open(&gz_clone)
                .map_err(|e| format!("Failed to open downloaded archive: {}", e))?;
            let mut decoder = flate2::read::GzDecoder::new(gz_file);
            let mut out_file = std::fs::File::create(&part_clone)
                .map_err(|e| format!("Failed to create destination ffmpeg file: {}", e))?;
            std::io::copy(&mut decoder, &mut out_file)
                .map_err(|e| format!("Decompression error: {}", e))?;
            drop(out_file);
            let _ = std::fs::remove_file(&gz_clone);
            std::fs::rename(&part_clone, &target_clone)
                .map_err(|e| format!("Failed to finalize ffmpeg: {}", e))?;
            Ok(())
        })
        .await;

        match extract_result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                let _ = tx.send(DependencyStatus::Error(e));
                return;
            }
            Err(e) => {
                let _ = tx.send(DependencyStatus::Error(format!("Task execution error: {}", e)));
                return;
            }
        }
    }

    let _ = tx.send(DependencyStatus::Ready);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_command_runs_without_window() {
        let mut cmd = create_command("cmd");
        cmd.args(["/C", "echo hello"]);
        let output = cmd.output().expect("Failed to execute command");
        assert!(output.status.success());
        assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "hello");
    }

    #[test]
    fn test_app_bin_dir_exists() {
        let bin_dir = app_bin_dir();
        assert!(bin_dir.exists(), "Bin directory should exist");
    }

    #[test]
    fn test_check_missing_returns_bool_pair() {
        let (need_ytdlp, need_ffmpeg) = check_missing();
        // Just verify it runs and doesn't panic
        println!("need_ytdlp: {}, need_ffmpeg: {}", need_ytdlp, need_ffmpeg);
    }
}
