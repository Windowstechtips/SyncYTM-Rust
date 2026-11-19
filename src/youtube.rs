use crate::config::YOUTUBE_API_KEY;
use crate::models::VideoItem;
use std::path::{Path, PathBuf};
use std::process::Command;

pub struct YoutubeService {
    yt_dlp_path: PathBuf,
    client: reqwest::Client,
}

impl YoutubeService {
    pub fn new() -> Self {
        let yt_dlp_path = Self::find_yt_dlp();
        Self {
            yt_dlp_path,
            client: reqwest::Client::new(),
        }
    }

    fn find_yt_dlp() -> PathBuf {
        // 1. Current directory
        if Path::new("yt-dlp.exe").exists() {
            return PathBuf::from("yt-dlp.exe");
        }
        // 2. Next to executable
        if let Ok(exe_dir) = std::env::current_exe().map(|p| p.parent().unwrap_or(Path::new("")).to_path_buf()) {
            let candidate = exe_dir.join("yt-dlp.exe");
            if candidate.exists() {
                return candidate;
            }
        }
        // 3. User cargo bin directory
        if let Ok(home) = std::env::var("USERPROFILE") {
            let candidate = PathBuf::from(home).join(".cargo").join("bin").join("yt-dlp.exe");
            if candidate.exists() {
                return candidate;
            }
        }
        // 4. Default to system PATH
        PathBuf::from("yt-dlp")
    }

    /// Extract direct audio stream URL and duration in seconds from YouTube video ID or URL
    pub async fn resolve_audio_url(&self, video_id: &str) -> Result<(String, f64), String> {
        let yt_dlp = self.yt_dlp_path.clone();
        let target_url = if video_id.starts_with("http") {
            video_id.to_string()
        } else {
            format!("https://www.youtube.com/watch?v={}", video_id)
        };

        tokio::task::spawn_blocking(move || {
            let output = Command::new(&yt_dlp)
                .args(["--print", "%(duration)s", "-g", "-f", "ba/b", "--no-warnings", &target_url])
                .output()
                .map_err(|e| format!("Failed to run yt-dlp: {}", e))?;

            if !output.status.success() {
                let err = String::from_utf8_lossy(&output.stderr);
                return Err(format!("yt-dlp resolution error: {}", err.trim()));
            }

            let stdout = String::from_utf8_lossy(&output.stdout);
            let mut lines = stdout.lines().filter(|l| !l.trim().is_empty());
            
            let first = lines.next().unwrap_or("").trim();
            let second = lines.next().unwrap_or("").trim();

            let (duration_sec, url) = if !second.is_empty() {
                let dur = first.parse::<f64>().unwrap_or(240.0);
                (dur, second.to_string())
            } else {
                (240.0, first.to_string())
            };

            if url.is_empty() {
                Err("Empty audio URL from yt-dlp".to_string())
            } else {
                Ok((url, duration_sec))
            }
        })
        .await
        .map_err(|e| format!("Task join error: {}", e))?
    }

    /// Search YouTube videos by keyword (tries YouTube Data API v3 first, falls back to yt-dlp)
    pub async fn search(&self, query: &str) -> Result<Vec<VideoItem>, String> {
        if query.trim().is_empty() {
            return Ok(Vec::new());
        }

        // Try YouTube Data API first if API key configured
        if !YOUTUBE_API_KEY.is_empty() {
            if let Ok(results) = self.search_api(query).await {
                if !results.is_empty() {
                    return Ok(results);
                }
            }
        }

        // Fallback: yt-dlp ytsearch
        self.search_yt_dlp(query).await
    }

    async fn search_api(&self, query: &str) -> Result<Vec<VideoItem>, String> {
        let url = format!(
            "https://www.googleapis.com/youtube/v3/search?part=snippet&maxResults=15&q={}&type=video&key={}",
            urlencoding::encode(query),
            YOUTUBE_API_KEY
        );

        let res = self.client.get(&url).send().await.map_err(|e| e.to_string())?;
        let json: serde_json::Value = res.json().await.map_err(|e| e.to_string())?;

        if let Some(items) = json.get("items").and_then(|i| i.as_array()) {
            let mut results = Vec::new();
            for item in items {
                let id = item["id"]["videoId"].as_str().unwrap_or("").to_string();
                let title = item["snippet"]["title"].as_str().unwrap_or("").to_string();
                let channel = item["snippet"]["channelTitle"].as_str().unwrap_or("").to_string();
                let thumbnail = item["snippet"]["thumbnails"]["medium"]["url"].as_str().map(|s| s.to_string());

                if !id.is_empty() {
                    results.push(VideoItem {
                        id,
                        title: html_escape::decode_html_entities(&title).to_string(),
                        channel: html_escape::decode_html_entities(&channel).to_string(),
                        thumbnail,
                        queue_item_id: None,
                        duration_sec: None,
                    });
                }
            }
            return Ok(results);
        }

        Err("No items found".to_string())
    }

    async fn search_yt_dlp(&self, query: &str) -> Result<Vec<VideoItem>, String> {
        let yt_dlp = self.yt_dlp_path.clone();
        let search_arg = format!("ytsearch10:{}", query);

        tokio::task::spawn_blocking(move || {
            let output = Command::new(&yt_dlp)
                .args(["--dump-json", "--flat-playlist", "--no-warnings", &search_arg])
                .output()
                .map_err(|e| format!("Failed to run yt-dlp search: {}", e))?;

            let stdout = String::from_utf8_lossy(&output.stdout);
            let mut results = Vec::new();
            for line in stdout.lines() {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(line) {
                    let id = v["id"].as_str().unwrap_or("").to_string();
                    let title = v["title"].as_str().unwrap_or("").to_string();
                    let channel = v["uploader"].as_str()
                        .or_else(|| v["channel"].as_str())
                        .unwrap_or("Unknown")
                        .to_string();
                    let thumbnail = v["thumbnail"].as_str().map(|s| s.to_string());
                    let duration_sec = v["duration"].as_f64();

                    if !id.is_empty() {
                        results.push(VideoItem {
                            id,
                            title,
                            channel,
                            thumbnail,
                            queue_item_id: None,
                            duration_sec,
                        });
                    }
                }
            }
            Ok(results)
        })
        .await
        .map_err(|e| format!("Task join error: {}", e))?
    }

    /// Fetch all video items from a YouTube or YouTube Music playlist
    pub async fn fetch_playlist(&self, playlist_url: &str) -> Result<Vec<VideoItem>, String> {
        let yt_dlp = self.yt_dlp_path.clone();
        let url = playlist_url.to_string();

        tokio::task::spawn_blocking(move || {
            let output = Command::new(&yt_dlp)
                .args(["--dump-json", "--flat-playlist", "--no-warnings", &url])
                .output()
                .map_err(|e| format!("Failed to run yt-dlp playlist extraction: {}", e))?;

            let stdout = String::from_utf8_lossy(&output.stdout);
            let mut results = Vec::new();
            for line in stdout.lines() {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(line) {
                    let id = v["id"].as_str().unwrap_or("").to_string();
                    let title = v["title"].as_str().unwrap_or("").to_string();
                    let channel = v["uploader"].as_str()
                        .or_else(|| v["channel"].as_str())
                        .unwrap_or("Unknown")
                        .to_string();
                    let thumbnail = v["thumbnail"].as_str().map(|s| s.to_string());
                    let duration_sec = v["duration"].as_f64();

                    if !id.is_empty() {
                        results.push(VideoItem {
                            id,
                            title,
                            channel,
                            thumbnail,
                            queue_item_id: None,
                            duration_sec,
                        });
                    }
                }
            }
            Ok(results)
        })
        .await
        .map_err(|e| format!("Task join error: {}", e))?
    }
}
