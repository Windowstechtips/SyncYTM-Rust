use crate::config::DISCORD_APP_ID;
use discord_rich_presence::activity::{Activity, Assets, Button, Timestamps};
use discord_rich_presence::{DiscordIpc, DiscordIpcClient};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug)]
pub struct PresencePayload {
    pub title: String,
    pub artist: String,
    pub room_name: String,
    pub room_id: String,
    pub thumbnail: Option<String>,
    pub is_playing: bool,
    pub progress_sec: f64,
    pub duration_sec: f64,
}

enum DiscordCommand {
    Update(PresencePayload),
    Clear,
}

pub struct DiscordManager {
    tx: crossbeam_channel::Sender<DiscordCommand>,
}

fn get_app_id() -> String {
    if let Ok(id) = std::env::var("DISCORD_APP_ID") {
        let trimmed = id.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }
    DISCORD_APP_ID.to_string()
}

/// Sanitizes Discord Rich Presence text fields (enforces min 2 chars, max limit, trims).
pub fn sanitize_str(text: &str, fallback: &str, max_len: usize) -> String {
    let trimmed = text.trim();
    let text = if trimmed.is_empty() {
        fallback
    } else {
        trimmed
    };

    // Discord rejects activity text fields shorter than 2 characters
    let mut s = if text.chars().count() < 2 {
        format!("{} ", text)
    } else {
        text.to_string()
    };

    // Discord limits activity strings to max_len (typically 128 characters)
    if s.chars().count() > max_len {
        let mut truncated: String = s.chars().take(max_len.saturating_sub(1)).collect();
        truncated.push('…');
        s = truncated;
    }

    s
}

impl DiscordManager {
    pub fn new() -> Self {
        let (tx, rx) = crossbeam_channel::unbounded::<DiscordCommand>();

        // Spawn dedicated background thread for Discord IPC so UI NEVER blocks
        thread::spawn(move || {
            let mut client: Option<DiscordIpcClient> = None;
            let mut last_connect_attempt = Instant::now() - Duration::from_secs(60);

            while let Ok(cmd) = rx.recv() {
                // Drain any extra queued commands to only process the latest update
                let mut latest_cmd = cmd;
                while let Ok(next) = rx.try_recv() {
                    latest_cmd = next;
                }

                match latest_cmd {
                    DiscordCommand::Clear => {
                        if let Some(ref mut c) = client {
                            let _ = c.clear_activity();
                            crate::log_debug!("[Discord] Cleared Rich Presence activity");
                        }
                    }
                    DiscordCommand::Update(p) => {
                        // Connect to local Discord IPC if not currently connected (with backoff)
                        if client.is_none() {
                            if last_connect_attempt.elapsed() < Duration::from_secs(5) {
                                continue;
                            }
                            last_connect_attempt = Instant::now();
                            let app_id = get_app_id();
                            match DiscordIpcClient::new(&app_id) {
                                Ok(mut c) => match c.connect() {
                                    Ok(_) => {
                                        crate::log_debug!(
                                            "[Discord] Connected to Discord IPC with App ID: {}",
                                            app_id
                                        );
                                        client = Some(c);
                                    }
                                    Err(_e) => {
                                        crate::log_debug!("[Discord] Connect failed: {:?}", _e);
                                    }
                                },
                                Err(_e) => {
                                    crate::log_debug!(
                                        "[Discord] Failed to create Discord IPC client: {:?}",
                                        _e
                                    );
                                }
                            }
                        }

                        let Some(ref mut c) = client else {
                            continue;
                        };

                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs() as i64;

                        let details = sanitize_str(&p.title, "Listening in SyncYTM", 128);

                        let raw_state = if p.artist.trim().is_empty() {
                            format!("Room: {}", p.room_name)
                        } else {
                            format!("{} • Room: {}", p.artist.trim(), p.room_name)
                        };
                        let state_str = sanitize_str(&raw_state, "In Sync Room", 128);

                        let mut activity = Activity::new()
                            .details(&details)
                            .state(&state_str);

                        // Only show advancing timeline bar when audio is actively playing
                        if p.is_playing {
                            let start_time = (now - (p.progress_sec as i64)).max(0);
                            let mut ts = Timestamps::new().start(start_time);
                            if p.duration_sec > p.progress_sec && p.duration_sec > 0.0 {
                                let end_time = start_time + (p.duration_sec as i64);
                                ts = ts.end(end_time);
                            }
                            activity = activity.timestamps(ts);
                        }

                        // Assets (album cover / YouTube thumbnail + small badge)
                        let mut assets = Assets::new();
                        if let Some(ref thumb) = p.thumbnail {
                            let trimmed = thumb.trim();
                            if !trimmed.is_empty() {
                                assets = assets.large_image(trimmed);
                            }
                        }
                        assets = assets.large_text("SyncYTM");

                        if p.is_playing {
                            assets = assets.small_text("Playing");
                        } else {
                            assets = assets.small_text("Paused");
                        }
                        activity = activity.assets(assets);

                        // Button to join the synced listening room
                        let room_url = if !p.room_id.trim().is_empty() {
                            Some(format!(
                                "https://syncytm.netlify.app/room/{}",
                                urlencoding::encode(p.room_id.trim())
                            ))
                        } else {
                            None
                        };

                        if let Some(ref url) = room_url {
                            activity = activity.buttons(vec![Button::new("Join Room", url)]);
                        }

                        if let Err(_e) = c.set_activity(activity) {
                            crate::log_debug!(
                                "[Discord] Failed to set activity: {:?}. Reconnecting on next tick.",
                                _e
                            );
                            let _ = c.close();
                            client = None;
                            last_connect_attempt = Instant::now();
                        }
                    }
                }
            }
        });

        Self { tx }
    }

    pub fn update_presence(
        &self,
        title: &str,
        artist: &str,
        room_name: &str,
        room_id: &str,
        thumbnail: Option<&str>,
        is_playing: bool,
        progress_sec: f64,
        duration_sec: f64,
    ) {
        let _ = self.tx.send(DiscordCommand::Update(PresencePayload {
            title: title.to_string(),
            artist: artist.to_string(),
            room_name: room_name.to_string(),
            room_id: room_id.to_string(),
            thumbnail: thumbnail.map(|s| s.to_string()),
            is_playing,
            progress_sec,
            duration_sec,
        }));
    }

    pub fn clear(&self) {
        let _ = self.tx.send(DiscordCommand::Clear);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_str() {
        assert_eq!(sanitize_str("", "Fallback", 128), "Fallback");
        assert_eq!(sanitize_str("a", "Fallback", 128), "a ");
        assert_eq!(sanitize_str("Song Title", "Fallback", 128), "Song Title");

        let long = "a".repeat(150);
        let sanitized = sanitize_str(&long, "Fallback", 128);
        assert_eq!(sanitized.chars().count(), 128);
        assert!(sanitized.ends_with('…'));
    }

    #[test]
    fn test_discord_rpc_live() {
        let app_id = get_app_id();
        println!("Testing Discord IPC connection with App ID: {}", app_id);
        let mut client = match DiscordIpcClient::new(&app_id) {
            Ok(c) => c,
            Err(e) => {
                println!("Discord not installed or client creation failed: {:?}", e);
                return;
            }
        };

        if let Err(e) = client.connect() {
            println!("Could not connect to local Discord client (may not be running): {:?}", e);
            return;
        }

        println!("Connected to Discord IPC! Setting test activity...");
        let activity = Activity::new()
            .details("Testing SyncYTM")
            .state("Room: Chill Lounge")
            .assets(
                Assets::new()
                    .large_image("https://i.ytimg.com/vi/dQw4w9WgXcQ/hqdefault.jpg")
                    .large_text("SyncYTM")
                    .small_text("Playing"),
            )
            .buttons(vec![Button::new("Join Room", "https://syncytm.netlify.app/room/test")]);

        match client.set_activity(activity) {
            Ok(_) => println!("Successfully set Discord Rich Presence!"),
            Err(e) => panic!("set_activity failed: {:?}", e),
        }

        let _ = client.close();
    }
}
