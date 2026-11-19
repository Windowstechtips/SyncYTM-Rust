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
                        }
                    }
                    DiscordCommand::Update(p) => {
                        // Only try connecting if disconnected and at least 10s passed since last attempt
                        if client.is_none() {
                            if last_connect_attempt.elapsed() < Duration::from_secs(10) {
                                continue;
                            }
                            last_connect_attempt = Instant::now();
                            if let Ok(mut c) = DiscordIpcClient::new(DISCORD_APP_ID) {
                                if c.connect().is_ok() {
                                    client = Some(c);
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

                        let start_time = if p.is_playing {
                            now - (p.progress_sec as i64)
                        } else {
                            now
                        };

                        let mut timestamps = Timestamps::new().start(start_time);
                        if p.is_playing && p.duration_sec > p.progress_sec && p.duration_sec > 0.0 {
                            let end_time = start_time + (p.duration_sec as i64);
                            timestamps = timestamps.end(end_time);
                        }

                        let details = if p.title.is_empty() {
                            "Listening in SyncYTM".to_string()
                        } else {
                            p.title
                        };

                        let state_str = if p.artist.is_empty() {
                            format!("Room: {}", p.room_name)
                        } else {
                            format!("by {} • Room: {}", p.artist, p.room_name)
                        };

                        let room_url = format!("https://syncytm.netlify.app/room/{}", p.room_id);

                        let activity = Activity::new()
                            .details(&details)
                            .state(&state_str)
                            .timestamps(timestamps)
                            .assets(
                                Assets::new()
                                    .large_image("ytm_logo")
                                    .large_text("SyncYTM"),
                            )
                            .buttons(vec![Button::new("Join Room", &room_url)]);

                        if c.set_activity(activity).is_err() {
                            // On IPC error, drop the client so we can reconnect cleanly with backoff
                            client = None;
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
        is_playing: bool,
        progress_sec: f64,
        duration_sec: f64,
    ) {
        let _ = self.tx.send(DiscordCommand::Update(PresencePayload {
            title: title.to_string(),
            artist: artist.to_string(),
            room_name: room_name.to_string(),
            room_id: room_id.to_string(),
            is_playing,
            progress_sec,
            duration_sec,
        }));
    }

    pub fn clear(&self) {
        let _ = self.tx.send(DiscordCommand::Clear);
    }
}
