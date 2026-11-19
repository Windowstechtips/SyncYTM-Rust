use serde::{Deserialize, Deserializer, Serialize};

pub fn deserialize_null_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Default + Deserialize<'de>,
{
    let opt = Option::deserialize(deserializer)?;
    Ok(opt.unwrap_or_default())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VideoItem {
    #[serde(default, alias = "videoId")]
    pub id: String,
    #[serde(default)]
    pub title: String,
    #[serde(default, alias = "channelTitle", alias = "uploader")]
    pub channel: String,
    #[serde(default)]
    pub thumbnail: Option<String>,
    #[serde(rename = "queueItemId", alias = "queue_item_id", default)]
    pub queue_item_id: Option<String>,
    #[serde(rename = "durationSec", alias = "duration_sec", alias = "duration", default)]
    pub duration_sec: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Room {
    pub id: String,
    pub host_id: String,
    pub name: String,
    #[serde(default, alias = "isPrivate", alias = "is_private")]
    pub is_private: bool,
    #[serde(default)]
    pub password_hash: Option<String>,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    pub queue: Vec<VideoItem>,
    #[serde(default, alias = "currentVideo", alias = "current_video")]
    pub current_video: Option<VideoItem>,
    #[serde(default, alias = "isPlaying", alias = "is_playing")]
    pub is_playing: bool,
    #[serde(default)]
    pub progress: Option<f64>,
    #[serde(default, alias = "activeListeners", alias = "active_listeners")]
    pub active_listeners: Option<i32>,
    #[serde(default, alias = "remoteUsers", alias = "remote_users", deserialize_with = "deserialize_null_default")]
    pub remote_users: Vec<String>,
}

impl Room {
    pub fn listeners(&self) -> i32 {
        self.active_listeners.unwrap_or(0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserSession {
    pub id: String,
    pub email: String,
    pub username: String,
    pub access_token: String,
    #[serde(default)]
    pub refresh_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub id: u64,
    pub user: String,
    pub text: String,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    pub peer_id: String,
    pub user_email: String,
    pub username: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoomStatePayload {
    #[serde(default)]
    pub url: Option<String>,
    #[serde(rename = "isPlaying", default)]
    pub is_playing: bool,
    #[serde(default)]
    pub queue: Option<Vec<VideoItem>>,
    #[serde(rename = "currentVideo", default)]
    pub current_video: Option<VideoItem>,
    #[serde(default)]
    pub time: f64,
}

/// Dynamic sync message payload exchanged across peers via Supabase Realtime broadcast
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SyncData {
    #[serde(rename = "sync-state")]
    SyncState {
        state: RoomStatePayload,
        #[serde(rename = "clientSentAt", default)]
        client_sent_at: Option<f64>,
        #[serde(rename = "hostSentAt", default)]
        host_sent_at: Option<f64>,
    },
    #[serde(rename = "request-sync")]
    RequestSync {
        #[serde(rename = "clientSentAt", default)]
        client_sent_at: Option<f64>,
    },
    #[serde(rename = "play")]
    Play {
        #[serde(default)]
        time: Option<f64>,
    },
    #[serde(rename = "pause")]
    Pause,
    #[serde(rename = "seek")]
    Seek {
        time: f64,
    },
    #[serde(rename = "play-video")]
    PlayVideo {
        video: VideoItem,
    },
    #[serde(rename = "queue-add")]
    QueueAdd {
        video: VideoItem,
    },
    #[serde(rename = "queue-add-batch")]
    QueueAddBatch {
        videos: Vec<VideoItem>,
    },
    #[serde(rename = "queue-remove")]
    QueueRemove {
        #[serde(rename = "itemId")]
        item_id: String,
    },
    #[serde(rename = "queue-remove-batch")]
    QueueRemoveBatch {
        #[serde(rename = "itemIds")]
        item_ids: Vec<String>,
    },
    #[serde(rename = "queue-clear")]
    QueueClear,
    #[serde(rename = "request-song")]
    RequestSong {
        video: VideoItem,
    },
    #[serde(rename = "chat")]
    Chat {
        payload: String,
    },
    #[serde(rename = "time-ping")]
    TimePing {
        time: f64,
        #[serde(rename = "hostSentAt", default)]
        host_sent_at: Option<f64>,
    },
    #[serde(rename = "grant-remote")]
    GrantRemote {
        #[serde(rename = "targetEmail")]
        target_email: String,
        value: bool,
    },
    #[serde(rename = "sync-remotes")]
    SyncRemotes {
        remotes: Vec<String>,
        #[serde(rename = "_fromHost", default)]
        from_host: bool,
    },
    #[serde(rename = "host-handoff")]
    HostHandoff {
        #[serde(default)]
        target: Option<String>,
        state: RoomStatePayload,
    },
    #[serde(rename = "request-host-handoff")]
    RequestHostHandoff,
}
