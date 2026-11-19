use crate::audio::{AudioController, AudioEngine};
use crate::discord::DiscordManager;
use crate::models::{ChatMessage, PeerInfo, Room, RoomStatePayload, SyncData, UserSession, VideoItem};
use crate::realtime::{RealtimeClient, RealtimeEvent};
use crate::supabase::SupabaseService;
use crate::youtube::YoutubeService;
use eframe::egui::{self, Color32, Margin, RichText, Stroke, Vec2};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(PartialEq)]
enum Screen {
    Auth,
    Lobby,
    Room,
}

#[derive(PartialEq)]
enum RoomTab {
    Queue,
    Search,
    Chat,
    Listeners,
}

#[derive(Clone, Debug)]
struct PendingSongRequest {
    id: u64,
    requester: String,
    video: VideoItem,
}

type TrackResolvedPayload = (String, f64, VideoItem, f64, bool);

fn session_file_path() -> std::path::PathBuf {
    if let Ok(appdata) = std::env::var("APPDATA") {
        let dir = std::path::PathBuf::from(appdata).join("SyncYTM");
        let _ = std::fs::create_dir_all(&dir);
        return dir.join("session.json");
    }
    std::path::PathBuf::from(".session.json")
}

fn save_session(session: &UserSession) {
    let path = session_file_path();
    if let Ok(data) = serde_json::to_string_pretty(session) {
        let _ = std::fs::write(&path, data);
        crate::log_debug!("[Auth] Saved user session to {:?}", path);
    }
}

fn load_saved_session() -> Option<UserSession> {
    let path = session_file_path();
    if let Ok(data) = std::fs::read_to_string(&path) {
        if let Ok(session) = serde_json::from_str::<UserSession>(&data) {
            crate::log_debug!("[Auth] Restored saved session for {}", session.email);
            return Some(session);
        }
    }
    None
}

fn clear_saved_session() {
    let path = session_file_path();
    let _ = std::fs::remove_file(&path);
    crate::log_debug!("[Auth] Cleared saved session file");
}

pub struct SyncYtmApp {
    screen: Screen,
    active_tab: RoomTab,

    // Services
    supabase: Arc<SupabaseService>,
    youtube: Arc<YoutubeService>,
    audio: AudioController,
    _audio_stream: cpal::Stream,
    discord: Arc<DiscordManager>,

    // Auth state
    auth_email: String,
    auth_password: String,
    auth_username: String,
    auth_is_signup: bool,
    auth_error: Option<String>,
    auth_loading: bool,
    user_session: Option<UserSession>,

    // Lobby state
    rooms_list: Vec<Room>,
    lobby_loading: bool,
    lobby_error: Option<String>,
    new_room_name: String,
    new_room_private: bool,
    new_room_password: String,
    direct_room_id: String,
    direct_room_password: String,

    // Active Room state
    current_room: Option<Room>,
    realtime: Option<RealtimeClient>,
    is_host: bool,
    is_acting_host: bool,
    active_host_id: Option<String>,
    acting_host_name: Option<String>,
    remote_users: HashSet<String>,
    peers: Vec<PeerInfo>,
    song_requests: Vec<PendingSongRequest>,

    // Playback state
    current_video: Option<VideoItem>,
    queue: Vec<VideoItem>,
    is_playing: bool,
    track_duration_sec: f64,
    last_drift_ping: Instant,
    last_discord_update: Instant,
    last_seek_time: Instant,
    pending_resync: bool,
    resync_sent_at: Option<f64>,
    sync_status_toast: Option<(String, Instant)>,
    is_resolving_track: bool,
    auto_resync_at: Option<Instant>,

    // Search & Add state
    search_query: String,
    search_results: Vec<VideoItem>,
    is_searching: bool,
    is_importing: bool,
    direct_import_url: String,

    // Chat state
    chat_messages: Vec<ChatMessage>,
    new_chat_text: String,

    // Async background task receivers (100% non-blocking)
    rx_auth: Option<crossbeam_channel::Receiver<Result<UserSession, String>>>,
    rx_refresh: Option<crossbeam_channel::Receiver<Result<UserSession, String>>>,
    rx_rooms: Option<crossbeam_channel::Receiver<Result<Vec<Room>, String>>>,
    rx_room_action: Option<crossbeam_channel::Receiver<Result<Room, String>>>,
    rx_search: Option<crossbeam_channel::Receiver<Result<Vec<VideoItem>, String>>>,
    rx_import: Option<crossbeam_channel::Receiver<Result<Vec<VideoItem>, String>>>,
    tx_track_resolved: crossbeam_channel::Sender<Result<TrackResolvedPayload, String>>,
    rx_track_resolved: crossbeam_channel::Receiver<Result<TrackResolvedPayload, String>>,

    // Prefetch cache: video_id -> (audio_url, duration_sec)
    // Pre-resolves the next song while the current one plays, eliminating startup lag.
    prefetch_cache: HashMap<String, (String, f64)>,
    tx_prefetch: crossbeam_channel::Sender<(String, String, f64)>, // (video_id, url, duration)
    rx_prefetch: crossbeam_channel::Receiver<(String, String, f64)>,
    prefetching_id: Option<String>, // video_id currently being prefetched

    // Dependency auto-installer state
    deps_status: crate::deps::DependencyStatus,
    rx_deps: Option<crossbeam_channel::Receiver<crate::deps::DependencyStatus>>,
    deps_banner_dismiss_at: Option<Instant>,
}

impl SyncYtmApp {
    pub fn new() -> Self {
        let (need_ytdlp, need_ffmpeg) = crate::deps::check_missing();
        let (deps_status, rx_deps) = if need_ytdlp || need_ffmpeg {
            let (tx, rx) = crossbeam_channel::unbounded();
            tokio::spawn(async move {
                crate::deps::install_dependencies(tx, need_ytdlp, need_ffmpeg).await;
            });
            (crate::deps::DependencyStatus::Checking, Some(rx))
        } else {
            (crate::deps::DependencyStatus::Ready, None)
        };

        let supabase = Arc::new(SupabaseService::new());
        let youtube = Arc::new(YoutubeService::new());
        let audio_engine = AudioEngine::new().expect("Failed to initialize audio engine");
        let audio = audio_engine.controller;
        let _audio_stream = audio_engine.stream;
        let discord = Arc::new(DiscordManager::new());
        let (tx_track_resolved, rx_track_resolved) = crossbeam_channel::unbounded();
        let (tx_prefetch, rx_prefetch) = crossbeam_channel::unbounded();

        let saved_session = load_saved_session();
        let initial_screen = if saved_session.is_some() { Screen::Lobby } else { Screen::Auth };
        let initial_email = saved_session.as_ref().map(|s| s.email.clone()).unwrap_or_default();

        let mut app = Self {
            screen: initial_screen,
            active_tab: RoomTab::Queue,

            supabase,
            youtube,
            audio,
            _audio_stream,
            discord,

            auth_email: initial_email,
            auth_password: String::new(),
            auth_username: String::new(),
            auth_is_signup: false,
            auth_error: None,
            auth_loading: false,
            user_session: saved_session,

            rooms_list: Vec::new(),
            lobby_loading: false,
            lobby_error: None,
            new_room_name: String::new(),
            new_room_private: false,
            new_room_password: String::new(),
            direct_room_id: String::new(),
            direct_room_password: String::new(),

            current_room: None,
            realtime: None,
            is_host: false,
            is_acting_host: false,
            active_host_id: None,
            acting_host_name: None,
            remote_users: HashSet::new(),
            peers: Vec::new(),
            song_requests: Vec::new(),

            current_video: None,
            queue: Vec::new(),
            is_playing: false,
            track_duration_sec: 240.0,
            last_drift_ping: Instant::now(),
            last_discord_update: Instant::now(),
            last_seek_time: Instant::now(),
            pending_resync: false,
            resync_sent_at: None,
            sync_status_toast: None,
            is_resolving_track: false,
            auto_resync_at: None,

            search_query: String::new(),
            search_results: Vec::new(),
            is_searching: false,
            is_importing: false,
            direct_import_url: String::new(),

            chat_messages: Vec::new(),
            new_chat_text: String::new(),

            rx_auth: None,
            rx_refresh: None,
            rx_rooms: None,
            rx_room_action: None,
            rx_search: None,
            rx_import: None,
            tx_track_resolved,
            rx_track_resolved,

            prefetch_cache: HashMap::new(),
            tx_prefetch,
            rx_prefetch,
            prefetching_id: None,

            deps_status,
            rx_deps,
            deps_banner_dismiss_at: None,
        };

        if app.user_session.is_some() {
            app.load_public_rooms();
        }

        app
    }

    pub fn retry_dependency_install(&mut self) {
        let (need_ytdlp, need_ffmpeg) = crate::deps::check_missing();
        if need_ytdlp || need_ffmpeg {
            let (tx, rx) = crossbeam_channel::unbounded();
            self.deps_status = crate::deps::DependencyStatus::Checking;
            self.rx_deps = Some(rx);
            tokio::spawn(async move {
                crate::deps::install_dependencies(tx, need_ytdlp, need_ffmpeg).await;
            });
        } else {
            self.deps_status = crate::deps::DependencyStatus::Ready;
            self.rx_deps = None;
        }
    }

    fn has_controls(&self) -> bool {
        if self.is_host || self.is_acting_host {
            return true;
        }
        if let Some(ref u) = self.user_session {
            return self.remote_users.contains(&u.email) || self.remote_users.contains(&u.username);
        }
        false
    }

    fn update_host_status(&mut self) {
        let Some(ref room) = self.current_room else { return; };
        let Some(ref u) = self.user_session else { return; };

        let is_real_host = room.host_id == u.id;
        if is_real_host {
            self.is_host = true;
            self.is_acting_host = false;
            self.active_host_id = Some(u.id.clone());
            self.acting_host_name = None;
            return;
        }

        // Check if real host is currently online in the room
        let is_main_host_online = self.peers.iter().any(|p| p.peer_id == room.host_id);

        if is_main_host_online {
            if self.is_acting_host {
                self.sync_status_toast = Some((
                    "Main Host has returned! Host controls restored to Room Owner".to_string(),
                    Instant::now(),
                ));
            }
            self.is_host = false;
            self.is_acting_host = false;
            self.active_host_id = Some(room.host_id.clone());
            self.acting_host_name = None;
            return;
        }

        // Main host is away! Elect an acting host deterministically among connected peers
        let my_info = PeerInfo {
            peer_id: u.id.clone(),
            user_email: u.email.clone(),
            username: u.username.clone(),
        };
        let mut candidates: Vec<PeerInfo> = self
            .peers
            .iter()
            .filter(|p| p.peer_id != room.host_id)
            .cloned()
            .collect();
        candidates.push(my_info);

        // Priority 1: Peers who were granted Remote Control by the host
        let remote_candidates: Vec<PeerInfo> = candidates
            .iter()
            .filter(|c| self.remote_users.contains(&c.user_email))
            .cloned()
            .collect();
        let mut pool = if !remote_candidates.is_empty() {
            remote_candidates
        } else {
            candidates
        };

        // Deterministic sort by peer_id (matches web client exactly)
        pool.sort_by(|a, b| a.peer_id.cmp(&b.peer_id));
        let elected = &pool[0];

        self.active_host_id = Some(elected.peer_id.clone());
        self.acting_host_name = Some(elected.username.clone());

        if elected.peer_id == u.id {
            if !self.is_acting_host {
                self.sync_status_toast = Some((
                    "Main host is away. Host duties temporarily assigned to YOU.".to_string(),
                    Instant::now(),
                ));
            }
            self.is_host = true;
            self.is_acting_host = true;
        } else {
            if self.is_acting_host {
                self.sync_status_toast = Some((
                    format!("Acting Host duties transferred to {}", elected.username),
                    Instant::now(),
                ));
            }
            self.is_host = false;
            self.is_acting_host = false;
        }
    }

    fn play_video_local_and_broadcast(&mut self, video: VideoItem) {
        if let Some(dur) = video.duration_sec {
            self.track_duration_sec = dur;
        }

        // Stop old audio immediately so it doesn't keep playing during URL resolution
        self.audio.stop();

        self.current_video = Some(video.clone());
        self.is_playing = true;
        self.last_seek_time = Instant::now();
        self.audio.reset_finished();

        // Evict the prefetch entry for this song (we're about to use it)
        let cached = self.prefetch_cache.remove(&video.id);

        if let Some((url, duration)) = cached {
            // Cache hit → instant playback, no yt-dlp wait
            crate::log_debug!("[Prefetch] Cache HIT for '{}' — playing instantly", video.title);
            self.is_resolving_track = false;
            self.track_duration_sec = duration;
            self.audio.play(&url, 0.0);
            // Start prefetching the song after this one
            if let Some(next) = self.peek_next_video(&video.id).cloned() {
                self.start_prefetch(&next);
            }
        } else {
            // Cache miss → resolve via yt-dlp (normal path)
            crate::log_debug!("[Prefetch] Cache MISS for '{}' — resolving via yt-dlp", video.title);
            self.is_resolving_track = true;
            let yt = self.youtube.clone();
            let vid_id = video.id.clone();
            let tx = self.tx_track_resolved.clone();
            let vid_clone = video.clone();
            tokio::spawn(async move {
                match yt.resolve_audio_url(&vid_id).await {
                    Ok((url, duration)) => {
                        let _ = tx.send(Ok((url, duration, vid_clone, 0.0, true)));
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e));
                    }
                }
            });
        }

        if let Some(ref rt) = self.realtime {
            rt.send(SyncData::PlayVideo {
                video: video.clone(),
            });
        }

        if self.is_host {
            if let (Some(ref u), Some(ref r)) = (&self.user_session, &self.current_room) {
                let sb = self.supabase.clone();
                let token = u.access_token.clone();
                let r_id = r.id.clone();
                let vid = video.clone();
                let q = self.queue.clone();
                tokio::spawn(async move {
                    let _ = sb
                        .update_room_state(&token, &r_id, Some(&vid), Some(&q), Some(true), Some(0.0))
                        .await;
                });
            }
        }
    }

    fn next_track(&mut self) {
        if self.queue.is_empty() {
            return;
        }
        let next_song = if let Some(ref cur) = self.current_video {
            if let Some(idx) = self.queue.iter().position(|v| v.id == cur.id) {
                if idx + 1 < self.queue.len() {
                    Some(self.queue[idx + 1].clone())
                } else {
                    None
                }
            } else {
                Some(self.queue[0].clone())
            }
        } else {
            Some(self.queue[0].clone())
        };

        if let Some(next) = next_song {
            crate::log_debug!("[Playlist] Next track: {}", next.title);
            self.play_video_local_and_broadcast(next);
        } else {
            crate::log_debug!("[Playlist] End of playlist reached.");
            self.is_playing = false;
            self.audio.pause();
            if let Some(ref rt) = self.realtime {
                rt.send(SyncData::Pause);
            }
        }
    }

    fn prev_track(&mut self) {
        if self.queue.is_empty() {
            return;
        }
        let prev_song = if let Some(ref cur) = self.current_video {
            if let Some(idx) = self.queue.iter().position(|v| v.id == cur.id) {
                if idx > 0 {
                    Some(self.queue[idx - 1].clone())
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        if let Some(prev) = prev_song {
            crate::log_debug!("[Playlist] Previous track: {}", prev.title);
            self.play_video_local_and_broadcast(prev);
        } else {
            self.last_seek_time = Instant::now();
            self.audio.seek(0.0);
            if let Some(ref rt) = self.realtime {
                rt.send(SyncData::Seek { time: 0.0 });
            }
        }
    }

    /// Kick off a background yt-dlp resolve for `video` if it isn't already in the cache
    /// or currently being fetched. The result lands in `rx_prefetch`.
    fn start_prefetch(&mut self, video: &VideoItem) {
        let id = video.id.clone();
        // Already cached or already in-flight → skip
        if self.prefetch_cache.contains_key(&id) {
            return;
        }
        if self.prefetching_id.as_deref() == Some(&id) {
            return;
        }
        self.prefetching_id = Some(id.clone());
        let yt = self.youtube.clone();
        let tx = self.tx_prefetch.clone();
        crate::log_debug!("[Prefetch] Starting background resolve for: {}", video.title);
        tokio::spawn(async move {
            if let Ok((url, duration)) = yt.resolve_audio_url(&id).await {
                let _ = tx.send((id, url, duration));
            }
        });
    }

    /// Return the next song in the queue after `current_id`, or None.
    fn peek_next_video(&self, current_id: &str) -> Option<&VideoItem> {
        let idx = self.queue.iter().position(|v| v.id == current_id)?;
        self.queue.get(idx + 1)
    }

    fn poll_async_tasks(&mut self) {        // 0. Token Refresh task
        if let Some(ref rx) = self.rx_refresh {
            if let Ok(res) = rx.try_recv() {
                match res {
                    Ok(session) => {
                        save_session(&session);
                        self.user_session = Some(session);
                        self.lobby_error = None;
                        self.load_public_rooms();
                    }
                    Err(_) => {
                        clear_saved_session();
                        self.user_session = None;
                        self.auth_error = Some("Session expired. Please sign in again.".to_string());
                        self.screen = Screen::Auth;
                    }
                }
                self.rx_refresh = None;
            }
        }

        // 1. Auth task
        if let Some(ref rx) = self.rx_auth {
            if let Ok(res) = rx.try_recv() {
                self.auth_loading = false;
                match res {
                    Ok(session) => {
                        save_session(&session);
                        self.user_session = Some(session);
                        self.screen = Screen::Lobby;
                        self.auth_error = None;
                        self.load_public_rooms();
                    }
                    Err(e) => {
                        self.auth_error = Some(e);
                    }
                }
                self.rx_auth = None;
            }
        }

        // 2. Rooms list task
        if let Some(ref rx) = self.rx_rooms {
            if let Ok(res) = rx.try_recv() {
                self.lobby_loading = false;
                match res {
                    Ok(rooms) => {
                        self.rooms_list = rooms;
                        self.lobby_error = None;
                    }
                    Err(e) => {
                        if e.contains("401") || e.to_lowercase().contains("unauthorized") {
                            if let Some(refresh_tok) = self.user_session.as_ref().and_then(|s| s.refresh_token.clone()) {
                                self.try_refresh_token(&refresh_tok);
                            } else {
                                clear_saved_session();
                                self.user_session = None;
                                self.auth_error = Some("Session expired. Please sign in again.".to_string());
                                self.screen = Screen::Auth;
                            }
                        } else {
                            self.lobby_error = Some(e);
                        }
                    }
                }
                self.rx_rooms = None;
            }
        }

        // 3. Room create/join task
        if let Some(ref rx) = self.rx_room_action {
            if let Ok(res) = rx.try_recv() {
                match res {
                    Ok(room) => self.join_room(room),
                    Err(e) => {
                        if e.contains("401") || e.to_lowercase().contains("unauthorized") {
                            if let Some(refresh_tok) = self.user_session.as_ref().and_then(|s| s.refresh_token.clone()) {
                                self.try_refresh_token(&refresh_tok);
                            } else {
                                clear_saved_session();
                                self.user_session = None;
                                self.auth_error = Some("Session expired. Please sign in again.".to_string());
                                self.screen = Screen::Auth;
                            }
                        } else {
                            self.lobby_error = Some(e);
                        }
                    }
                }
                self.rx_room_action = None;
            }
        }

        // 4. Search task
        if let Some(ref rx) = self.rx_search {
            if let Ok(res) = rx.try_recv() {
                self.is_searching = false;
                if let Ok(results) = res {
                    self.search_results = results;
                }
                self.rx_search = None;
            }
        }

        // 5. Import playlist task
        if let Some(ref rx) = self.rx_import {
            if let Ok(res) = rx.try_recv() {
                self.is_importing = false;
                if let Ok(videos) = res {
                    for v in &videos {
                        self.queue.push(v.clone());
                    }
                    if let Some(ref rt) = self.realtime {
                        rt.send(SyncData::QueueAddBatch { videos });
                    }
                }
                self.rx_import = None;
            }
        }

        // 6. Track Audio Resolution task
        while let Ok(res) = self.rx_track_resolved.try_recv() {
            self.is_resolving_track = false;
            match res {
                Ok((url, duration, _video, start_time, play_flag)) => {
                    self.track_duration_sec = duration;
                    self.audio.play(&url, start_time);
                    if !play_flag {
                        self.audio.pause();
                    } else if !self.is_host {
                        // Automatically trigger precision resync 2.5s after audio loads and starts playing
                        self.auto_resync_at = Some(Instant::now() + Duration::from_millis(2500));
                    }
                    crate::log_debug!("[Audio] Track started: {} ({:.1}s) at {:.1}s", _video.title, duration, start_time);
                    // Kick off prefetch for the next song now that this one is playing
                    if let Some(next) = self.peek_next_video(&_video.id).cloned() {
                        self.start_prefetch(&next);
                    }
                }
                Err(_e) => {
                    crate::log_debug!("[Audio] Track resolution failed: {}", _e);
                }
            }
        }

        // 6b. Prefetch results — store resolved URLs in cache for instant playback
        while let Ok((id, url, duration)) = self.rx_prefetch.try_recv() {
            crate::log_debug!("[Prefetch] Cached audio URL for video_id: {}", id);
            self.prefetch_cache.insert(id.clone(), (url, duration));
            if self.prefetching_id.as_deref() == Some(&id) {
                self.prefetching_id = None;
            }
        }

        // 7. Dependency auto-installer task
        if let Some(ref rx) = self.rx_deps {
            while let Ok(status) = rx.try_recv() {
                if status.is_ready() {
                    self.deps_banner_dismiss_at = Some(Instant::now() + Duration::from_secs(4));
                }
                self.deps_status = status;
            }
        }
    }

    fn handle_realtime_events(&mut self) {
        let events: Vec<RealtimeEvent> = if let Some(ref rt) = self.realtime {
            let mut list = Vec::new();
            while let Some(ev) = rt.try_recv() {
                list.push(ev);
            }
            list
        } else {
            Vec::new()
        };

        for ev in events {
            match ev {
                RealtimeEvent::Connected => {
                    self.sync_status_toast =
                        Some(("Connected to Sync Room".to_string(), Instant::now()));
                    self.update_host_status();
                    if self.is_host && !self.is_acting_host {
                        if let Some(ref rt) = self.realtime {
                            rt.send(SyncData::RequestHostHandoff);
                        }
                    } else if !self.is_host {
                        self.trigger_resync();
                    }
                }
                RealtimeEvent::Disconnected => {
                    self.sync_status_toast =
                        Some(("Reconnecting...".to_string(), Instant::now()));
                }
                RealtimeEvent::PeersUpdated(peers) => {
                    self.peers = peers;
                    self.update_host_status();
                }
                RealtimeEvent::Broadcast {
                    sender_email,
                    sender_name,
                    data,
                    ..
                } => {
                    self.process_sync_data(data, sender_email, sender_name);
                }
            }
        }
    }

    fn trigger_resync(&mut self) {
        let now_ms = current_epoch_ms();
        self.pending_resync = true;
        self.resync_sent_at = Some(now_ms);
        if let Some(ref rt) = self.realtime {
            rt.send(SyncData::RequestSync {
                client_sent_at: Some(now_ms),
            });
        }
    }

    fn process_sync_data(&mut self, data: SyncData, sender_email: String, sender_name: String) {
        match data {
            SyncData::SyncState {
                state,
                client_sent_at,
                host_sent_at,
            } => {
                if self.is_host {
                    return;
                }

                if let Some(q) = state.queue {
                    self.queue = q;
                }

                let now_ms = current_epoch_ms();

                // Compute precise network latency compensation
                let sent = client_sent_at.or(self.resync_sent_at);
                let (rtt_ms, compensated_time) = if let Some(sent_at) = sent {
                    let rtt = (now_ms - sent_at).max(0.0);
                    let one_way = (rtt / 2.0) / 1000.0;
                    let comp = state.time + if state.is_playing { one_way } else { 0.0 };
                    (rtt, comp)
                } else if let Some(host_sent) = host_sent_at {
                    let transit_ms = (now_ms - host_sent).max(0.0);
                    let transit_sec = (transit_ms / 1000.0).min(5.0);
                    let comp = state.time + if state.is_playing { transit_sec } else { 0.0 };
                    (transit_ms, comp)
                } else {
                    (0.0, state.time)
                };

                let was_resync = self.pending_resync || client_sent_at.is_some();

                if let Some(v) = state.current_video {
                    if let Some(dur) = v.duration_sec {
                        self.track_duration_sec = dur;
                    }

                    let is_new = self.current_video.as_ref().map(|c| &c.id) != Some(&v.id);
                    self.current_video = Some(v.clone());

                    if is_new {
                        let yt = self.youtube.clone();
                        let vid_id = v.id.clone();
                        let play_flag = state.is_playing;
                        let comp_time = compensated_time;
                        let tx = self.tx_track_resolved.clone();
                        let vid_clone = v.clone();

                        self.last_seek_time = Instant::now();
                        self.is_resolving_track = true;
                        self.audio.reset_finished();

                        tokio::spawn(async move {
                            match yt.resolve_audio_url(&vid_id).await {
                                Ok((url, duration)) => {
                                    let _ = tx.send(Ok((url, duration, vid_clone, comp_time, play_flag)));
                                }
                                Err(e) => {
                                    let _ = tx.send(Err(e));
                                }
                            }
                        });
                    } else if was_resync || (self.audio.get_time_sec() - compensated_time).abs() > 2.5 {
                        self.last_seek_time = Instant::now();
                        self.audio.seek(compensated_time);
                    }
                }

                self.is_playing = state.is_playing;
                if self.is_playing {
                    self.audio.resume();
                } else {
                    self.audio.pause();
                }

                if was_resync {
                    self.pending_resync = false;
                    self.resync_sent_at = None;
                    self.sync_status_toast = Some((
                        format!("Resynced ({:.0}ms latency, seek to {})", rtt_ms, format_duration(compensated_time)),
                        Instant::now(),
                    ));
                }
            }
            SyncData::RequestSync { client_sent_at } => {
                if self.is_host {
                    if let Some(ref rt) = self.realtime {
                        let state = RoomStatePayload {
                            url: self
                                .current_video
                                .as_ref()
                                .map(|v| format!("https://www.youtube.com/watch?v={}", v.id)),
                            is_playing: self.is_playing,
                            queue: Some(self.queue.clone()),
                            current_video: self.current_video.clone(),
                            time: self.audio.get_time_sec(),
                        };
                        rt.send(SyncData::SyncState {
                            state,
                            client_sent_at,
                            host_sent_at: Some(current_epoch_ms()),
                        });
                    }
                }
            }
            SyncData::RequestSong { video } => {
                if self.is_host {
                    let requester = if sender_name.is_empty() { sender_email.clone() } else { sender_name.clone() };
                    self.sync_status_toast = Some((
                        format!("Song request: {} by {}", video.title, requester),
                        Instant::now(),
                    ));
                    self.song_requests.push(PendingSongRequest {
                        id: SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64,
                        requester,
                        video,
                    });
                }
            }
            SyncData::Play { time } => {
                self.is_playing = true;
                self.audio.resume();
                if let Some(t) = time {
                    if (self.audio.get_time_sec() - t).abs() > 1.2
                        && self.last_seek_time.elapsed() > Duration::from_secs(3)
                    {
                        self.last_seek_time = Instant::now();
                        self.audio.seek(t);
                    }
                }
            }
            SyncData::Pause => {
                self.is_playing = false;
                self.audio.pause();
            }
            SyncData::Seek { time } => {
                self.last_seek_time = Instant::now();
                self.audio.seek(time);
            }
            SyncData::PlayVideo { video } => {
                if let Some(dur) = video.duration_sec {
                    self.track_duration_sec = dur;
                }
                self.current_video = Some(video.clone());
                self.is_playing = true;
                self.last_seek_time = Instant::now();
                // Stop old audio immediately — prevents the old song from
                // continuing to play while yt-dlp resolves the new URL
                self.audio.stop();
                self.audio.reset_finished();

                // Evict the prefetch entry for this song (we're about to use it)
                let cached = self.prefetch_cache.remove(&video.id);

                if let Some((url, duration)) = cached {
                    // Cache hit → instant playback
                    crate::log_debug!("[Prefetch] Cache HIT (peer) for '{}' — playing instantly", video.title);
                    self.is_resolving_track = false;
                    self.track_duration_sec = duration;
                    self.audio.play(&url, 0.0);
                    self.auto_resync_at = Some(Instant::now() + Duration::from_millis(2500));
                    // Prefetch the next track
                    if let Some(next) = self.peek_next_video(&video.id).cloned() {
                        self.start_prefetch(&next);
                    }
                } else {
                    // Cache miss → resolve via yt-dlp
                    crate::log_debug!("[Prefetch] Cache MISS (peer) for '{}' — resolving via yt-dlp", video.title);
                    self.is_resolving_track = true;
                    let yt = self.youtube.clone();
                    let vid_id = video.id.clone();
                    let tx = self.tx_track_resolved.clone();
                    let vid_clone = video.clone();
                    tokio::spawn(async move {
                        match yt.resolve_audio_url(&vid_id).await {
                            Ok((url, duration)) => {
                                let _ = tx.send(Ok((url, duration, vid_clone, 0.0, true)));
                            }
                            Err(e) => {
                                let _ = tx.send(Err(e));
                            }
                        }
                    });
                }
            }
            SyncData::QueueAdd { video } => {
                if !self.queue.iter().any(|v| v.id == video.id) {
                    self.queue.push(video);
                }
            }
            SyncData::QueueAddBatch { videos } => {
                for v in videos {
                    if !self.queue.iter().any(|item| item.id == v.id) {
                        self.queue.push(v);
                    }
                }
            }
            SyncData::QueueRemove { item_id } => {
                self.queue.retain(|v| v.id != item_id && v.queue_item_id.as_deref() != Some(&item_id));
            }
            SyncData::QueueRemoveBatch { item_ids } => {
                let set: HashSet<String> = item_ids.into_iter().collect();
                self.queue.retain(|v| !set.contains(&v.id) && !v.queue_item_id.as_ref().map_or(false, |qid| set.contains(qid)));
            }
            SyncData::QueueClear => {
                self.queue.clear();
            }
            SyncData::Chat { payload } => {
                self.chat_messages.push(ChatMessage {
                    id: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64,
                    user: if sender_name.is_empty() { sender_email } else { sender_name },
                    text: payload,
                    timestamp: chrono_now_time(),
                });
            }
            SyncData::TimePing { time, host_sent_at } => {
                if !self.is_host && self.is_playing && !self.is_resolving_track {
                    let transit = if let Some(sent) = host_sent_at {
                        let now_ms = current_epoch_ms();
                        ((now_ms - sent).max(0.0) / 1000.0).min(5.0)
                    } else {
                        0.0
                    };
                    let target = time + transit;
                    let drift = (self.audio.get_time_sec() - target).abs();
                    if drift > 3.0 && self.last_seek_time.elapsed() > Duration::from_secs(6) {
                        self.last_seek_time = Instant::now();
                        self.audio.seek(target);
                    }
                }
            }
            SyncData::GrantRemote { target_email, value } => {
                if value {
                    self.remote_users.insert(target_email);
                } else {
                    self.remote_users.remove(&target_email);
                }
            }
            SyncData::SyncRemotes { remotes, .. } => {
                self.remote_users = remotes.into_iter().collect();
            }
            SyncData::RequestHostHandoff => {
                if self.is_acting_host {
                    if let Some(ref rt) = self.realtime {
                        let cur_video = self.current_video.clone();
                        let payload = RoomStatePayload {
                            url: cur_video.as_ref().map(|v| format!("https://www.youtube.com/watch?v={}", v.id)),
                            is_playing: self.is_playing,
                            time: self.audio.get_time_sec(),
                            queue: Some(self.queue.clone()),
                            current_video: cur_video,
                        };
                        if let Some(ref room) = self.current_room {
                            rt.send(SyncData::HostHandoff {
                                target: Some(room.host_id.clone()),
                                state: payload,
                            });
                        }
                    }
                }
            }
            SyncData::HostHandoff { state, .. } => {
                let is_real_host = self.current_room.as_ref().map_or(false, |r| {
                    self.user_session.as_ref().map_or(false, |u| r.host_id == u.id)
                });
                if is_real_host {
                    eprintln!("[Handoff] Reclaiming live room state from acting host");
                    if let Some(q) = state.queue {
                        self.queue = q;
                    }
                    if let Some(ref v) = state.current_video {
                        if let Some(dur) = v.duration_sec {
                            self.track_duration_sec = dur;
                        }
                    }
                    self.current_video = state.current_video.clone();
                    self.is_playing = state.is_playing;
                    if let Some(ref v) = state.current_video {
                        let yt = self.youtube.clone();
                        let vid_id = v.id.clone();
                        let tx = self.tx_track_resolved.clone();
                        let vid_clone = v.clone();
                        let comp_time = state.time;
                        let is_play = state.is_playing;
                        self.is_resolving_track = true;
                        self.audio.reset_finished();
                        tokio::spawn(async move {
                            match yt.resolve_audio_url(&vid_id).await {
                                Ok((url, duration)) => {
                                    let _ = tx.send(Ok((url, duration, vid_clone, comp_time, is_play)));
                                }
                                Err(e) => {
                                    let _ = tx.send(Err(e));
                                }
                            }
                        });
                    } else {
                        self.audio.seek(state.time);
                    }
                    self.sync_status_toast = Some(("Live room state reclaimed from temporary host".to_string(), Instant::now()));
                }
            }
        }
    }

    fn check_periodic_tasks(&mut self) {
        if let Some(target) = self.auto_resync_at {
            if Instant::now() >= target {
                self.auto_resync_at = None;
                if !self.is_host && self.is_playing {
                    eprintln!("[Sync] Auto post-load resync firing to align with host");
                    self.trigger_resync();
                }
            }
        }

        if self.is_host && self.is_playing && self.last_drift_ping.elapsed() > Duration::from_secs(6) {
            self.last_drift_ping = Instant::now();
            if let Some(ref rt) = self.realtime {
                rt.send(SyncData::TimePing {
                    time: self.audio.get_time_sec(),
                    host_sent_at: Some(current_epoch_ms()),
                });
            }
        }

        if self.last_discord_update.elapsed() > Duration::from_secs(2) {
            self.last_discord_update = Instant::now();
            if let Some(ref r) = self.current_room {
                let title = self.current_video.as_ref().map(|v| v.title.as_str()).unwrap_or("");
                let artist = self.current_video.as_ref().map(|v| v.channel.as_str()).unwrap_or("");
                let thumbnail = self.current_video.as_ref().and_then(|v| v.thumbnail.as_deref());
                self.discord.update_presence(
                    title,
                    artist,
                    &r.name,
                    &r.id,
                    thumbnail,
                    self.is_playing,
                    self.audio.get_time_sec(),
                    self.track_duration_sec,
                );
            }
        }

        // Auto-advance queue when current song ends (HOST ONLY!)
        if self.is_host && self.is_playing && !self.is_resolving_track && self.audio.is_finished() {
            self.audio.reset_finished();
            self.next_track();
        }
    }
}

fn chrono_now_time() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let mins = (secs / 60) % 60;
    let hours = (secs / 3600 + 5) % 24;
    format!("{:02}:{:02}", hours, mins)
}

fn current_epoch_ms() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as f64
}

fn format_duration(sec: f64) -> String {
    let total_s = sec.max(0.0) as u64;
    let m = total_s / 60;
    let s = total_s % 60;
    format!("{:02}:{:02}", m, s)
}

impl eframe::App for SyncYtmApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        ctx.request_repaint_after(Duration::from_millis(100));

        self.poll_async_tasks();
        self.handle_realtime_events();
        self.check_periodic_tasks();

        self.ui_deps_banner(ctx);

        egui::CentralPanel::default().show(ctx, |ui| match self.screen {
            Screen::Auth => self.ui_auth(ui),
            Screen::Lobby => self.ui_lobby(ui),
            Screen::Room => self.ui_room(ui),
        });
    }
}

// ─── UI Color Palette ───────────────────────────────────────────────────────
const BG_BASE: Color32       = Color32::from_rgb(13, 13, 15);
const BG_PANEL: Color32      = Color32::from_rgb(18, 18, 24);
const BG_CARD: Color32       = Color32::from_rgb(24, 24, 32);
const BG_ACTIVE: Color32     = Color32::from_rgb(30, 15, 17);
const BORDER: Color32        = Color32::from_rgb(40, 40, 52);
const BORDER_ACTIVE: Color32 = Color32::from_rgb(220, 50, 55);
const ACCENT: Color32        = Color32::from_rgb(232, 54, 61);
const TEXT_PRIMARY: Color32  = Color32::from_rgb(240, 240, 245);
const TEXT_MUTED: Color32    = Color32::from_rgb(120, 120, 135);
const TEXT_SUBTLE: Color32   = Color32::from_rgb(80, 80, 92);
const SUCCESS: Color32       = Color32::from_rgb(72, 199, 142);
const WARNING: Color32       = Color32::from_rgb(240, 180, 40);
const INFO: Color32          = Color32::from_rgb(99, 179, 237);

impl SyncYtmApp {
    fn ui_deps_banner(&mut self, ctx: &egui::Context) {
        let show = match &self.deps_status {
            crate::deps::DependencyStatus::Ready => {
                if let Some(dismiss_at) = self.deps_banner_dismiss_at {
                    Instant::now() < dismiss_at
                } else {
                    false
                }
            }
            _ => true,
        };

        if !show {
            return;
        }

        let mut retry_clicked = false;

        egui::TopBottomPanel::top("deps_install_banner")
            .frame(
                egui::Frame::none()
                    .fill(BG_CARD)
                    .stroke(Stroke::new(1.0_f32, BORDER))
                    .inner_margin(Margin::symmetric(16.0, 8.0)),
            )
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    match &self.deps_status {
                        crate::deps::DependencyStatus::Checking => {
                            ui.spinner();
                            ui.label(
                                RichText::new("Checking required media components...")
                                    .size(12.0)
                                    .color(TEXT_PRIMARY),
                            );
                        }
                        crate::deps::DependencyStatus::Downloading {
                            item,
                            progress_pct,
                            details,
                        } => {
                            ui.spinner();
                            ui.label(
                                RichText::new(format!("Downloading required media tool ({})", item))
                                    .size(12.0)
                                    .color(TEXT_PRIMARY),
                            );
                            ui.add_space(8.0);
                            ui.add(
                                egui::ProgressBar::new(*progress_pct)
                                    .desired_width(180.0)
                                    .text(details.as_str()),
                            );
                        }
                        crate::deps::DependencyStatus::Extracting { item } => {
                            ui.spinner();
                            ui.label(
                                RichText::new(format!("Installing {}...", item))
                                    .size(12.0)
                                    .color(TEXT_PRIMARY),
                            );
                        }
                        crate::deps::DependencyStatus::Ready => {
                            ui.label(
                                RichText::new("✓ Media tools (yt-dlp & ffmpeg) are ready!")
                                    .size(12.0)
                                    .color(SUCCESS),
                            );
                        }
                        crate::deps::DependencyStatus::Error(err) => {
                            ui.label(
                                RichText::new(format!("⚠️ Media tool setup error: {}", err))
                                    .size(12.0)
                                    .color(BORDER_ACTIVE),
                            );
                            ui.add_space(8.0);
                            if ui.button("Retry Installation").clicked() {
                                retry_clicked = true;
                            }
                        }
                    }
                });
            });

        if retry_clicked {
            self.retry_dependency_install();
        }
    }

    fn ui_auth(&mut self, ui: &mut egui::Ui) {
        // Fill the whole panel background
        let rect = ui.available_rect_before_wrap();
        ui.painter().rect_filled(rect, 0.0, BG_BASE);

        ui.vertical_centered(|ui| {
            ui.add_space(60.0);

            // Logo / branding
            ui.label(RichText::new("SyncYTM").size(28.0).strong().color(ACCENT));
            ui.add_space(4.0);
            ui.label(RichText::new("Synchronized YouTube Music").size(13.0).color(TEXT_MUTED));
            ui.add_space(28.0);

            // Auth card
            egui::Frame::none()
                .fill(BG_CARD)
                .stroke(Stroke::new(1.0_f32, BORDER))
                .inner_margin(Margin::same(24.0))
                .rounding(6.0)
                .show(ui, |ui| {
                    ui.set_max_width(340.0);

                    if self.auth_is_signup {
                        ui.label(RichText::new("Username").size(12.0).color(TEXT_MUTED));
                        ui.add_space(2.0);
                        ui.add(
                            egui::TextEdit::singleline(&mut self.auth_username)
                                .desired_width(f32::INFINITY)
                        );
                        ui.add_space(10.0);
                    }

                    ui.label(RichText::new("Email").size(12.0).color(TEXT_MUTED));
                    ui.add_space(2.0);
                    ui.add(
                        egui::TextEdit::singleline(&mut self.auth_email)
                            .desired_width(f32::INFINITY)
                    );
                    ui.add_space(10.0);

                    ui.label(RichText::new("Password").size(12.0).color(TEXT_MUTED));
                    ui.add_space(2.0);
                    ui.add(
                        egui::TextEdit::singleline(&mut self.auth_password)
                            .password(true)
                            .desired_width(f32::INFINITY)
                    );
                    ui.add_space(16.0);

                    if let Some(ref err) = self.auth_error.clone() {
                        ui.label(RichText::new(err).color(Color32::from_rgb(255, 100, 100)).size(12.0));
                        ui.add_space(8.0);
                    }

                    let btn_text = if self.auth_loading {
                        "Please wait…"
                    } else if self.auth_is_signup {
                        "Create Account"
                    } else {
                        "Sign In"
                    };

                    let btn = egui::Button::new(
                        RichText::new(btn_text).color(Color32::WHITE).strong().size(13.0),
                    )
                    .fill(if self.auth_loading { TEXT_SUBTLE } else { ACCENT })
                    .min_size(Vec2::new(ui.available_width(), 36.0));

                    if ui.add_enabled(!self.auth_loading, btn).clicked() {
                        let sb = self.supabase.clone();
                        let email = self.auth_email.trim().to_string();
                        let pwd = self.auth_password.clone();
                        let username = self.auth_username.trim().to_string();
                        let is_signup = self.auth_is_signup;

                        let (tx, rx) = crossbeam_channel::bounded::<Result<UserSession, String>>(1);
                        self.auth_loading = true;
                        self.auth_error = None;

                        tokio::spawn(async move {
                            let res = if is_signup {
                                sb.signup(&email, &pwd, &username).await
                            } else {
                                sb.login(&email, &pwd).await
                            };
                            let _ = tx.send(res);
                        });

                        self.rx_auth = Some(rx);
                    }

                    ui.add_space(10.0);

                    let toggle_text = if self.auth_is_signup {
                        "Already have an account? Sign In"
                    } else {
                        "No account yet? Sign Up"
                    };
                    ui.centered_and_justified(|ui| {
                        if ui.selectable_label(false, RichText::new(toggle_text).color(TEXT_MUTED).size(12.0)).clicked() {
                            self.auth_is_signup = !self.auth_is_signup;
                            self.auth_error = None;
                        }
                    });
                });
        });
    }

    fn try_refresh_token(&mut self, refresh_token: &str) {
        let (tx, rx) = crossbeam_channel::bounded(1);
        let sb = self.supabase.clone();
        let tok = refresh_token.to_string();

        tokio::spawn(async move {
            let res = sb.refresh_session(&tok).await;
            let _ = tx.send(res);
        });

        self.rx_refresh = Some(rx);
    }

    fn load_public_rooms(&mut self) {
        let Some(ref u) = self.user_session else { return; };
        let sb = self.supabase.clone();
        let token = u.access_token.clone();
        self.lobby_loading = true;

        let (tx, rx) = crossbeam_channel::bounded(1);
        tokio::spawn(async move {
            let res = sb.list_rooms(&token).await;
            let _ = tx.send(res);
        });

        self.rx_rooms = Some(rx);
    }

    fn ui_lobby(&mut self, ui: &mut egui::Ui) {
        // Top bar
        ui.horizontal(|ui| {
            ui.label(RichText::new("SyncYTM").size(18.0).strong().color(ACCENT));
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                if ui.add(egui::Button::new(
                    RichText::new("Sign Out").size(12.0).color(TEXT_MUTED)
                ).fill(Color32::TRANSPARENT)).clicked() {
                    self.leave_room();
                    clear_saved_session();
                    self.user_session = None;
                    self.screen = Screen::Auth;
                }
                if let Some(ref u) = self.user_session {
                    ui.label(RichText::new(&u.username).strong().color(ACCENT).size(13.0));
                }
            });
        });

        ui.add(egui::Separator::default().spacing(8.0));

        let user_id = self.user_session.as_ref().map(|u| u.id.clone()).unwrap_or_default();
        let my_rooms: Vec<Room> = self.rooms_list.iter().filter(|r| r.host_id == user_id).cloned().collect();
        let other_rooms: Vec<Room> = self.rooms_list.iter().filter(|r| r.host_id != user_id).cloned().collect();

        // Two columns (60/40 split with right padding so boxes don't touch window edge)
        let total_w = ui.available_width() - 10.0;
        let left_w = total_w * 0.58 - 6.0;
        let right_w = total_w * 0.42 - 6.0;

        ui.horizontal_top(|ui| {
            // ── LEFT COLUMN ──────────────────────────────────────────────
            ui.vertical(|ui| {
                ui.set_width(left_w);

                ui.horizontal(|ui| {
                    ui.label(RichText::new("Rooms").size(15.0).strong().color(TEXT_PRIMARY));
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        let refresh_lbl = if self.lobby_loading { "Refreshing…" } else { "⟳ Refresh" };
                        if ui.add(egui::Button::new(
                            RichText::new(refresh_lbl).size(12.0).color(TEXT_MUTED)
                        ).fill(Color32::TRANSPARENT)).clicked() {
                            self.load_public_rooms();
                        }
                    });
                });

                if let Some(ref err) = self.lobby_error.clone() {
                    ui.label(RichText::new(format!("⚠ {}", err)).color(Color32::from_rgb(255, 100, 100)).size(12.0));
                }
                ui.add_space(6.0);

                let mut room_to_join: Option<Room> = None;

                egui::ScrollArea::vertical().max_height(ui.available_height() - 4.0).show(ui, |ui| {
                    // — My Rooms —
                    ui.label(RichText::new("MY ROOMS").size(11.0).color(TEXT_SUBTLE).strong());
                    ui.add_space(4.0);

                    if my_rooms.is_empty() {
                        ui.label(RichText::new("You haven't created any rooms yet.").color(TEXT_MUTED).size(12.0));
                    } else {
                        for r in &my_rooms {
                            let rc = r.clone();
                            egui::Frame::none()
                                .fill(BG_CARD)
                                .stroke(Stroke::new(1.0_f32, BORDER_ACTIVE))
                                .inner_margin(Margin::same(10.0))
                                .rounding(4.0)
                                .show(ui, |ui| {
                                    ui.horizontal(|ui| {
                                        ui.vertical(|ui| {
                                            ui.horizontal(|ui| {
                                                ui.label(RichText::new(&r.name).strong().size(14.0).color(TEXT_PRIMARY));
                                                if r.is_private {
                                                    ui.label(RichText::new("🔒").size(11.0).color(TEXT_MUTED));
                                                }
                                            });
                                            ui.label(RichText::new(format!("{} listeners", r.listeners())).size(11.0).color(TEXT_MUTED));
                                        });
                                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                            if ui.add(egui::Button::new(
                                                RichText::new("Enter").color(Color32::WHITE).strong().size(12.0)
                                            ).fill(ACCENT)).clicked() {
                                                room_to_join = Some(rc);
                                            }
                                        });
                                    });
                                });
                            ui.add_space(4.0);
                        }
                    }

                    ui.add_space(10.0);
                    ui.add(egui::Separator::default().spacing(6.0));
                    ui.add_space(4.0);

                    // — Public / Explore Rooms —
                    ui.label(RichText::new("EXPLORE ROOMS").size(11.0).color(TEXT_SUBTLE).strong());
                    ui.add_space(4.0);

                    if other_rooms.is_empty() && !self.lobby_loading {
                        ui.label(RichText::new("No other rooms found.").color(TEXT_MUTED).size(12.0));
                    } else {
                        for r in &other_rooms {
                            let rc = r.clone();
                            egui::Frame::none()
                                .fill(BG_CARD)
                                .stroke(Stroke::new(1.0_f32, BORDER))
                                .inner_margin(Margin::same(10.0))
                                .rounding(4.0)
                                .show(ui, |ui| {
                                    ui.horizontal(|ui| {
                                        ui.vertical(|ui| {
                                            ui.horizontal(|ui| {
                                                ui.label(RichText::new(&r.name).strong().size(14.0).color(TEXT_PRIMARY));
                                                if r.is_private {
                                                    ui.label(RichText::new("🔒").size(11.0).color(TEXT_MUTED));
                                                }
                                            });
                                            ui.label(RichText::new(format!("{} listeners", r.listeners())).size(11.0).color(TEXT_MUTED));
                                        });
                                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                            if ui.button(RichText::new("Join").size(12.0).color(TEXT_PRIMARY)).clicked() {
                                                room_to_join = Some(rc);
                                            }
                                        });
                                    });
                                });
                            ui.add_space(4.0);
                        }
                    }
                });

                if let Some(r) = room_to_join {
                    self.join_room(r);
                }
            });

            ui.add_space(12.0);

            // ── RIGHT COLUMN ─────────────────────────────────────────────
            ui.vertical(|ui| {
                ui.set_width(right_w);

                // Create Room
                ui.label(RichText::new("CREATE ROOM").size(11.0).color(TEXT_SUBTLE).strong());
                ui.add_space(4.0);
                egui::Frame::none()
                    .fill(BG_CARD)
                    .stroke(Stroke::new(1.0_f32, BORDER))
                    .inner_margin(Margin::same(14.0))
                    .rounding(4.0)
                    .show(ui, |ui| {
                        ui.label(RichText::new("Room Name").size(12.0).color(TEXT_MUTED));
                        ui.add_space(2.0);
                        ui.add(egui::TextEdit::singleline(&mut self.new_room_name).desired_width(f32::INFINITY));
                        ui.add_space(8.0);
                        ui.checkbox(&mut self.new_room_private, RichText::new("Private (password-protected)").size(12.0).color(TEXT_MUTED));
                        if self.new_room_private {
                            ui.add_space(4.0);
                            ui.label(RichText::new("Password").size(12.0).color(TEXT_MUTED));
                            ui.add(egui::TextEdit::singleline(&mut self.new_room_password).password(true).desired_width(f32::INFINITY));
                        }
                        ui.add_space(10.0);
                        let create_btn = egui::Button::new(
                            RichText::new("Create & Host").color(Color32::WHITE).strong().size(13.0)
                        )
                        .fill(ACCENT)
                        .min_size(Vec2::new(ui.available_width(), 32.0));
                        if ui.add(create_btn).clicked() && !self.new_room_name.trim().is_empty() {
                            self.create_room_action();
                        }
                    });

                ui.add_space(14.0);

                // Join by ID
                ui.label(RichText::new("JOIN BY ID").size(11.0).color(TEXT_SUBTLE).strong());
                ui.add_space(4.0);
                egui::Frame::none()
                    .fill(BG_CARD)
                    .stroke(Stroke::new(1.0_f32, BORDER))
                    .inner_margin(Margin::same(14.0))
                    .rounding(4.0)
                    .show(ui, |ui| {
                        ui.label(RichText::new("Room UUID or invite link").size(12.0).color(TEXT_MUTED));
                        ui.add_space(2.0);
                        ui.add(egui::TextEdit::singleline(&mut self.direct_room_id).desired_width(f32::INFINITY));
                        ui.add_space(6.0);
                        ui.label(RichText::new("Password (if private)").size(12.0).color(TEXT_MUTED));
                        ui.add_space(2.0);
                        ui.add(egui::TextEdit::singleline(&mut self.direct_room_password).password(true).desired_width(f32::INFINITY));
                        ui.add_space(10.0);
                        let join_btn = egui::Button::new(
                            RichText::new("Connect to Room").size(13.0).color(TEXT_PRIMARY)
                        )
                        .min_size(Vec2::new(ui.available_width(), 32.0));
                        if ui.add(join_btn).clicked() && !self.direct_room_id.trim().is_empty() {
                            self.join_by_id_action();
                        }
                    });
            });
        });
    }

    fn create_room_action(&mut self) {
        let Some(ref u) = self.user_session else { return; };
        let sb = self.supabase.clone();
        let token = u.access_token.clone();
        let host_id = u.id.clone();
        let name = self.new_room_name.trim().to_string();
        let is_private = self.new_room_private;
        let pwd = if is_private { Some(self.new_room_password.clone()) } else { None };

        let (tx, rx) = crossbeam_channel::bounded(1);
        tokio::spawn(async move {
            let res = sb.create_room(&token, &host_id, &name, is_private, pwd.as_deref()).await;
            let _ = tx.send(res);
        });

        self.rx_room_action = Some(rx);
    }

    fn join_by_id_action(&mut self) {
        let Some(ref u) = self.user_session else { return; };
        let sb = self.supabase.clone();
        let token = u.access_token.clone();

        // Accept a full syncytm.netlify.app/room/<id> link or a bare UUID
        let raw = self.direct_room_id.trim().to_string();
        let room_id = if let Some(idx) = raw.find("/room/") {
            raw[idx + "/room/".len()..].trim_matches('/').to_string()
        } else {
            raw
        };

        let (tx, rx) = crossbeam_channel::bounded(1);
        tokio::spawn(async move {
            let res = sb.get_room(&token, &room_id).await;
            let _ = tx.send(res);
        });

        self.rx_room_action = Some(rx);
    }

    fn leave_room(&mut self) {
        self.audio.stop();
        self.discord.clear();
        if let Some(ref rt) = self.realtime {
            rt.close();
        }
        self.realtime = None;
        self.current_room = None;
        self.auto_resync_at = None;
        self.is_acting_host = false;
        self.active_host_id = None;
        self.acting_host_name = None;
        self.peers.clear();
        self.song_requests.clear();
        self.chat_messages.clear();
        self.queue.clear();
        self.current_video = None;
        self.is_playing = false;
        self.prefetch_cache.clear();
        self.prefetching_id = None;
        self.screen = Screen::Lobby;
        self.load_public_rooms();
    }

    fn join_room(&mut self, room: Room) {
        let Some(ref u) = self.user_session else { return; };
        self.is_host = room.host_id == u.id;
        self.is_acting_host = false;
        self.active_host_id = Some(room.host_id.clone());
        self.acting_host_name = None;
        self.queue = room.queue.clone();
        self.current_video = room.current_video.clone();
        if self.current_video.is_none() && !self.queue.is_empty() {
            self.current_video = Some(self.queue[0].clone());
        }
        self.is_playing = room.is_playing;
        self.remote_users = room.remote_users.iter().cloned().collect();
        self.peers.clear();

        if let Some(ref v) = self.current_video {
            if let Some(dur) = v.duration_sec {
                self.track_duration_sec = dur;
            }
        }

        let rt = RealtimeClient::new(room.id.clone(), u.clone());
        self.realtime = Some(rt);
        self.current_room = Some(room);
        self.update_host_status();
        self.screen = Screen::Room;
        self.chat_messages.clear();
        self.song_requests.clear();
        self.last_seek_time = Instant::now();
        self.last_discord_update = Instant::now();

        if let Some(ref v) = self.current_video {
            let yt = self.youtube.clone();
            let vid_id = v.id.clone();
            let is_play = self.is_playing;
            let tx = self.tx_track_resolved.clone();
            let vid_clone = v.clone();
            self.is_resolving_track = true;
            self.audio.reset_finished();

            tokio::spawn(async move {
                match yt.resolve_audio_url(&vid_id).await {
                    Ok((url, duration)) => {
                        let _ = tx.send(Ok((url, duration, vid_clone, 0.0, is_play)));
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e));
                    }
                }
            });
        }
    }

    fn ui_room(&mut self, ui: &mut egui::Ui) {
        if self.current_room.is_none() {
            return;
        }
        let room_name = self.current_room.as_ref().map(|r| r.name.clone()).unwrap_or_default();

        // 1. COMPACT TOP HEADER BAR
        ui.horizontal(|ui| {
            ui.label(RichText::new(&room_name).strong().size(18.0).color(TEXT_PRIMARY));
            ui.add_space(4.0);

            // Role badge
            if self.is_host && !self.is_acting_host {
                ui.label(
                    RichText::new(" HOST ")
                        .background_color(ACCENT)
                        .color(Color32::WHITE)
                        .strong()
                        .size(10.0),
                );
            } else if self.is_acting_host {
                ui.label(
                    RichText::new(" ACTING HOST ")
                        .background_color(WARNING)
                        .color(Color32::BLACK)
                        .strong()
                        .size(10.0),
                );
            } else if self.has_controls() {
                ui.label(
                    RichText::new(" REMOTE ")
                        .background_color(INFO)
                        .color(Color32::BLACK)
                        .strong()
                        .size(10.0),
                );
            } else {
                ui.label(
                    RichText::new(" LISTENER ")
                        .background_color(BORDER)
                        .color(TEXT_MUTED)
                        .size(10.0),
                );
            }

            // Connection badge
            let is_conn = self.realtime.as_ref().map_or(false, |r| r.is_connected());
            if is_conn {
                ui.label(RichText::new(" Connected ").background_color(SUCCESS).color(Color32::BLACK).size(10.0));
            } else {
                ui.label(RichText::new(" Connecting… ").background_color(WARNING).color(Color32::BLACK).size(10.0));
            }

            ui.label(RichText::new(format!("· {} online", self.peers.len() + 1)).color(TEXT_MUTED).size(12.0));

            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                if ui.add(egui::Button::new(
                    RichText::new("Leave Room").size(12.0).color(TEXT_MUTED)
                ).fill(Color32::TRANSPARENT)).clicked() {
                    self.leave_room();
                }

                if !self.is_host {
                    if ui.add(egui::Button::new(
                        RichText::new("⚡ Resync").strong().size(12.0).color(INFO)
                    ).fill(Color32::TRANSPARENT)).clicked() {
                        self.trigger_resync();
                    }
                }

                // Copy invite link
                if let Some(ref r) = self.current_room.clone() {
                    let link = format!("https://syncytm.netlify.app/room/{}", r.id);
                    if ui.add(egui::Button::new(
                        RichText::new("🔗 Copy Link").size(12.0).color(INFO)
                    ).fill(Color32::TRANSPARENT)).clicked() {
                        ui.output_mut(|o| o.copied_text = link);
                    }
                }
            });
        });

        // Toast / acting host notice
        if self.is_acting_host {
            ui.label(
                RichText::new("🛡  Main host is away — host duties temporarily assigned to YOU.")
                    .color(WARNING)
                    .size(11.0),
            );
        } else if let Some(ref name) = self.acting_host_name.clone() {
            ui.label(
                RichText::new(format!("🛡  Main host is away — duties temporarily assigned to {}.", name))
                    .color(WARNING)
                    .size(11.0),
            );
        }

        if let Some((ref msg, time)) = self.sync_status_toast.clone() {
            if time.elapsed() < Duration::from_secs(4) {
                ui.label(RichText::new(msg).color(INFO).size(11.0));
            }
        }

        ui.add(egui::Separator::default().spacing(6.0));

        // 2. NOW PLAYING PANEL
        egui::Frame::none()
            .fill(BG_PANEL)
            .stroke(Stroke::new(1.0_f32, BORDER_ACTIVE))
            .inner_margin(Margin::same(12.0))
            .rounding(4.0)
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    // Track info (left side)
                    ui.vertical(|ui| {
                        let title   = self.current_video.as_ref().map(|v| v.title.as_str()).unwrap_or("No song playing");
                        let channel = self.current_video.as_ref().map(|v| v.channel.as_str()).unwrap_or("Queue a song to start listening");
                        ui.label(RichText::new(title).strong().size(15.0).color(TEXT_PRIMARY));
                        ui.label(RichText::new(channel).size(12.0).color(TEXT_MUTED));
                    });

                    // Controls (right side)
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        // Volume slider
                        let mut vol = self.audio.get_volume() * 100.0;
                        ui.add(egui::Slider::new(&mut vol, 0.0_f32..=100.0_f32).text("").show_value(false));
                        self.audio.set_volume(vol / 100.0);
                        ui.label(RichText::new("Vol").size(11.0).color(TEXT_MUTED));

                        let can_control = self.has_controls();
                        if can_control {
                            if ui.add(egui::Button::new(RichText::new("⏭").size(16.0)).fill(Color32::TRANSPARENT)).clicked() {
                                self.next_track();
                            }

                            let play_icon = if self.is_playing { "⏸" } else { "▶" };
                            if ui.add(egui::Button::new(RichText::new(play_icon).strong().size(18.0)).fill(Color32::TRANSPARENT)).clicked() {
                                self.is_playing = !self.is_playing;
                                if self.is_playing {
                                    self.audio.resume();
                                    if let Some(ref rt) = self.realtime {
                                        rt.send(SyncData::Play { time: Some(self.audio.get_time_sec()) });
                                    }
                                } else {
                                    self.audio.pause();
                                    if let Some(ref rt) = self.realtime {
                                        rt.send(SyncData::Pause);
                                    }
                                }
                            }

                            if ui.add(egui::Button::new(RichText::new("⏮").size(16.0)).fill(Color32::TRANSPARENT)).clicked() {
                                self.prev_track();
                            }
                        }
                    });
                });

                ui.add_space(6.0);

                // Progress row
                let current_sec = self.audio.get_time_sec();
                let dur_sec     = self.track_duration_sec;
                let mut seek_pos = current_sec;

                ui.horizontal(|ui| {
                    ui.label(RichText::new(format_duration(current_sec)).size(11.0).color(TEXT_MUTED));
                    let slider_resp = ui.add(
                        egui::Slider::new(&mut seek_pos, 0.0_f64..=dur_sec)
                            .show_value(false)
                            .trailing_fill(true),
                    );
                    if slider_resp.drag_stopped() && self.has_controls() {
                        self.last_seek_time = Instant::now();
                        self.audio.seek(seek_pos);
                        if let Some(ref rt) = self.realtime {
                            rt.send(SyncData::Seek { time: seek_pos });
                        }
                    }
                    ui.label(RichText::new(format_duration(dur_sec)).size(11.0).color(TEXT_MUTED));
                });
            });

        ui.add_space(6.0);

        // 3. TAB BAR (styled)
        let req_badge = if !self.song_requests.is_empty() && self.is_host {
            format!("Queue ({}) [{}⚑]", self.queue.len(), self.song_requests.len())
        } else {
            format!("Queue ({})", self.queue.len())
        };

        ui.horizontal(|ui| {
            let tabs: &[(&str, RoomTab)] = &[
                (&req_badge, RoomTab::Queue),
                ("Search & Add", RoomTab::Search),
                (&format!("Chat ({})", self.chat_messages.len()), RoomTab::Chat),
                (&format!("Listeners ({})", self.peers.len() + 1), RoomTab::Listeners),
            ];

            for (label, tab_variant) in tabs.iter() {
                let is_active = std::mem::discriminant(&self.active_tab) == std::mem::discriminant(tab_variant);
                let btn = if is_active {
                    egui::Button::new(
                        RichText::new(*label).size(12.5).strong().color(Color32::WHITE),
                    )
                    .fill(ACCENT)
                    .rounding(4.0)
                } else {
                    egui::Button::new(
                        RichText::new(*label).size(12.5).color(TEXT_MUTED),
                    )
                    .fill(Color32::TRANSPARENT)
                    .rounding(4.0)
                };

                if ui.add(btn).clicked() {
                    match tab_variant {
                        RoomTab::Queue     => self.active_tab = RoomTab::Queue,
                        RoomTab::Search    => self.active_tab = RoomTab::Search,
                        RoomTab::Chat      => self.active_tab = RoomTab::Chat,
                        RoomTab::Listeners => self.active_tab = RoomTab::Listeners,
                    }
                }
            }
        });

        ui.add(egui::Separator::default().spacing(4.0));
        ui.add_space(4.0);

        match self.active_tab {
            RoomTab::Queue     => self.ui_tab_queue(ui),
            RoomTab::Search    => self.ui_tab_search(ui),
            RoomTab::Chat      => self.ui_tab_chat(ui),
            RoomTab::Listeners => self.ui_tab_listeners(ui),
        }
    }

    fn ui_tab_queue(&mut self, ui: &mut egui::Ui) {
        // Pending Song Requests (Host only)
        if self.is_host && !self.song_requests.is_empty() {
            egui::Frame::none()
                .fill(BG_CARD)
                .stroke(Stroke::new(1.0_f32, WARNING))
                .inner_margin(Margin::same(8.0))
                .rounding(4.0)
                .show(ui, |ui| {
                    ui.label(RichText::new("📩 Pending Song Requests").strong().size(13.0).color(WARNING));
                    ui.add_space(4.0);

                    let mut req_to_accept = None;
                    let mut req_to_decline = None;

                    for req in &self.song_requests {
                        ui.horizontal(|ui| {
                            ui.vertical(|ui| {
                                ui.label(RichText::new(&req.video.title).strong().size(12.0).color(TEXT_PRIMARY));
                                ui.label(RichText::new(format!("Requested by {}", req.requester)).size(11.0).color(TEXT_MUTED));
                            });
                            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                if ui.add(egui::Button::new(RichText::new("✕ Decline").size(11.0).color(Color32::from_rgb(255, 100, 100))).fill(Color32::TRANSPARENT)).clicked() {
                                    req_to_decline = Some(req.id);
                                }
                                if ui.add(egui::Button::new(RichText::new("✓ Accept").size(11.0).color(SUCCESS).strong()).fill(Color32::TRANSPARENT)).clicked() {
                                    req_to_accept = Some(req.clone());
                                }
                            });
                        });
                    }

                    if let Some(req) = req_to_accept {
                        self.song_requests.retain(|r| r.id != req.id);
                        self.queue.push(req.video.clone());
                        if let Some(ref rt) = self.realtime {
                            rt.send(SyncData::QueueAdd { video: req.video });
                        }
                    }
                    if let Some(id) = req_to_decline {
                        self.song_requests.retain(|r| r.id != id);
                    }
                });
            ui.add_space(6.0);
        }

        ui.horizontal(|ui| {
            ui.label(RichText::new("Upcoming Tracks").strong().size(14.0).color(TEXT_PRIMARY));
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                if self.has_controls() {
                    if ui.add(egui::Button::new(RichText::new("Clear Queue").size(12.0).color(TEXT_MUTED)).fill(Color32::TRANSPARENT)).clicked() {
                        self.queue.clear();
                        if let Some(ref rt) = self.realtime {
                            rt.send(SyncData::QueueClear);
                        }
                    }
                }
            });
        });
        ui.add_space(4.0);

        if self.queue.is_empty() {
            ui.label(RichText::new("Queue is empty — search for a song to get started!").color(TEXT_MUTED).size(12.0));
            return;
        }

        let mut item_to_play   = None;
        let mut item_to_remove = None;

        egui::ScrollArea::vertical().show(ui, |ui| {
            for (idx, song) in self.queue.iter().enumerate() {
                let is_current = self.current_video.as_ref().map_or(false, |c| {
                    (!c.id.is_empty() && c.id == song.id)
                        || (!c.title.is_empty() && c.title == song.title)
                        || (c.queue_item_id.is_some() && c.queue_item_id == song.queue_item_id)
                });

                let frame_res = egui::Frame::none()
                    .fill(if is_current { BG_ACTIVE } else { BG_CARD })
                    .stroke(Stroke::new(if is_current { 1.5_f32 } else { 1.0_f32 }, if is_current { BORDER_ACTIVE } else { BORDER }))
                    .inner_margin(Margin::same(8.0))
                    .rounding(4.0)
                    .show(ui, |ui| {
                        ui.set_width(ui.available_width());
                        ui.horizontal(|ui| {
                            if is_current {
                                ui.label(RichText::new("▶").color(ACCENT).strong());
                            } else {
                                ui.label(RichText::new(format!("#{}", idx + 1)).color(TEXT_SUBTLE).size(11.0));
                            }
                            ui.vertical(|ui| {
                                ui.label(
                                    RichText::new(&song.title)
                                        .strong()
                                        .size(13.0)
                                        .color(if is_current { Color32::WHITE } else { TEXT_PRIMARY }),
                                );
                                ui.label(RichText::new(&song.channel).size(11.0).color(TEXT_MUTED));
                            });

                            if self.has_controls() {
                                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                    if ui.add(egui::Button::new(RichText::new("✕").size(11.0).color(TEXT_MUTED)).fill(Color32::TRANSPARENT)).clicked() {
                                        item_to_remove = Some(song.id.clone());
                                    }
                                });
                            }
                        });
                    });

                // Clicking the song card plays the song if host/controller and not already playing
                if self.has_controls() && !is_current {
                    let card_id = ui.make_persistent_id(format!("q_card_{}_{}", idx, song.id));
                    let card_resp = ui.interact(frame_res.response.rect, card_id, egui::Sense::click());
                    if card_resp.hovered() {
                        ui.ctx().set_cursor_icon(egui::CursorIcon::PointingHand);
                        ui.painter().rect_stroke(
                            frame_res.response.rect,
                            4.0,
                            Stroke::new(1.0_f32, ACCENT),
                        );
                    }
                    if card_resp.clicked() && item_to_remove.is_none() {
                        item_to_play = Some(song.clone());
                    }
                }

                ui.add_space(3.0);
            }
        });

        if let Some(song) = item_to_play {
            self.play_video_local_and_broadcast(song);
        }
        if let Some(id) = item_to_remove {
            self.queue.retain(|s| s.id != id);
            if let Some(ref rt) = self.realtime {
                rt.send(SyncData::QueueRemove { item_id: id });
            }
        }
    }

    fn ui_tab_search(&mut self, ui: &mut egui::Ui) {
        // Search bar row
        ui.horizontal(|ui| {
            let resp = ui.add(
                egui::TextEdit::singleline(&mut self.search_query)
                    .desired_width(ui.available_width() - 90.0)
                    .hint_text("Search YouTube…"),
            );
            let enter_pressed = resp.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter));

            let search_lbl = if self.is_searching { "Searching…" } else { "Search" };
            let do_search = ui.add(
                egui::Button::new(RichText::new(search_lbl).color(Color32::WHITE).strong().size(13.0))
                    .fill(ACCENT)
                    .min_size(Vec2::new(80.0, 28.0)),
            ).clicked() || enter_pressed;

            if do_search && !self.search_query.trim().is_empty() && !self.is_searching {
                let yt = self.youtube.clone();
                let q = self.search_query.trim().to_string();
                self.is_searching = true;
                let (tx, rx) = crossbeam_channel::bounded(1);
                tokio::spawn(async move {
                    let res = yt.search(&q).await;
                    let _ = tx.send(res);
                });
                self.rx_search = Some(rx);
            }
        });

        ui.add_space(6.0);

        // Import URL collapsible
        ui.collapsing(RichText::new("Import by YouTube URL").size(12.0).color(TEXT_MUTED), |ui| {
            ui.horizontal(|ui| {
                ui.add(egui::TextEdit::singleline(&mut self.direct_import_url)
                    .desired_width(ui.available_width() - 90.0)
                    .hint_text("YouTube video or playlist URL"));
                let import_lbl = if self.is_importing { "Importing…" } else { "Import" };
                if ui.button(import_lbl).clicked() && !self.direct_import_url.trim().is_empty() && !self.is_importing {
                    let yt = self.youtube.clone();
                    let url = self.direct_import_url.trim().to_string();
                    let (tx, rx) = crossbeam_channel::bounded(1);
                    self.is_importing = true;
                    tokio::spawn(async move {
                        let res = yt.fetch_playlist(&url).await;
                        let _ = tx.send(res);
                    });
                    self.rx_import = Some(rx);
                    self.direct_import_url.clear();
                }
            });
        });

        ui.add_space(4.0);

        if self.is_searching {
            ui.label(RichText::new("Searching YouTube…").color(INFO).size(12.0));
        }

        let mut video_to_queue   = None;
        let mut video_to_request = None;
        let has_ctrl = self.has_controls();

        egui::ScrollArea::vertical().show(ui, |ui| {
            for video in &self.search_results {
                egui::Frame::none()
                    .fill(BG_CARD)
                    .stroke(Stroke::new(1.0_f32, BORDER))
                    .inner_margin(Margin::same(8.0))
                    .rounding(4.0)
                    .show(ui, |ui| {
                        ui.horizontal(|ui| {
                            ui.vertical(|ui| {
                                ui.label(RichText::new(&video.title).strong().size(13.0).color(TEXT_PRIMARY));
                                ui.label(RichText::new(&video.channel).size(11.0).color(TEXT_MUTED));
                            });
                            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                if has_ctrl {
                                    if ui.add(egui::Button::new(RichText::new("+ Add to Queue").size(11.0).color(Color32::WHITE).strong()).fill(ACCENT)).clicked() {
                                        video_to_queue = Some(video.clone());
                                    }
                                } else {
                                    if ui.add(egui::Button::new(RichText::new("📨 Request").size(11.0).color(Color32::BLACK).strong()).fill(WARNING)).clicked() {
                                        video_to_request = Some(video.clone());
                                    }
                                }
                            });
                        });
                    });
                ui.add_space(3.0);
            }
        });

        if let Some(v) = video_to_queue {
            self.queue.push(v.clone());
            if let Some(ref rt) = self.realtime {
                rt.send(SyncData::QueueAdd { video: v });
            }
        }
        if let Some(v) = video_to_request {
            if let Some(ref rt) = self.realtime {
                rt.send(SyncData::RequestSong { video: v.clone() });
                self.sync_status_toast = Some((format!("Requested: {}", v.title), Instant::now()));
            }
        }
    }

    fn ui_tab_chat(&mut self, ui: &mut egui::Ui) {
        egui::ScrollArea::vertical()
            .max_height(ui.available_height() - 46.0)
            .stick_to_bottom(true)
            .show(ui, |ui| {
                for msg in &self.chat_messages {
                    ui.horizontal_wrapped(|ui| {
                        ui.label(RichText::new(&msg.timestamp).size(10.0).color(TEXT_SUBTLE));
                        ui.label(RichText::new(format!("{}:", msg.user)).strong().size(12.0).color(ACCENT));
                        ui.label(RichText::new(&msg.text).size(12.0).color(TEXT_PRIMARY));
                    });
                }
            });

        ui.add(egui::Separator::default().spacing(4.0));

        ui.horizontal(|ui| {
            let resp = ui.add(
                egui::TextEdit::singleline(&mut self.new_chat_text)
                    .desired_width(ui.available_width() - 70.0)
                    .hint_text("Type a message…"),
            );
            let send_btn = ui.add(
                egui::Button::new(RichText::new("Send").color(Color32::WHITE).strong().size(12.0))
                    .fill(ACCENT)
                    .min_size(Vec2::new(60.0, 28.0)),
            );

            if (send_btn.clicked() || (resp.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter))))
                && !self.new_chat_text.trim().is_empty()
            {
                let text = self.new_chat_text.trim().to_string();
                let username = self.user_session.as_ref().map(|u| u.username.clone()).unwrap_or_else(|| "Me".to_string());

                self.chat_messages.push(ChatMessage {
                    id: SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64,
                    user: username,
                    text: text.clone(),
                    timestamp: chrono_now_time(),
                });

                if let Some(ref rt) = self.realtime {
                    rt.send(SyncData::Chat { payload: text });
                }

                self.new_chat_text.clear();
            }
        });
    }

    fn ui_tab_listeners(&mut self, ui: &mut egui::Ui) {
        ui.label(RichText::new("CONNECTED LISTENERS").size(11.0).color(TEXT_SUBTLE).strong());
        ui.add_space(6.0);

        // "You" row
        if let Some(ref u) = self.user_session.clone() {
            let is_main_host = self.current_room.as_ref().map(|r| r.host_id == u.id).unwrap_or(false);
            egui::Frame::none()
                .fill(BG_CARD)
                .stroke(Stroke::new(1.0_f32, BORDER))
                .inner_margin(Margin::same(10.0))
                .rounding(4.0)
                .show(ui, |ui| {
                    ui.set_width(ui.available_width());
                    ui.horizontal(|ui| {
                        ui.label(RichText::new(&u.username).strong().size(13.0).color(TEXT_PRIMARY));
                        ui.label(RichText::new(&u.email).size(11.0).color(TEXT_MUTED));

                        if is_main_host {
                            ui.label(RichText::new(" HOST ").background_color(ACCENT).color(Color32::WHITE).size(10.0));
                        } else if self.is_acting_host {
                            ui.label(RichText::new(" ACTING HOST ").background_color(WARNING).color(Color32::BLACK).size(10.0));
                        } else if self.remote_users.contains(&u.email) {
                            ui.label(RichText::new(" REMOTE ").background_color(INFO).color(Color32::BLACK).size(10.0));
                        }

                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            ui.label(RichText::new("(You)").size(11.0).color(TEXT_SUBTLE));
                        });
                    });
                });
            ui.add_space(3.0);
        }

        let mut toggle_remote = None;

        let peers_snapshot: Vec<_> = self.peers.iter().cloned().collect();
        for peer in &peers_snapshot {
            let is_main_host = self.current_room.as_ref().map(|r| r.host_id == peer.peer_id).unwrap_or(false);
            let is_acting    = self.active_host_id.as_ref().map(|id| id == &peer.peer_id && !is_main_host).unwrap_or(false);
            let has_remote   = self.remote_users.contains(&peer.user_email);

            egui::Frame::none()
                .fill(BG_CARD)
                .stroke(Stroke::new(1.0_f32, BORDER))
                .inner_margin(Margin::same(10.0))
                .rounding(4.0)
                .show(ui, |ui| {
                    ui.set_width(ui.available_width());
                    ui.horizontal(|ui| {
                        ui.label(RichText::new(&peer.username).strong().size(13.0).color(TEXT_PRIMARY));
                        ui.label(RichText::new(&peer.user_email).size(11.0).color(TEXT_MUTED));

                        if is_main_host {
                            ui.label(RichText::new(" HOST ").background_color(ACCENT).color(Color32::WHITE).size(10.0));
                        } else if is_acting {
                            ui.label(RichText::new(" ACTING HOST ").background_color(WARNING).color(Color32::BLACK).size(10.0));
                        } else if has_remote {
                            ui.label(RichText::new(" REMOTE ").background_color(INFO).color(Color32::BLACK).size(10.0));
                        }

                        if self.is_host {
                            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                let label = if has_remote { "Revoke Remote" } else { "Grant Remote" };
                                if ui.button(RichText::new(label).size(11.0).color(TEXT_MUTED)).clicked() {
                                    toggle_remote = Some((peer.user_email.clone(), !has_remote));
                                }
                            });
                        }
                    });
                });
            ui.add_space(3.0);
        }

        if let Some((target_email, val)) = toggle_remote {
            if val {
                self.remote_users.insert(target_email.clone());
            } else {
                self.remote_users.remove(&target_email);
            }
            if let Some(ref rt) = self.realtime {
                rt.send(SyncData::GrantRemote { target_email, value: val });
            }
        }
    }
}
