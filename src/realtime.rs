use crate::config::{SUPABASE_ANON_KEY, SUPABASE_URL};
use crate::models::{PeerInfo, SyncData, UserSession};
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::protocol::Message;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum RealtimeEvent {
    Connected,
    Disconnected,
    Broadcast {
        sender_id: String,
        sender_email: String,
        sender_name: String,
        data: SyncData,
    },
    PeersUpdated(Vec<PeerInfo>),
}

pub struct RealtimeClient {
    tx_outgoing: crossbeam_channel::Sender<SyncData>,
    rx_incoming: crossbeam_channel::Receiver<RealtimeEvent>,
    is_connected: Arc<std::sync::atomic::AtomicBool>,
    is_running: Arc<std::sync::atomic::AtomicBool>,
    abort_handle: tokio::task::AbortHandle,
}

impl RealtimeClient {
    pub fn new(room_id: String, user: UserSession) -> Self {
        let (tx_out, rx_out) = crossbeam_channel::unbounded::<SyncData>();
        let (tx_in, rx_in) = crossbeam_channel::unbounded::<RealtimeEvent>();
        let is_connected = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let is_running = Arc::new(std::sync::atomic::AtomicBool::new(true));

        let is_conn_clone = is_connected.clone();
        let is_run_clone = is_running.clone();

        let handle = tokio::spawn(async move {
            Self::run_connection_loop(room_id, user, rx_out, tx_in, is_conn_clone, is_run_clone).await;
        });

        Self {
            tx_outgoing: tx_out,
            rx_incoming: rx_in,
            is_connected,
            is_running,
            abort_handle: handle.abort_handle(),
        }
    }

    pub fn close(&self) {
        self.is_running.store(false, Ordering::Relaxed);
        self.is_connected.store(false, Ordering::Relaxed);
        self.abort_handle.abort();
    }

    pub fn send(&self, data: SyncData) {
        let _ = self.tx_outgoing.send(data);
    }

    pub fn try_recv(&self) -> Option<RealtimeEvent> {
        self.rx_incoming.try_recv().ok()
    }

    pub fn is_connected(&self) -> bool {
        self.is_connected.load(Ordering::Relaxed)
    }

    async fn run_connection_loop(
        room_id: String,
        user: UserSession,
        rx_out: crossbeam_channel::Receiver<SyncData>,
        tx_in: crossbeam_channel::Sender<RealtimeEvent>,
        is_connected: Arc<std::sync::atomic::AtomicBool>,
        is_running: Arc<std::sync::atomic::AtomicBool>,
    ) {
        let ws_url = format!(
            "{}/realtime/v1/websocket?apikey={}&vsn=1.0.0",
            SUPABASE_URL.replace("https://", "wss://"),
            SUPABASE_ANON_KEY
        );
        let topic = format!("realtime:room:{}", room_id);
        let ref_counter = Arc::new(AtomicU64::new(1));

        while is_running.load(Ordering::Relaxed) {
            let res = connect_async(&ws_url).await;
            let (mut ws_stream, _) = match res {
                Ok(stream) => stream,
                Err(e) => {
                    eprintln!("Realtime connection error: {}, retrying in 3s...", e);
                    let _ = tx_in.send(RealtimeEvent::Disconnected);
                    is_connected.store(false, Ordering::Relaxed);
                    if !is_running.load(Ordering::Relaxed) {
                        break;
                    }
                    sleep(Duration::from_secs(3)).await;
                    continue;
                }
            };

            // 1. Join room channel with presence enabled
            let join_msg = json!({
                "topic": &topic,
                "event": "phx_join",
                "payload": {
                    "config": {
                        "broadcast": { "self": false, "ack": false },
                        "presence": { "key": &user.id, "enabled": true },
                        "postgres_changes": [],
                        "private": false
                    }
                },
                "ref": "1",
                "join_ref": "1"
            });

            if let Err(e) = ws_stream.send(Message::Text(join_msg.to_string())).await {
                eprintln!("Failed to join channel: {}", e);
                if !is_running.load(Ordering::Relaxed) {
                    break;
                }
                sleep(Duration::from_secs(2)).await;
                continue;
            }

            // 2. Track presence immediately (Supabase Realtime protocol)
            let track_ref = ref_counter.fetch_add(1, Ordering::Relaxed);
            let track_msg = json!({
                "topic": &topic,
                "event": "presence",
                "payload": {
                    "type": "presence",
                    "event": "track",
                    "payload": {
                        "user_email": &user.email,
                        "username": &user.username
                    }
                },
                "ref": track_ref.to_string(),
                "join_ref": "1"
            });
            let _ = ws_stream.send(Message::Text(track_msg.to_string())).await;

            is_connected.store(true, Ordering::Relaxed);
            let _ = tx_in.send(RealtimeEvent::Connected);

            // Maintain peers map
            let mut peers: HashMap<String, PeerInfo> = HashMap::new();

            let mut heartbeat_timer = tokio::time::interval(Duration::from_secs(20));
            let mut poll_outgoing_timer = tokio::time::interval(Duration::from_millis(50));

            loop {
                tokio::select! {
                    _ = heartbeat_timer.tick() => {
                        if !is_running.load(Ordering::Relaxed) {
                            break;
                        }
                        let cur_ref = ref_counter.fetch_add(1, Ordering::Relaxed);
                        let hb = json!({
                            "topic": "phoenix",
                            "event": "heartbeat",
                            "payload": {},
                            "ref": cur_ref.to_string()
                        });
                        if ws_stream.send(Message::Text(hb.to_string())).await.is_err() {
                            break;
                        }

                        // Periodically refresh presence so Supabase server keeps our presence active
                        let p_ref = ref_counter.fetch_add(1, Ordering::Relaxed);
                        let p_msg = json!({
                            "topic": &topic,
                            "event": "presence",
                            "payload": {
                                "type": "presence",
                                "event": "track",
                                "payload": {
                                    "user_email": &user.email,
                                    "username": &user.username
                                }
                            },
                            "ref": p_ref.to_string(),
                            "join_ref": "1"
                        });
                        let _ = ws_stream.send(Message::Text(p_msg.to_string())).await;
                    }

                    _ = poll_outgoing_timer.tick() => {
                        if !is_running.load(Ordering::Relaxed) {
                            let leave_ref = ref_counter.fetch_add(1, Ordering::Relaxed);
                            let leave_msg = json!({
                                "topic": &topic,
                                "event": "phx_leave",
                                "payload": {},
                                "ref": leave_ref.to_string(),
                                "join_ref": "1"
                            });
                            let _ = ws_stream.send(Message::Text(leave_msg.to_string())).await;
                            let _ = ws_stream.send(Message::Close(None)).await;
                            break;
                        }

                        let mut should_exit = false;
                        loop {
                            match rx_out.try_recv() {
                                Ok(data) => {
                                    let cur_ref = ref_counter.fetch_add(1, Ordering::Relaxed);
                                    let msg = json!({
                                        "topic": &topic,
                                        "event": "broadcast",
                                        "payload": {
                                            "type": "broadcast",
                                            "event": "message",
                                            "payload": {
                                                "sender": &user.id,
                                                "senderEmail": &user.email,
                                                "senderName": &user.username,
                                                "data": data
                                            }
                                        },
                                        "ref": cur_ref.to_string()
                                    });
                                    if ws_stream.send(Message::Text(msg.to_string())).await.is_err() {
                                        should_exit = true;
                                        break;
                                    }
                                }
                                Err(crossbeam_channel::TryRecvError::Empty) => break,
                                Err(crossbeam_channel::TryRecvError::Disconnected) => {
                                    should_exit = true;
                                    is_running.store(false, Ordering::Relaxed);
                                    let leave_ref = ref_counter.fetch_add(1, Ordering::Relaxed);
                                    let leave_msg = json!({
                                        "topic": &topic,
                                        "event": "phx_leave",
                                        "payload": {},
                                        "ref": leave_ref.to_string(),
                                        "join_ref": "1"
                                    });
                                    let _ = ws_stream.send(Message::Text(leave_msg.to_string())).await;
                                    let _ = ws_stream.send(Message::Close(None)).await;
                                    break;
                                }
                            }
                        }
                        if should_exit || !is_running.load(Ordering::Relaxed) {
                            break;
                        }
                    }

                    msg = ws_stream.next() => {
                        let Some(msg) = msg else { break; };
                        let msg = match msg {
                            Ok(Message::Text(t)) => t,
                            Ok(Message::Ping(p)) => {
                                let _ = ws_stream.send(Message::Pong(p)).await;
                                continue;
                            }
                            Ok(Message::Close(_)) | Err(_) => {
                                break;
                            }
                            _ => continue,
                        };

                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&msg) {
                            let event = v["event"].as_str().unwrap_or("");
                            let payload = &v["payload"];

                            if event == "broadcast" {
                                let inner = &payload["payload"];
                                let sender = inner["sender"].as_str().unwrap_or("");
                                if sender == user.id {
                                    continue;
                                }

                                if let Some(target) = inner["target"].as_str() {
                                    if target != user.id {
                                        continue;
                                    }
                                }

                                let sender_email = inner["senderEmail"].as_str().unwrap_or("Unknown").to_string();
                                let sender_name = inner["senderName"].as_str().unwrap_or(&sender_email).to_string();

                                if let Ok(data) = serde_json::from_value::<SyncData>(inner["data"].clone()) {
                                    let _ = tx_in.send(RealtimeEvent::Broadcast {
                                        sender_id: sender.to_string(),
                                        sender_email,
                                        sender_name,
                                        data,
                                    });
                                }
                            } else if event == "phx_reply" && v["ref"] == "1" {
                                // Channel joined successfully! Re-track presence right away
                                let t_ref = ref_counter.fetch_add(1, Ordering::Relaxed);
                                let t_msg = json!({
                                    "topic": &topic,
                                    "event": "presence",
                                    "payload": {
                                        "type": "presence",
                                        "event": "track",
                                        "payload": {
                                            "user_email": &user.email,
                                            "username": &user.username
                                        }
                                    },
                                    "ref": t_ref.to_string(),
                                    "join_ref": "1"
                                });
                                let _ = ws_stream.send(Message::Text(t_msg.to_string())).await;
                            } else if event == "presence_state" {
                                peers.clear();
                                if let Some(obj) = payload.as_object() {
                                    for (peer_id, presences) in obj {
                                        if peer_id == &user.id {
                                            continue;
                                        }
                                        let metas = if let Some(arr) = presences["metas"].as_array() {
                                            arr
                                        } else if let Some(arr) = presences.as_array() {
                                            arr
                                        } else {
                                            continue;
                                        };
                                        for p in metas {
                                            let email = p["user_email"]
                                                .as_str()
                                                .or_else(|| p["userEmail"].as_str())
                                                .unwrap_or("User")
                                                .to_string();
                                            let username = p["username"]
                                                .as_str()
                                                .or_else(|| p["userName"].as_str())
                                                .unwrap_or(&email)
                                                .to_string();
                                            peers.insert(peer_id.clone(), PeerInfo {
                                                peer_id: peer_id.clone(),
                                                user_email: email,
                                                username,
                                            });
                                        }
                                    }
                                }
                                eprintln!("[Realtime] Presence state parsed: {} connected peers", peers.len());
                                let _ = tx_in.send(RealtimeEvent::PeersUpdated(peers.values().cloned().collect()));
                            } else if event == "presence_diff" {
                                if let Some(joins) = payload["joins"].as_object() {
                                    for (peer_id, presences) in joins {
                                        if peer_id == &user.id {
                                            continue;
                                        }
                                        let metas = if let Some(arr) = presences["metas"].as_array() {
                                            arr
                                        } else if let Some(arr) = presences.as_array() {
                                            arr
                                        } else {
                                            continue;
                                        };
                                        for p in metas {
                                            let email = p["user_email"]
                                                .as_str()
                                                .or_else(|| p["userEmail"].as_str())
                                                .unwrap_or("User")
                                                .to_string();
                                            let username = p["username"]
                                                .as_str()
                                                .or_else(|| p["userName"].as_str())
                                                .unwrap_or(&email)
                                                .to_string();
                                            peers.insert(peer_id.clone(), PeerInfo {
                                                peer_id: peer_id.clone(),
                                                user_email: email,
                                                username,
                                            });
                                        }
                                    }
                                }
                                if let Some(leaves) = payload["leaves"].as_object() {
                                    for peer_id in leaves.keys() {
                                        peers.remove(peer_id);
                                    }
                                }
                                eprintln!("[Realtime] Presence diff parsed: {} connected peers", peers.len());
                                let _ = tx_in.send(RealtimeEvent::PeersUpdated(peers.values().cloned().collect()));
                            }
                        }
                    }
                }
            }

            is_connected.store(false, Ordering::Relaxed);
            let _ = tx_in.send(RealtimeEvent::Disconnected);
            if !is_running.load(Ordering::Relaxed) {
                break;
            }
            sleep(Duration::from_secs(2)).await;
        }
    }
}

impl Drop for RealtimeClient {
    fn drop(&mut self) {
        self.close();
    }
}
