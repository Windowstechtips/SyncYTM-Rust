use crate::config::{SUPABASE_ANON_KEY, SUPABASE_URL};
use crate::models::{Room, UserSession, VideoItem};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde_json::json;

pub struct SupabaseService {
    client: reqwest::Client,
}

impl SupabaseService {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }

    fn auth_headers(token: Option<&str>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("apikey", HeaderValue::from_static(SUPABASE_ANON_KEY));
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        if let Some(t) = token {
            if let Ok(val) = HeaderValue::from_str(&format!("Bearer {}", t)) {
                headers.insert(AUTHORIZATION, val);
            }
        }
        headers
    }

    pub async fn login(&self, email: &str, password: &str) -> Result<UserSession, String> {
        let url = format!("{}/auth/v1/token?grant_type=password", SUPABASE_URL);
        let body = json!({
            "email": email,
            "password": password
        });

        let res = self
            .client
            .post(&url)
            .headers(Self::auth_headers(None))
            .json(&body)
            .send()
            .await
            .map_err(|e| e.to_string())?;

        let status = res.status();
        let json: serde_json::Value = res.json().await.map_err(|e| e.to_string())?;

        if !status.is_success() {
            let msg = json["error_description"]
                .as_str()
                .or_else(|| json["msg"].as_str())
                .unwrap_or("Login failed");
            return Err(msg.to_string());
        }

        let access_token = json["access_token"].as_str().unwrap_or("").to_string();
        let refresh_token = json["refresh_token"].as_str().map(|s| s.to_string());
        let user = &json["user"];
        let id = user["id"].as_str().unwrap_or("").to_string();
        let email_str = user["email"].as_str().unwrap_or(email).to_string();
        let username = user["user_metadata"]["username"]
            .as_str()
            .unwrap_or(&email_str)
            .to_string();

        Ok(UserSession {
            id,
            email: email_str,
            username,
            access_token,
            refresh_token,
        })
    }

    pub async fn refresh_session(&self, refresh_token: &str) -> Result<UserSession, String> {
        let url = format!("{}/auth/v1/token?grant_type=refresh_token", SUPABASE_URL);
        let body = json!({
            "refresh_token": refresh_token
        });

        let res = self
            .client
            .post(&url)
            .headers(Self::auth_headers(None))
            .json(&body)
            .send()
            .await
            .map_err(|e| e.to_string())?;

        let status = res.status();
        let json: serde_json::Value = res.json().await.map_err(|e| e.to_string())?;

        if !status.is_success() {
            let msg = json["error_description"]
                .as_str()
                .or_else(|| json["msg"].as_str())
                .unwrap_or("Session refresh failed");
            return Err(msg.to_string());
        }

        let access_token = json["access_token"].as_str().unwrap_or("").to_string();
        let new_refresh_token = json["refresh_token"]
            .as_str()
            .map(|s| s.to_string())
            .or_else(|| Some(refresh_token.to_string()));
        let user = &json["user"];
        let id = user["id"].as_str().unwrap_or("").to_string();
        let email_str = user["email"].as_str().unwrap_or("").to_string();
        let username = user["user_metadata"]["username"]
            .as_str()
            .unwrap_or(&email_str)
            .to_string();

        Ok(UserSession {
            id,
            email: email_str,
            username,
            access_token,
            refresh_token: new_refresh_token,
        })
    }

    pub async fn signup(
        &self,
        email: &str,
        password: &str,
        username: &str,
    ) -> Result<UserSession, String> {
        let url = format!("{}/auth/v1/signup", SUPABASE_URL);
        let body = json!({
            "email": email,
            "password": password,
            "data": {
                "username": username
            }
        });

        let res = self
            .client
            .post(&url)
            .headers(Self::auth_headers(None))
            .json(&body)
            .send()
            .await
            .map_err(|e| e.to_string())?;

        let status = res.status();
        let json: serde_json::Value = res.json().await.map_err(|e| e.to_string())?;

        if !status.is_success() {
            let msg = json["msg"]
                .as_str()
                .or_else(|| json["error_description"].as_str())
                .unwrap_or("Sign up failed");
            return Err(msg.to_string());
        }

        // If email confirmation is disabled, user is returned with session
        if let Some(token) = json["access_token"].as_str() {
            let refresh = json["refresh_token"].as_str().map(|s| s.to_string());
            let user = &json["user"];
            return Ok(UserSession {
                id: user["id"].as_str().unwrap_or("").to_string(),
                email: email.to_string(),
                username: username.to_string(),
                access_token: token.to_string(),
                refresh_token: refresh,
            });
        }

        // Otherwise auto-login
        self.login(email, password).await
    }

    pub async fn list_rooms(&self, token: &str) -> Result<Vec<Room>, String> {
        let url = format!("{}/rest/v1/rooms?select=*&order=created_at.desc", SUPABASE_URL);

        let res = self
            .client
            .get(&url)
            .headers(Self::auth_headers(Some(token)))
            .send()
            .await
            .map_err(|e| e.to_string())?;

        if !res.status().is_success() {
            return Err(format!("Failed to list rooms: status {}", res.status()));
        }

        let rooms: Vec<Room> = res.json().await.map_err(|e| e.to_string())?;
        Ok(rooms)
    }

    pub async fn get_room(&self, token: &str, room_id: &str) -> Result<Room, String> {
        let url = format!("{}/rest/v1/rooms?id=eq.{}&select=*&limit=1", SUPABASE_URL, room_id);

        let res = self
            .client
            .get(&url)
            .headers(Self::auth_headers(Some(token)))
            .send()
            .await
            .map_err(|e| e.to_string())?;

        if !res.status().is_success() {
            return Err(format!("Room not found: status {}", res.status()));
        }

        let mut rooms: Vec<Room> = res.json().await.map_err(|e| e.to_string())?;
        rooms.pop().ok_or_else(|| "Room does not exist".to_string())
    }

    pub async fn create_room(
        &self,
        token: &str,
        host_id: &str,
        name: &str,
        is_private: bool,
        password: Option<&str>,
    ) -> Result<Room, String> {
        let url = format!("{}/rest/v1/rooms", SUPABASE_URL);
        let mut headers = Self::auth_headers(Some(token));
        headers.insert("Prefer", HeaderValue::from_static("return=representation"));

        let body = json!({
            "host_id": host_id,
            "name": name,
            "is_private": is_private,
            "password_hash": password,
            "queue": [],
            "is_playing": false,
            "progress": 0.0,
            "active_listeners": 1,
            "remote_users": []
        });

        let res = self
            .client
            .post(&url)
            .headers(headers)
            .json(&body)
            .send()
            .await
            .map_err(|e| e.to_string())?;

        if !res.status().is_success() {
            return Err(format!("Failed to create room: status {}", res.status()));
        }

        let mut rooms: Vec<Room> = res.json().await.map_err(|e| e.to_string())?;
        rooms.pop().ok_or_else(|| "Failed to parse created room".to_string())
    }

    pub async fn update_room_state(
        &self,
        token: &str,
        room_id: &str,
        current_video: Option<&VideoItem>,
        queue: Option<&Vec<VideoItem>>,
        is_playing: Option<bool>,
        progress: Option<f64>,
    ) -> Result<(), String> {
        let url = format!("{}/rest/v1/rooms?id=eq.{}", SUPABASE_URL, room_id);

        let mut map = serde_json::Map::new();
        if let Some(cv) = current_video {
            map.insert("current_video".to_string(), json!(cv));
        }
        if let Some(q) = queue {
            map.insert("queue".to_string(), json!(q));
        }
        if let Some(p) = is_playing {
            map.insert("is_playing".to_string(), json!(p));
        }
        if let Some(prog) = progress {
            map.insert("progress".to_string(), json!(prog));
        }

        if map.is_empty() {
            return Ok(());
        }

        let _ = self
            .client
            .patch(&url)
            .headers(Self::auth_headers(Some(token)))
            .json(&map)
            .send()
            .await;

        Ok(())
    }
}
