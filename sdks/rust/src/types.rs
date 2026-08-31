use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumeResult {
    pub ok: bool,
    pub remaining: i32,
    pub reset_at: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeenResult {
    pub seen: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheValue {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    pub exists: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub node: String,
    pub peers: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stats: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Peer {
    pub id: String,
    pub address: String,
    pub port: u16,
    pub latency_ms: i32,
}

#[derive(Debug, Serialize)]
pub struct ConsumeRequest {
    pub key: String,
    pub limit: i32,
    pub window: u64,
}

#[derive(Debug, Serialize)]
pub struct SeenRequest {
    pub key: String,
    pub ttl: u64,
}

#[derive(Debug, Serialize)]
pub struct CacheGetRequest {
    pub namespace: String,
    pub key: String,
}

#[derive(Debug, Serialize)]
pub struct CacheSetRequest {
    pub namespace: String,
    pub key: String,
    pub value: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl: Option<u64>,
}
