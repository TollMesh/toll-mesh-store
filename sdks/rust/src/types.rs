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

// ===== Job Queues =====

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    pub queue: String,
    pub payload: String,
    pub status: String,
    pub priority: i32,
    pub retry_count: i32,
    pub max_retries: i32,
    pub processed_by: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<String>,
    pub error: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub deadline_at: i64,
}

#[derive(Debug, Serialize)]
pub struct EnqueueRequest {
    pub queue: String,
    pub payload: String,
    pub priority: i32,
    pub max_retries: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deadline: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct ClaimRequest {
    pub queue: String,
    pub worker_id: String,
}

#[derive(Debug, Serialize)]
pub struct CompleteRequest {
    pub queue: String,
    pub job_id: String,
    pub result: String,
}

#[derive(Debug, Serialize)]
pub struct FailRequest {
    pub queue: String,
    pub job_id: String,
    pub error: String,
}

// ===== Sorted Sets =====

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortedSetMember {
    pub member: String,
    pub score: f64,
    pub timestamp: i64,
    pub node: String,
}

#[derive(Debug, Serialize)]
pub struct ZAddRequest {
    pub key: String,
    pub member: String,
    pub score: f64,
}

#[derive(Debug, Serialize)]
pub struct ZMemberRequest {
    pub key: String,
    pub member: String,
}

#[derive(Debug, Deserialize)]
pub struct ZScoreResponse {
    pub score: Option<f64>,
    pub exists: bool,
}

#[derive(Debug, Deserialize)]
pub struct ZRankResponse {
    pub rank: Option<i64>,
    pub exists: bool,
}

#[derive(Debug, Deserialize)]
pub struct ZRangeResponse {
    #[serde(default)]
    pub members: Vec<SortedSetMember>,
}

#[derive(Debug, Deserialize)]
pub struct ZCardResponse {
    pub card: i64,
}

// ===== Streams =====

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamEntry {
    pub id: String,
    pub timestamp: i64,
    pub fields: HashMap<String, String>,
    pub node: String,
    pub sequence: i64,
}

#[derive(Debug, Serialize)]
pub struct XAddRequest {
    pub stream: String,
    pub fields: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
pub struct XRangeResponse {
    #[serde(default)]
    pub entries: Vec<StreamEntry>,
}

#[derive(Debug, Deserialize)]
pub struct XLenResponse {
    pub length: i64,
}

#[derive(Debug, Serialize)]
pub struct XGroupCreateRequest {
    pub stream: String,
    pub group: String,
}

#[derive(Debug, Serialize)]
pub struct XReadGroupRequest {
    pub stream: String,
    pub group: String,
    pub consumer: String,
    pub limit: i64,
}

#[derive(Debug, Serialize)]
pub struct XAckRequest {
    pub stream: String,
    pub group: String,
    pub consumer: String,
    pub id: String,
}
