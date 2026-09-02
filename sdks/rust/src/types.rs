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

// ===== Pub/Sub =====

#[derive(Debug, Serialize)]
pub struct SubscribeRequest {
    pub subscriber_id: String,
    pub topic: String,
    pub pattern: String,
}

#[derive(Debug, Serialize)]
pub struct UnsubscribeRequest {
    pub subscriber_id: String,
    pub topic: String,
}

#[derive(Debug, Serialize)]
pub struct PublishRequest {
    pub topic: String,
    pub publisher: String,
    pub payload: String,
}

#[derive(Debug, Deserialize)]
pub struct PublishResponse {
    pub delivered_count: i64,
}

#[derive(Debug, Serialize)]
pub struct PollRequest {
    pub subscriber_id: String,
    pub limit: i64,
    pub timeout_ms: i64,
}

#[derive(Debug, Deserialize)]
pub struct PollResponse {
    #[serde(default)]
    pub messages: Vec<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub struct TopicsResponse {
    #[serde(default)]
    pub topics: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct SubscribersResponse {
    #[serde(default)]
    pub subscribers: Vec<String>,
}

// ===== Transactions =====

#[derive(Debug, Serialize)]
pub struct TxnIdRequest {
    pub txn_id: String,
}

#[derive(Debug, Serialize)]
pub struct TxnOperationRequest {
    pub txn_id: String,
    #[serde(rename = "type")]
    pub op_type: String,
    pub namespace: String,
    pub key: String,
    pub value: String,
}

#[derive(Debug, Deserialize)]
pub struct TxnStatusResponse {
    pub status: String,
}

// ===== Scripting: Pipelines =====

#[derive(Debug, Serialize)]
pub struct RegisterPipelineRequest {
    pub name: String,
    pub steps: Vec<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct NameRequest {
    pub name: String,
}

#[derive(Debug, Serialize)]
pub struct ExecuteInlinePipelineRequest {
    pub steps: Vec<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub struct PipelinesResponse {
    #[serde(default)]
    pub pipelines: Vec<serde_json::Value>,
}

// ===== Scripting: WASM =====

#[derive(Debug, Serialize)]
pub struct CompileScriptRequest {
    pub name: String,
    pub source: String,
}

#[derive(Debug, Serialize)]
pub struct ExecuteScriptRequest {
    pub name: String,
    pub input: String,
}

#[derive(Debug, Serialize)]
pub struct ExecuteInlineScriptRequest {
    pub source: String,
    pub input: String,
}

#[derive(Debug, Deserialize)]
pub struct ScriptOutputResponse {
    #[serde(default)]
    pub output: String,
}

#[derive(Debug, Deserialize)]
pub struct ScriptsResponse {
    #[serde(default)]
    pub scripts: Vec<serde_json::Value>,
}

// ===== Search =====

#[derive(Debug, Serialize)]
pub struct IndexDocumentRequest {
    pub id: String,
    pub content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector: Option<Vec<f32>>,
}

#[derive(Debug, Serialize)]
pub struct SearchVectorRequest {
    pub vector: Vec<f32>,
    pub topk: i64,
}

#[derive(Debug, Serialize)]
pub struct SearchHybridRequest {
    pub query: String,
    pub vector: Vec<f32>,
    pub topk: i64,
}

#[derive(Debug, Serialize)]
pub struct DeleteDocumentRequest {
    pub id: String,
}

#[derive(Debug, Deserialize)]
pub struct SearchResultsResponse {
    #[serde(default)]
    pub results: Vec<serde_json::Value>,
}

// ===== Ranking =====

#[derive(Debug, Serialize)]
pub struct RankRequest {
    pub items: Vec<serde_json::Value>,
    pub strategy: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boosts: Option<std::collections::HashMap<String, f32>>,
}

#[derive(Debug, Deserialize)]
pub struct RankResponse {
    #[serde(default)]
    pub items: Vec<serde_json::Value>,
}
