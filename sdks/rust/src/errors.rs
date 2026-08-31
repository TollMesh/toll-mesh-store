use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ErrorCode {
    Ok = 0,
    InvalidRequest = 400,
    NotFound = 404,
    Conflict = 409,
    RateLimited = 429,
    Internal = 500,
    Unavailable = 503,
    DeadlineExceeded = 504,
    ReplayDetected = 1001,
    CacheMiss = 1002,
    InvalidNamespace = 1003,
    InvalidKey = 1004,
    InvalidTtl = 1005,
    InvalidValue = 1006,
    PeerUnavailable = 1007,
    GossipFailed = 1008,
    TransactionFailed = 1009,
    ScriptError = 1010,
    SearchFailed = 1011,
    GraphError = 1012,
}

#[derive(Debug, Clone)]
pub struct TollMeshError {
    pub code: ErrorCode,
    pub message: String,
}

impl TollMeshError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn is_retryable(&self) -> bool {
        matches!(
            self.code,
            ErrorCode::Unavailable | ErrorCode::DeadlineExceeded | ErrorCode::GossipFailed
        )
    }

    pub fn is_rate_limited(&self) -> bool {
        self.code == ErrorCode::RateLimited
    }

    pub fn is_replay(&self) -> bool {
        self.code == ErrorCode::ReplayDetected
    }
}

impl fmt::Display for TollMeshError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Error {}: {}", self.code as i32, self.message)
    }
}

impl std::error::Error for TollMeshError {}
