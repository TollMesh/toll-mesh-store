/// TollMeshCache Rust SDK
/// Distributed CRDT-based caching and coordination

pub mod client;
pub mod config;
pub mod errors;
pub mod types;
pub mod retry;

pub use client::Client;
pub use config::ClientConfig;
pub use errors::{TollMeshError, ErrorCode};
pub use types::{ConsumeResult, SeenResult, CacheValue, HealthResponse, Peer};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_codes() {
        assert_eq!(ErrorCode::Ok as i32, 0);
        assert_eq!(ErrorCode::RateLimited as i32, 429);
        assert_eq!(ErrorCode::ReplayDetected as i32, 1001);
    }
}
