use crate::{ClientConfig, TollMeshError, ErrorCode};
use crate::types::*;
use std::time::Duration;

/// Async TollMeshCache client
pub struct Client {
    config: ClientConfig,
    http_client: reqwest::Client,
}

impl Client {
    /// Create new client with configuration
    pub fn new(config: ClientConfig) -> Result<Self, TollMeshError> {
        let http_client = reqwest::Client::builder()
            .timeout(config.timeout)
            .build()
            .map_err(|e| TollMeshError::new(
                ErrorCode::Internal,
                format!("Failed to create HTTP client: {}", e)
            ))?;

        Ok(Self { config, http_client })
    }

    /// Rate limit check
    pub async fn consume(
        &self,
        key: &str,
        limit: i32,
        window: Duration,
    ) -> Result<ConsumeResult, TollMeshError> {
        let req = ConsumeRequest {
            key: key.to_string(),
            limit,
            window: window.as_millis() as u64,
        };

        self.post("/consume", &req).await
    }

    /// Replay protection check
    pub async fn seen(
        &self,
        key: &str,
        ttl: Duration,
    ) -> Result<SeenResult, TollMeshError> {
        let req = SeenRequest {
            key: key.to_string(),
            ttl: ttl.as_millis() as u64,
        };

        self.post("/seen", &req).await
    }

    /// Get cached value
    pub async fn cache_get(
        &self,
        namespace: &str,
        key: &str,
    ) -> Result<CacheValue, TollMeshError> {
        let req = CacheGetRequest {
            namespace: namespace.to_string(),
            key: key.to_string(),
        };

        self.post("/cache/get", &req).await
    }

    /// Set cached value
    pub async fn cache_set(
        &self,
        namespace: &str,
        key: &str,
        value: &str,
        ttl: Option<Duration>,
    ) -> Result<(), TollMeshError> {
        let req = CacheSetRequest {
            namespace: namespace.to_string(),
            key: key.to_string(),
            value: value.to_string(),
            ttl: ttl.map(|d| d.as_millis() as u64),
        };

        let _: serde_json::Value = self.post("/cache/set", &req).await?;
        Ok(())
    }

    /// Health check
    pub async fn health(&self) -> Result<HealthResponse, TollMeshError> {
        self.get("/health").await
    }

    /// Get connected peers
    pub async fn get_peers(&self) -> Result<Vec<Peer>, TollMeshError> {
        #[derive(serde::Deserialize)]
        struct PeersResponse {
            peers: Vec<Peer>,
        }

        let response: PeersResponse = self.get("/peers").await?;
        Ok(response.peers)
    }

    async fn post<Req: serde::Serialize, Res: serde::de::DeserializeOwned>(
        &self,
        endpoint: &str,
        body: &Req,
    ) -> Result<Res, TollMeshError> {
        let url = format!("{}{}", self.config.base_url(), endpoint);

        let response = self.http_client
            .post(&url)
            .json(body)
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() {
                    TollMeshError::new(ErrorCode::DeadlineExceeded, format!("Request timeout: {}", e))
                } else if e.is_connect() {
                    TollMeshError::new(ErrorCode::Unavailable, format!("Connection failed: {}", e))
                } else {
                    TollMeshError::new(ErrorCode::Internal, format!("Request failed: {}", e))
                }
            })?;

        if !response.status().is_success() {
            let status = response.status().as_u16() as i32;
            let code = match status {
                400 => ErrorCode::InvalidRequest,
                404 => ErrorCode::NotFound,
                429 => ErrorCode::RateLimited,
                500 => ErrorCode::Internal,
                503 => ErrorCode::Unavailable,
                _ => ErrorCode::Internal,
            };

            return Err(TollMeshError::new(code, format!("HTTP {}", status)));
        }

        response.json()
            .await
            .map_err(|e| TollMeshError::new(ErrorCode::Internal, format!("Failed to parse response: {}", e)))
    }

    async fn get<Res: serde::de::DeserializeOwned>(
        &self,
        endpoint: &str,
    ) -> Result<Res, TollMeshError> {
        let url = format!("{}{}", self.config.base_url(), endpoint);

        let response = self.http_client
            .get(&url)
            .send()
            .await
            .map_err(|e| TollMeshError::new(ErrorCode::Unavailable, format!("Request failed: {}", e)))?;

        if !response.status().is_success() {
            return Err(TollMeshError::new(
                ErrorCode::Internal,
                format!("HTTP {}", response.status())
            ));
        }

        response.json()
            .await
            .map_err(|e| TollMeshError::new(ErrorCode::Internal, format!("Failed to parse response: {}", e)))
    }
}
