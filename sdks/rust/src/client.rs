use crate::{ClientConfig, TollMeshError, ErrorCode};
use crate::types::*;
use std::collections::HashMap;
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

    // ===== Job Queues =====

    /// Enqueue a job for distributed processing
    pub async fn enqueue(
        &self,
        queue: &str,
        payload: &str,
        priority: i32,
        max_retries: i32,
        deadline: Option<Duration>,
    ) -> Result<Job, TollMeshError> {
        let req = EnqueueRequest {
            queue: queue.to_string(),
            payload: payload.to_string(),
            priority,
            max_retries,
            deadline: deadline.map(|d| d.as_millis() as u64),
        };
        self.post("/queue/enqueue", &req).await
    }

    /// Claim the next available job from a queue
    pub async fn claim(&self, queue: &str, worker_id: &str) -> Result<Job, TollMeshError> {
        let req = ClaimRequest {
            queue: queue.to_string(),
            worker_id: worker_id.to_string(),
        };
        self.post("/queue/claim", &req).await
    }

    /// Mark a claimed job as completed
    pub async fn complete(&self, queue: &str, job_id: &str, result: &str) -> Result<(), TollMeshError> {
        let req = CompleteRequest {
            queue: queue.to_string(),
            job_id: job_id.to_string(),
            result: result.to_string(),
        };
        let _: serde_json::Value = self.post("/queue/complete", &req).await?;
        Ok(())
    }

    /// Mark a claimed job as failed, triggering retry or dead-lettering
    pub async fn fail(&self, queue: &str, job_id: &str, error: &str) -> Result<(), TollMeshError> {
        let req = FailRequest {
            queue: queue.to_string(),
            job_id: job_id.to_string(),
            error: error.to_string(),
        };
        let _: serde_json::Value = self.post("/queue/fail", &req).await?;
        Ok(())
    }

    /// Get the current status of a job
    pub async fn job_status(&self, queue: &str, job_id: &str) -> Result<Job, TollMeshError> {
        self.get_with_query("/queue/status", &[("queue", queue), ("job_id", job_id)]).await
    }

    /// Get statistics for a queue
    pub async fn queue_stats(&self, queue: &str) -> Result<serde_json::Value, TollMeshError> {
        self.get_with_query("/queue/stats", &[("queue", queue)]).await
    }

    // ===== Sorted Sets =====

    /// Add or update a member's score in a sorted set
    pub async fn zadd(&self, key: &str, member: &str, score: f64) -> Result<(), TollMeshError> {
        let req = ZAddRequest {
            key: key.to_string(),
            member: member.to_string(),
            score,
        };
        let _: serde_json::Value = self.post("/zset/add", &req).await?;
        Ok(())
    }

    /// Remove a member from a sorted set
    pub async fn zrem(&self, key: &str, member: &str) -> Result<(), TollMeshError> {
        let req = ZMemberRequest {
            key: key.to_string(),
            member: member.to_string(),
        };
        let _: serde_json::Value = self.post("/zset/remove", &req).await?;
        Ok(())
    }

    /// Get a member's score
    pub async fn zscore(&self, key: &str, member: &str) -> Result<ZScoreResponse, TollMeshError> {
        self.get_with_query("/zset/score", &[("key", key), ("member", member)]).await
    }

    /// Get a member's ascending-order rank (0 = lowest score)
    pub async fn zrank(&self, key: &str, member: &str) -> Result<ZRankResponse, TollMeshError> {
        self.get_with_query("/zset/rank", &[("key", key), ("member", member)]).await
    }

    /// Get a member's descending-order rank (0 = highest score)
    pub async fn zrevrank(&self, key: &str, member: &str) -> Result<ZRankResponse, TollMeshError> {
        self.get_with_query("/zset/revrank", &[("key", key), ("member", member)]).await
    }

    /// Get members with scores in [min, max], ascending order
    pub async fn zrange(&self, key: &str, min: f64, max: f64, limit: i64) -> Result<Vec<SortedSetMember>, TollMeshError> {
        let response: ZRangeResponse = self
            .get_with_query("/zset/range", &[
                ("key", key.to_string()),
                ("min", min.to_string()),
                ("max", max.to_string()),
                ("limit", limit.to_string()),
            ])
            .await?;
        Ok(response.members)
    }

    /// Get members with scores in [min, max], descending order (highest first)
    pub async fn zrevrange(&self, key: &str, max: f64, min: f64, limit: i64) -> Result<Vec<SortedSetMember>, TollMeshError> {
        let response: ZRangeResponse = self
            .get_with_query("/zset/revrange", &[
                ("key", key.to_string()),
                ("max", max.to_string()),
                ("min", min.to_string()),
                ("limit", limit.to_string()),
            ])
            .await?;
        Ok(response.members)
    }

    /// Get the number of members in a sorted set
    pub async fn zcard(&self, key: &str) -> Result<i64, TollMeshError> {
        let response: ZCardResponse = self.get_with_query("/zset/card", &[("key", key)]).await?;
        Ok(response.card)
    }

    // ===== Streams =====

    /// Append a new entry to a stream
    pub async fn xadd(&self, stream: &str, fields: HashMap<String, String>) -> Result<StreamEntry, TollMeshError> {
        let req = XAddRequest {
            stream: stream.to_string(),
            fields,
        };
        self.post("/stream/add", &req).await
    }

    /// Get entries from a stream between start and end IDs
    pub async fn xrange(&self, stream: &str, start: &str, end: &str, limit: i64) -> Result<Vec<StreamEntry>, TollMeshError> {
        let response: XRangeResponse = self
            .get_with_query("/stream/range", &[
                ("stream", stream.to_string()),
                ("start", start.to_string()),
                ("end", end.to_string()),
                ("limit", limit.to_string()),
            ])
            .await?;
        Ok(response.entries)
    }

    /// Get the number of entries in a stream
    pub async fn xlen(&self, stream: &str) -> Result<i64, TollMeshError> {
        let response: XLenResponse = self.get_with_query("/stream/len", &[("stream", stream)]).await?;
        Ok(response.length)
    }

    /// Create a consumer group for a stream
    pub async fn xgroup_create(&self, stream: &str, group: &str) -> Result<(), TollMeshError> {
        let req = XGroupCreateRequest {
            stream: stream.to_string(),
            group: group.to_string(),
        };
        let _: serde_json::Value = self.post("/stream/group/create", &req).await?;
        Ok(())
    }

    /// Read unacknowledged entries for a consumer in a group.
    ///
    /// First call for a given consumer registers it in the group. Entries
    /// remain re-deliverable until acknowledged with `xack`.
    pub async fn xreadgroup(&self, group: &str, consumer: &str, stream: &str, limit: i64) -> Result<Vec<StreamEntry>, TollMeshError> {
        let req = XReadGroupRequest {
            stream: stream.to_string(),
            group: group.to_string(),
            consumer: consumer.to_string(),
            limit,
        };
        let response: XRangeResponse = self.post("/stream/group/read", &req).await?;
        Ok(response.entries)
    }

    /// Acknowledge that a consumer has processed up to entry_id
    pub async fn xack(&self, stream: &str, group: &str, consumer: &str, entry_id: &str) -> Result<(), TollMeshError> {
        let req = XAckRequest {
            stream: stream.to_string(),
            group: group.to_string(),
            consumer: consumer.to_string(),
            id: entry_id.to_string(),
        };
        let _: serde_json::Value = self.post("/stream/group/ack", &req).await?;
        Ok(())
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

    async fn get_with_query<Res: serde::de::DeserializeOwned, Q: serde::Serialize>(
        &self,
        endpoint: &str,
        query: &Q,
    ) -> Result<Res, TollMeshError> {
        let url = format!("{}{}", self.config.base_url(), endpoint);

        let response = self.http_client
            .get(&url)
            .query(query)
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
