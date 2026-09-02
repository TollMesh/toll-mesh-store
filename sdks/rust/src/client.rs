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
    /// /cache/get is a GET endpoint taking query params (see
    /// api/http.go handleCacheGet), not a POST with a JSON body.
    pub async fn cache_get(
        &self,
        namespace: &str,
        key: &str,
    ) -> Result<CacheValue, TollMeshError> {
        self.get_with_query("/cache/get", &[("namespace", namespace), ("key", key)]).await
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

    // ===== Pub/Sub =====

    /// Subscribe to a topic with optional regex pattern matching
    pub async fn subscribe(&self, subscriber_id: &str, topic: &str, pattern: &str) -> Result<(), TollMeshError> {
        let req = SubscribeRequest { subscriber_id: subscriber_id.to_string(), topic: topic.to_string(), pattern: pattern.to_string() };
        let _: serde_json::Value = self.post("/pubsub/subscribe", &req).await?;
        Ok(())
    }

    /// Remove a subscription
    pub async fn unsubscribe(&self, subscriber_id: &str, topic: &str) -> Result<(), TollMeshError> {
        let req = UnsubscribeRequest { subscriber_id: subscriber_id.to_string(), topic: topic.to_string() };
        let _: serde_json::Value = self.post("/pubsub/unsubscribe", &req).await?;
        Ok(())
    }

    /// Publish a message to a topic; returns the number of subscribers it was delivered to
    pub async fn publish(&self, topic: &str, publisher: &str, payload: &str) -> Result<i64, TollMeshError> {
        let req = PublishRequest { topic: topic.to_string(), publisher: publisher.to_string(), payload: payload.to_string() };
        let response: PublishResponse = self.post("/pubsub/publish", &req).await?;
        Ok(response.delivered_count)
    }

    /// Retrieve up to limit currently-available messages for a subscriber,
    /// waiting up to timeout_ms if none are immediately available.
    pub async fn poll(&self, subscriber_id: &str, limit: i64, timeout_ms: i64) -> Result<Vec<serde_json::Value>, TollMeshError> {
        let req = PollRequest { subscriber_id: subscriber_id.to_string(), limit, timeout_ms };
        let response: PollResponse = self.post("/pubsub/poll", &req).await?;
        Ok(response.messages)
    }

    /// Get all known pub/sub topics
    pub async fn get_topics(&self) -> Result<Vec<String>, TollMeshError> {
        let response: TopicsResponse = self.get("/pubsub/topics").await?;
        Ok(response.topics)
    }

    /// Get subscriber IDs for a topic
    pub async fn get_topic_subscribers(&self, topic: &str) -> Result<Vec<String>, TollMeshError> {
        let response: SubscribersResponse = self.get_with_query("/pubsub/subscribers", &[("topic", topic)]).await?;
        Ok(response.subscribers)
    }

    /// Get pub/sub statistics
    pub async fn pubsub_stats(&self) -> Result<serde_json::Value, TollMeshError> {
        self.get("/pubsub/stats").await
    }

    // ===== Transactions =====

    /// Start a new transaction
    pub async fn begin_transaction(&self, txn_id: &str) -> Result<serde_json::Value, TollMeshError> {
        let req = TxnIdRequest { txn_id: txn_id.to_string() };
        self.post("/txn/begin", &req).await
    }

    /// Queue an operation within a pending transaction. Only "set" operations are applied on commit.
    pub async fn add_transaction_operation(&self, txn_id: &str, op_type: &str, namespace: &str, key: &str, value: &str) -> Result<(), TollMeshError> {
        let req = TxnOperationRequest {
            txn_id: txn_id.to_string(),
            op_type: op_type.to_string(),
            namespace: namespace.to_string(),
            key: key.to_string(),
            value: value.to_string(),
        };
        let _: serde_json::Value = self.post("/txn/operation", &req).await?;
        Ok(())
    }

    /// Commit a transaction, applying all of its queued "set" operations to the real cache atomically
    pub async fn commit_transaction(&self, txn_id: &str) -> Result<(), TollMeshError> {
        let req = TxnIdRequest { txn_id: txn_id.to_string() };
        let _: serde_json::Value = self.post("/txn/commit", &req).await?;
        Ok(())
    }

    /// Roll back a pending transaction, discarding its queued operations
    pub async fn rollback_transaction(&self, txn_id: &str) -> Result<(), TollMeshError> {
        let req = TxnIdRequest { txn_id: txn_id.to_string() };
        let _: serde_json::Value = self.post("/txn/rollback", &req).await?;
        Ok(())
    }

    /// Get the status of a transaction: pending, committed, rolled_back, or failed
    pub async fn transaction_status(&self, txn_id: &str) -> Result<String, TollMeshError> {
        let response: TxnStatusResponse = self.get_with_query("/txn/status", &[("txn_id", txn_id)]).await?;
        Ok(response.status)
    }

    // ===== Persistence =====

    /// Capture the current live store state to disk
    pub async fn create_snapshot(&self) -> Result<(), TollMeshError> {
        let _: serde_json::Value = self.post("/persistence/snapshot", &serde_json::json!({})).await?;
        Ok(())
    }

    /// Get the most recent snapshot
    pub async fn get_latest_snapshot(&self) -> Result<serde_json::Value, TollMeshError> {
        self.get("/persistence/snapshot/latest").await
    }

    /// Load the most recent snapshot and apply it to live store state
    pub async fn restore_from_latest_snapshot(&self) -> Result<(), TollMeshError> {
        let _: serde_json::Value = self.post("/persistence/restore", &serde_json::json!({})).await?;
        Ok(())
    }

    /// Get persistence statistics
    pub async fn persistence_stats(&self) -> Result<serde_json::Value, TollMeshError> {
        self.get("/persistence/stats").await
    }

    // ===== Scripting: Pipelines (safe operation composition) =====

    /// Register a named pipeline: an ordered list of steps, each naming an
    /// existing store operation (e.g. "zadd", "get", "set") plus its
    /// arguments. A step can save its result under a name for later steps
    /// to reference via "$name".
    pub async fn register_pipeline(&self, name: &str, steps: Vec<serde_json::Value>) -> Result<(), TollMeshError> {
        let req = RegisterPipelineRequest { name: name.to_string(), steps };
        let _: serde_json::Value = self.post("/pipeline/register", &req).await?;
        Ok(())
    }

    /// Run a registered pipeline by name
    pub async fn execute_pipeline(&self, name: &str) -> Result<serde_json::Value, TollMeshError> {
        let req = NameRequest { name: name.to_string() };
        self.post("/pipeline/execute", &req).await
    }

    /// Run an ad-hoc list of steps without registering them
    pub async fn execute_inline_pipeline(&self, steps: Vec<serde_json::Value>) -> Result<serde_json::Value, TollMeshError> {
        let req = ExecuteInlinePipelineRequest { steps };
        self.post("/pipeline/execute-inline", &req).await
    }

    /// Retrieve a registered pipeline by name
    pub async fn get_pipeline(&self, name: &str) -> Result<serde_json::Value, TollMeshError> {
        self.get_with_query("/pipeline/get", &[("name", name)]).await
    }

    /// List all registered pipelines
    pub async fn list_pipelines(&self) -> Result<Vec<serde_json::Value>, TollMeshError> {
        let response: PipelinesResponse = self.get("/pipeline/list").await?;
        Ok(response.pipelines)
    }

    /// Remove a registered pipeline
    pub async fn delete_pipeline(&self, name: &str) -> Result<(), TollMeshError> {
        let req = NameRequest { name: name.to_string() };
        let _: serde_json::Value = self.post("/pipeline/delete", &req).await?;
        Ok(())
    }

    // ===== Scripting: WASM (real arbitrary Go code execution) =====

    /// Compile Go source to a sandboxed WASM module via TinyGo and register
    /// it under name. This is slow (real seconds) and expected to happen
    /// far less often than execute_script.
    pub async fn compile_script(&self, name: &str, source: &str) -> Result<serde_json::Value, TollMeshError> {
        let req = CompileScriptRequest { name: name.to_string(), source: source.to_string() };
        self.post("/script/compile", &req).await
    }

    /// Run a previously compiled script by name, feeding input on stdin
    pub async fn execute_script(&self, name: &str, input: &str) -> Result<String, TollMeshError> {
        let req = ExecuteScriptRequest { name: name.to_string(), input: input.to_string() };
        let response: ScriptOutputResponse = self.post("/script/execute", &req).await?;
        Ok(response.output)
    }

    /// Compile and immediately run Go source without registering it
    pub async fn execute_inline_script(&self, source: &str, input: &str) -> Result<String, TollMeshError> {
        let req = ExecuteInlineScriptRequest { source: source.to_string(), input: input.to_string() };
        let response: ScriptOutputResponse = self.post("/script/execute-inline", &req).await?;
        Ok(response.output)
    }

    /// Retrieve a registered script by name
    pub async fn get_script(&self, name: &str) -> Result<serde_json::Value, TollMeshError> {
        self.get_with_query("/script/get", &[("name", name)]).await
    }

    /// List all registered scripts
    pub async fn list_scripts(&self) -> Result<Vec<serde_json::Value>, TollMeshError> {
        let response: ScriptsResponse = self.get("/script/list").await?;
        Ok(response.scripts)
    }

    /// Remove a registered script
    pub async fn delete_script(&self, name: &str) -> Result<(), TollMeshError> {
        let req = NameRequest { name: name.to_string() };
        let _: serde_json::Value = self.post("/script/delete", &req).await?;
        Ok(())
    }

    // ===== Search =====

    /// Add a document to the search index
    pub async fn index_document(&self, id: &str, content: &str, metadata: Option<serde_json::Value>, vector: Option<Vec<f32>>) -> Result<(), TollMeshError> {
        let req = IndexDocumentRequest { id: id.to_string(), content: content.to_string(), metadata, vector };
        let _: serde_json::Value = self.post("/search/index", &req).await?;
        Ok(())
    }

    /// Perform BM25 full-text search
    pub async fn search_bm25(&self, query: &str, top_k: i64) -> Result<Vec<serde_json::Value>, TollMeshError> {
        let response: SearchResultsResponse = self.get_with_query("/search/bm25", &[("query", query.to_string()), ("topk", top_k.to_string())]).await?;
        Ok(response.results)
    }

    /// Perform vector similarity search
    pub async fn search_vector(&self, vector: Vec<f32>, top_k: i64) -> Result<Vec<serde_json::Value>, TollMeshError> {
        let req = SearchVectorRequest { vector, topk: top_k };
        let response: SearchResultsResponse = self.post("/search/vector", &req).await?;
        Ok(response.results)
    }

    /// Perform hybrid BM25 + vector search
    pub async fn search_hybrid(&self, query: &str, vector: Vec<f32>, top_k: i64) -> Result<Vec<serde_json::Value>, TollMeshError> {
        let req = SearchHybridRequest { query: query.to_string(), vector, topk: top_k };
        let response: SearchResultsResponse = self.post("/search/hybrid", &req).await?;
        Ok(response.results)
    }

    /// Remove a document from the search index
    pub async fn delete_search_document(&self, id: &str) -> Result<(), TollMeshError> {
        let req = DeleteDocumentRequest { id: id.to_string() };
        let _: serde_json::Value = self.post("/search/delete", &req).await?;
        Ok(())
    }

    // ===== Ranking =====

    /// Re-rank a list of already-scored items using the named strategy
    /// ("bm25", "vector", "llm", or "context"). boosts (for "context") is
    /// a per-ID score multiplier map.
    pub async fn rank(&self, items: Vec<serde_json::Value>, strategy: &str, boosts: Option<std::collections::HashMap<String, f32>>) -> Result<Vec<serde_json::Value>, TollMeshError> {
        let req = RankRequest { items, strategy: strategy.to_string(), boosts };
        let response: RankResponse = self.post("/rank", &req).await?;
        Ok(response.items)
    }

    // ===== Metrics =====

    /// Get current operational metrics
    pub async fn get_metrics(&self) -> Result<serde_json::Value, TollMeshError> {
        self.get("/metrics").await
    }

    /// Get metrics formatted for Prometheus scraping
    pub async fn get_prometheus_metrics(&self) -> Result<String, TollMeshError> {
        let url = format!("{}/metrics/prometheus", self.config.base_url());
        let response = self.http_client
            .get(&url)
            .send()
            .await
            .map_err(|e| TollMeshError::new(ErrorCode::Unavailable, format!("Request failed: {}", e)))?;
        response.text()
            .await
            .map_err(|e| TollMeshError::new(ErrorCode::Internal, format!("Failed to read response: {}", e)))
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
