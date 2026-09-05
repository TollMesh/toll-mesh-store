package com.tollmesh.store;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

/**
 * Java SDK Client for TollMeshCache
 * Provides methods for rate limiting, replay protection, and distributed caching
 */
public class Client implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(Client.class);
    private static final MediaType JSON = MediaType.get("application/json; charset=utf-8");

    private final ClientConfig config;
    private final OkHttpClient httpClient;
    private final ObjectMapper mapper;
    private final String baseUrl;

    /**
     * Initialize TollMeshCache client
     *
     * @param config Client configuration
     */
    public Client(ClientConfig config) {
        this.config = config;
        this.mapper = new ObjectMapper()
                // Response POJOs only declare the fields this SDK uses;
                // the server may include additional fields (internal
                // bookkeeping like vector clocks, timestamps) that should
                // be tolerated rather than breaking deserialization.
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        this.baseUrl = config.getBaseUrl();

        OkHttpClient.Builder builder = new OkHttpClient.Builder()
                .connectTimeout(Duration.ofMillis((long) config.getTimeout()))
                .readTimeout(Duration.ofMillis((long) config.getTimeout()))
                .writeTimeout(Duration.ofMillis((long) config.getTimeout()));

        if (!config.isVerifySSL()) {
            builder.hostnameVerifier((hostname, session) -> true);
        }

        // OkHttp has no built-in "default headers on every request" option
        // (unlike reqwest/axios/Python's requests.Session) -- an
        // Interceptor is the idiomatic way. Without this, ClientConfig's
        // apiKey was accepted and stored but never actually sent anywhere.
        if (config.getApiKey() != null && !config.getApiKey().isEmpty()) {
            String apiKey = config.getApiKey();
            builder.addInterceptor(chain -> chain.proceed(
                    chain.request().newBuilder().header("X-API-Key", apiKey).build()));
        }

        this.httpClient = builder.build();
    }

    /**
     * Initialize with default configuration
     */
    public Client() {
        this(new ClientConfig());
    }

    /**
     * Check and consume rate limit tokens
     *
     * @param key Rate limit key
     * @param limit Maximum tokens per window
     * @param window Time window duration
     * @return ConsumeResult with status and remaining tokens
     * @throws TollMeshException if operation fails
     */
    public ConsumeResult consume(String key, int limit, Duration window) throws TollMeshException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("key", key);
        body.put("limit", limit);
        body.put("window", window.toMillis());

        return post("/consume", body, ConsumeResult.class);
    }

    /**
     * Check replay protection
     *
     * @param key Nonce or unique identifier
     * @param ttl Time-to-live for tracking
     * @return SeenResult indicating if seen before
     * @throws TollMeshException if operation fails
     */
    public SeenResult seen(String key, Duration ttl) throws TollMeshException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("key", key);
        body.put("ttl", ttl.toMillis());

        return post("/seen", body, SeenResult.class);
    }

    /**
     * Get value from distributed cache
     *
     * @param namespace Cache namespace
     * @param key Cache key
     * @return CacheValue with value and existence flag
     * @throws TollMeshException if operation fails
     */
    /**
     * /cache/get is a GET endpoint taking query params (see
     * api/http.go handleCacheGet), not a POST with a JSON body.
     */
    public CacheValue cacheGet(String namespace, String key) throws TollMeshException {
        Map<String, String> query = new LinkedHashMap<>();
        query.put("namespace", namespace);
        query.put("key", key);
        return get("/cache/get", query, CacheValue.class);
    }

    /**
     * Set value in distributed cache
     *
     * @param namespace Cache namespace
     * @param key Cache key
     * @param value Value to cache
     * @param ttl Time-to-live (null = no expiration)
     * @throws TollMeshException if operation fails
     */
    public void cacheSet(String namespace, String key, String value, Duration ttl) throws TollMeshException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("namespace", namespace);
        body.put("key", key);
        body.put("value", value);
        if (ttl != null) {
            body.put("ttl", ttl.toMillis());
        }

        post("/cache/set", body, Void.class);
    }

    /**
     * Get server health status
     *
     * @return HealthResponse with status and stats
     * @throws TollMeshException if operation fails
     */
    public HealthResponse health() throws TollMeshException {
        return get("/health", HealthResponse.class);
    }

    /**
     * Get connected peers
     *
     * @return List of connected peers
     * @throws TollMeshException if operation fails
     */
    public List<Peer> getPeers() throws TollMeshException {
        PeersResponse response = get("/peers", PeersResponse.class);
        return response.getPeers();
    }

    // ===== Job Queues =====

    /**
     * Enqueue a job for distributed processing
     */
    public Job enqueue(String queue, String payload, int priority, int maxRetries, Duration deadline) throws TollMeshException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("queue", queue);
        body.put("payload", payload);
        body.put("priority", priority);
        body.put("max_retries", maxRetries);
        if (deadline != null) {
            body.put("deadline", deadline.toMillis());
        }
        return post("/queue/enqueue", body, Job.class);
    }

    /**
     * Claim the next available job from a queue
     */
    public Job claim(String queue, String workerId) throws TollMeshException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("queue", queue);
        body.put("worker_id", workerId);
        return post("/queue/claim", body, Job.class);
    }

    /**
     * Mark a claimed job as completed
     */
    public void complete(String queue, String jobId, String result) throws TollMeshException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("queue", queue);
        body.put("job_id", jobId);
        body.put("result", result == null ? "" : result);
        post("/queue/complete", body, Void.class);
    }

    /**
     * Mark a claimed job as failed, triggering retry or dead-lettering
     */
    public void fail(String queue, String jobId, String error) throws TollMeshException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("queue", queue);
        body.put("job_id", jobId);
        body.put("error", error);
        post("/queue/fail", body, Void.class);
    }

    /**
     * Get the current status of a job
     */
    public Job jobStatus(String queue, String jobId) throws TollMeshException {
        Map<String, String> query = new LinkedHashMap<>();
        query.put("queue", queue);
        query.put("job_id", jobId);
        return get("/queue/status", query, Job.class);
    }

    /**
     * Get statistics for a queue
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> queueStats(String queue) throws TollMeshException {
        Map<String, String> query = new LinkedHashMap<>();
        query.put("queue", queue);
        return get("/queue/stats", query, Map.class);
    }

    // ===== Sorted Sets =====

    /**
     * Add or update a member's score in a sorted set
     */
    public void zadd(String key, String member, double score) throws TollMeshException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("key", key);
        body.put("member", member);
        body.put("score", score);
        post("/zset/add", body, Void.class);
    }

    /**
     * Remove a member from a sorted set
     */
    public void zrem(String key, String member) throws TollMeshException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("key", key);
        body.put("member", member);
        post("/zset/remove", body, Void.class);
    }

    /**
     * Get a member's score
     */
    public ZScoreResult zscore(String key, String member) throws TollMeshException {
        Map<String, String> query = new LinkedHashMap<>();
        query.put("key", key);
        query.put("member", member);
        return get("/zset/score", query, ZScoreResult.class);
    }

    /**
     * Get a member's ascending-order rank (0 = lowest score)
     */
    public ZRankResult zrank(String key, String member) throws TollMeshException {
        Map<String, String> query = new LinkedHashMap<>();
        query.put("key", key);
        query.put("member", member);
        return get("/zset/rank", query, ZRankResult.class);
    }

    /**
     * Get a member's descending-order rank (0 = highest score)
     */
    public ZRankResult zrevrank(String key, String member) throws TollMeshException {
        Map<String, String> query = new LinkedHashMap<>();
        query.put("key", key);
        query.put("member", member);
        return get("/zset/revrank", query, ZRankResult.class);
    }

    /**
     * Get members with scores in [min, max], ascending order
     */
    public List<SortedSetMember> zrange(String key, double min, double max, long limit) throws TollMeshException {
        Map<String, String> query = new LinkedHashMap<>();
        query.put("key", key);
        query.put("min", String.valueOf(min));
        query.put("max", String.valueOf(max));
        query.put("limit", String.valueOf(limit));
        ZRangeResponse response = get("/zset/range", query, ZRangeResponse.class);
        return response.getMembers() != null ? response.getMembers() : Collections.emptyList();
    }

    /**
     * Get members with scores in [min, max], descending order (highest first).
     * Following Redis's ZREVRANGEBYSCORE convention, max comes before min.
     */
    public List<SortedSetMember> zrevrange(String key, double max, double min, long limit) throws TollMeshException {
        Map<String, String> query = new LinkedHashMap<>();
        query.put("key", key);
        query.put("max", String.valueOf(max));
        query.put("min", String.valueOf(min));
        query.put("limit", String.valueOf(limit));
        ZRangeResponse response = get("/zset/revrange", query, ZRangeResponse.class);
        return response.getMembers() != null ? response.getMembers() : Collections.emptyList();
    }

    /**
     * Get the number of members in a sorted set
     */
    public long zcard(String key) throws TollMeshException {
        Map<String, String> query = new LinkedHashMap<>();
        query.put("key", key);
        return get("/zset/card", query, ZCardResponse.class).getCard();
    }

    // ===== Streams =====

    /**
     * Append a new entry to a stream
     */
    public StreamEntry xadd(String stream, Map<String, String> fields) throws TollMeshException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("stream", stream);
        body.put("fields", fields);
        return post("/stream/add", body, StreamEntry.class);
    }

    /**
     * Get entries from a stream between start and end IDs
     */
    public List<StreamEntry> xrange(String stream, String start, String end, long limit) throws TollMeshException {
        Map<String, String> query = new LinkedHashMap<>();
        query.put("stream", stream);
        query.put("start", start);
        query.put("end", end);
        query.put("limit", String.valueOf(limit));
        XRangeResponse response = get("/stream/range", query, XRangeResponse.class);
        return response.getEntries() != null ? response.getEntries() : Collections.emptyList();
    }

    /**
     * Get the number of entries in a stream
     */
    public long xlen(String stream) throws TollMeshException {
        Map<String, String> query = new LinkedHashMap<>();
        query.put("stream", stream);
        return get("/stream/len", query, XLenResponse.class).getLength();
    }

    /**
     * Create a consumer group for a stream
     */
    public void xgroupCreate(String stream, String group) throws TollMeshException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("stream", stream);
        body.put("group", group);
        post("/stream/group/create", body, Void.class);
    }

    /**
     * Read unacknowledged entries for a consumer in a group.
     * First call for a given consumer registers it in the group. Entries
     * remain re-deliverable until acknowledged with {@link #xack}.
     */
    public List<StreamEntry> xreadgroup(String group, String consumer, String stream, long limit) throws TollMeshException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("stream", stream);
        body.put("group", group);
        body.put("consumer", consumer);
        body.put("limit", limit);
        XRangeResponse response = post("/stream/group/read", body, XRangeResponse.class);
        return response.getEntries() != null ? response.getEntries() : Collections.emptyList();
    }

    /**
     * Acknowledge that a consumer has processed up to entryId
     */
    public void xack(String stream, String group, String consumer, String entryId) throws TollMeshException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("stream", stream);
        body.put("group", group);
        body.put("consumer", consumer);
        body.put("id", entryId);
        post("/stream/group/ack", body, Void.class);
    }

    // ===== Pub/Sub =====

    public void subscribe(String subscriberId, String topic, String pattern) throws TollMeshException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("subscriber_id", subscriberId);
        body.put("topic", topic);
        body.put("pattern", pattern);
        post("/pubsub/subscribe", body, Void.class);
    }

    public void unsubscribe(String subscriberId, String topic) throws TollMeshException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("subscriber_id", subscriberId);
        body.put("topic", topic);
        post("/pubsub/unsubscribe", body, Void.class);
    }

    @SuppressWarnings("unchecked")
    public int publish(String topic, String publisher, String payload) throws TollMeshException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("topic", topic);
        body.put("publisher", publisher);
        body.put("payload", payload);
        Map<String, Object> response = post("/pubsub/publish", body, Map.class);
        return ((Number) response.get("delivered_count")).intValue();
    }

    @SuppressWarnings("unchecked")
    public List<Object> poll(String subscriberId, int limit, long timeoutMs) throws TollMeshException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("subscriber_id", subscriberId);
        body.put("limit", limit);
        body.put("timeout_ms", timeoutMs);
        Map<String, Object> response = post("/pubsub/poll", body, Map.class);
        List<Object> messages = (List<Object>) response.get("messages");
        return messages != null ? messages : Collections.emptyList();
    }

    @SuppressWarnings("unchecked")
    public List<String> getTopics() throws TollMeshException {
        Map<String, Object> response = get("/pubsub/topics", Map.class);
        List<String> topics = (List<String>) response.get("topics");
        return topics != null ? topics : Collections.emptyList();
    }

    @SuppressWarnings("unchecked")
    public List<String> getTopicSubscribers(String topic) throws TollMeshException {
        Map<String, String> query = new LinkedHashMap<>();
        query.put("topic", topic);
        Map<String, Object> response = get("/pubsub/subscribers", query, Map.class);
        List<String> subscribers = (List<String>) response.get("subscribers");
        return subscribers != null ? subscribers : Collections.emptyList();
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> pubsubStats() throws TollMeshException {
        return get("/pubsub/stats", Map.class);
    }

    // ===== Transactions =====

    @SuppressWarnings("unchecked")
    public Map<String, Object> beginTransaction(String txnId) throws TollMeshException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("txn_id", txnId);
        return post("/txn/begin", body, Map.class);
    }

    public void addTransactionOperation(String txnId, String type, String namespace, String key, String value) throws TollMeshException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("txn_id", txnId);
        body.put("type", type);
        body.put("namespace", namespace);
        body.put("key", key);
        body.put("value", value);
        post("/txn/operation", body, Void.class);
    }

    public void commitTransaction(String txnId) throws TollMeshException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("txn_id", txnId);
        post("/txn/commit", body, Void.class);
    }

    public void rollbackTransaction(String txnId) throws TollMeshException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("txn_id", txnId);
        post("/txn/rollback", body, Void.class);
    }

    @SuppressWarnings("unchecked")
    public String transactionStatus(String txnId) throws TollMeshException {
        Map<String, String> query = new LinkedHashMap<>();
        query.put("txn_id", txnId);
        Map<String, Object> response = get("/txn/status", query, Map.class);
        return (String) response.get("status");
    }

    // ===== Persistence =====

    public void createSnapshot() throws TollMeshException {
        post("/persistence/snapshot", new LinkedHashMap<>(), Void.class);
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getLatestSnapshot() throws TollMeshException {
        return get("/persistence/snapshot/latest", Map.class);
    }

    public void restoreFromLatestSnapshot() throws TollMeshException {
        post("/persistence/restore", new LinkedHashMap<>(), Void.class);
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> persistenceStats() throws TollMeshException {
        return get("/persistence/stats", Map.class);
    }

    // ===== Scripting: Pipelines (safe operation composition) =====

    public void registerPipeline(String name, List<Map<String, Object>> steps) throws TollMeshException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("name", name);
        body.put("steps", steps);
        post("/pipeline/register", body, Void.class);
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> executePipeline(String name) throws TollMeshException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("name", name);
        return post("/pipeline/execute", body, Map.class);
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> executeInlinePipeline(List<Map<String, Object>> steps) throws TollMeshException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("steps", steps);
        return post("/pipeline/execute-inline", body, Map.class);
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getPipeline(String name) throws TollMeshException {
        Map<String, String> query = new LinkedHashMap<>();
        query.put("name", name);
        return get("/pipeline/get", query, Map.class);
    }

    @SuppressWarnings("unchecked")
    public List<Object> listPipelines() throws TollMeshException {
        Map<String, Object> response = get("/pipeline/list", Map.class);
        List<Object> pipelines = (List<Object>) response.get("pipelines");
        return pipelines != null ? pipelines : Collections.emptyList();
    }

    public void deletePipeline(String name) throws TollMeshException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("name", name);
        post("/pipeline/delete", body, Void.class);
    }

    // ===== Scripting: WASM (real arbitrary Go code execution) =====

    @SuppressWarnings("unchecked")
    public Map<String, Object> compileScript(String name, String source) throws TollMeshException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("name", name);
        body.put("source", source);
        return post("/script/compile", body, Map.class, Duration.ofSeconds(65));
    }

    @SuppressWarnings("unchecked")
    public String executeScript(String name, String input) throws TollMeshException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("name", name);
        body.put("input", input);
        Map<String, Object> response = post("/script/execute", body, Map.class);
        return (String) response.get("output");
    }

    @SuppressWarnings("unchecked")
    public String executeInlineScript(String source, String input) throws TollMeshException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("source", source);
        body.put("input", input);
        // execute-inline compiles the script itself, so it needs the same
        // extended timeout as compileScript, not just execute's normal one.
        Map<String, Object> response = post("/script/execute-inline", body, Map.class, Duration.ofSeconds(65));
        return (String) response.get("output");
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getScript(String name) throws TollMeshException {
        Map<String, String> query = new LinkedHashMap<>();
        query.put("name", name);
        return get("/script/get", query, Map.class);
    }

    @SuppressWarnings("unchecked")
    public List<Object> listScripts() throws TollMeshException {
        Map<String, Object> response = get("/script/list", Map.class);
        List<Object> scripts = (List<Object>) response.get("scripts");
        return scripts != null ? scripts : Collections.emptyList();
    }

    public void deleteScript(String name) throws TollMeshException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("name", name);
        post("/script/delete", body, Void.class);
    }

    // ===== Search =====

    public void indexDocument(String id, String content, Map<String, Object> metadata, List<Float> vector) throws TollMeshException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("id", id);
        body.put("content", content);
        if (metadata != null) body.put("metadata", metadata);
        if (vector != null) body.put("vector", vector);
        post("/search/index", body, Void.class);
    }

    @SuppressWarnings("unchecked")
    public List<Object> searchBM25(String query, int topK) throws TollMeshException {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("query", query);
        params.put("topk", String.valueOf(topK));
        Map<String, Object> response = get("/search/bm25", params, Map.class);
        List<Object> results = (List<Object>) response.get("results");
        return results != null ? results : Collections.emptyList();
    }

    @SuppressWarnings("unchecked")
    public List<Object> searchVector(List<Float> vector, int topK) throws TollMeshException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("vector", vector);
        body.put("topk", topK);
        Map<String, Object> response = post("/search/vector", body, Map.class);
        List<Object> results = (List<Object>) response.get("results");
        return results != null ? results : Collections.emptyList();
    }

    @SuppressWarnings("unchecked")
    public List<Object> searchHybrid(String query, List<Float> vector, int topK) throws TollMeshException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("query", query);
        body.put("vector", vector);
        body.put("topk", topK);
        Map<String, Object> response = post("/search/hybrid", body, Map.class);
        List<Object> results = (List<Object>) response.get("results");
        return results != null ? results : Collections.emptyList();
    }

    public void deleteSearchDocument(String id) throws TollMeshException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("id", id);
        post("/search/delete", body, Void.class);
    }

    // ===== Ranking =====

    @SuppressWarnings("unchecked")
    public List<Object> rank(List<Map<String, Object>> items, String strategy, Map<String, Float> boosts) throws TollMeshException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("items", items);
        body.put("strategy", strategy);
        if (boosts != null) body.put("boosts", boosts);
        Map<String, Object> response = post("/rank", body, Map.class);
        List<Object> result = (List<Object>) response.get("items");
        return result != null ? result : Collections.emptyList();
    }

    // ===== Metrics =====

    @SuppressWarnings("unchecked")
    public Map<String, Object> getMetrics() throws TollMeshException {
        return get("/metrics", Map.class);
    }

    public String getPrometheusMetrics() throws TollMeshException {
        Request request = new Request.Builder()
                .url(baseUrl + "/metrics/prometheus")
                .get()
                .build();

        try (Response response = httpClient.newCall(request).execute()) {
            return response.body() != null ? response.body().string() : "";
        } catch (IOException e) {
            throw new TollMeshException(ErrorCode.UNAVAILABLE, "Request failed: " + e.getMessage(), e);
        }
    }

    private <T> T post(String endpoint, Map<String, Object> body, Class<T> responseType) throws TollMeshException {
        return post(endpoint, body, responseType, null);
    }

    // scriptCompileTimeout, when non-null, overrides the client's configured
    // timeout for this one call. TinyGo compilation (used by /script/compile
    // and /script/execute-inline) legitimately takes several real seconds --
    // the server allows up to 60s for it -- which is longer than this SDK's
    // default 5s HTTP timeout. Every other call keeps the configured default;
    // only the two compile-triggering endpoints need the longer allowance.
    private <T> T post(String endpoint, Map<String, Object> body, Class<T> responseType, Duration scriptCompileTimeout) throws TollMeshException {
        try {
            String json = mapper.writeValueAsString(body);
            RequestBody requestBody = RequestBody.create(json, JSON);

            Request request = new Request.Builder()
                    .url(baseUrl + endpoint)
                    .post(requestBody)
                    .build();

            OkHttpClient client = httpClient;
            if (scriptCompileTimeout != null) {
                client = httpClient.newBuilder().readTimeout(scriptCompileTimeout).build();
            }

            return executeRequest(client, request, responseType);
        } catch (IOException e) {
            throw new TollMeshException(ErrorCode.INTERNAL, "Failed to serialize request: " + e.getMessage(), e);
        }
    }

    private <T> T get(String endpoint, Class<T> responseType) throws TollMeshException {
        Request request = new Request.Builder()
                .url(baseUrl + endpoint)
                .get()
                .build();

        return executeRequest(request, responseType);
    }

    private <T> T get(String endpoint, Map<String, String> query, Class<T> responseType) throws TollMeshException {
        StringBuilder url = new StringBuilder(baseUrl + endpoint);
        if (query != null && !query.isEmpty()) {
            url.append('?');
            boolean first = true;
            for (Map.Entry<String, String> entry : query.entrySet()) {
                if (!first) {
                    url.append('&');
                }
                url.append(URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8));
                url.append('=');
                url.append(URLEncoder.encode(String.valueOf(entry.getValue()), StandardCharsets.UTF_8));
                first = false;
            }
        }

        Request request = new Request.Builder()
                .url(url.toString())
                .get()
                .build();

        return executeRequest(request, responseType);
    }

    private <T> T executeRequest(Request request, Class<T> responseType) throws TollMeshException {
        return executeRequest(httpClient, request, responseType);
    }

    private <T> T executeRequest(OkHttpClient client, Request request, Class<T> responseType) throws TollMeshException {
        try (Response response = client.newCall(request).execute()) {
            String body = response.body() != null ? response.body().string() : "";

            if (!response.isSuccessful()) {
                handleErrorResponse(body, response.code());
            }

            if (body.isEmpty()) {
                return null;
            }

            return mapper.readValue(body, responseType);
        } catch (IOException e) {
            throw new TollMeshException(ErrorCode.UNAVAILABLE, "Request failed: " + e.getMessage(), e);
        }
    }

    private void handleErrorResponse(String body, int statusCode) throws TollMeshException {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> error = mapper.readValue(body, Map.class);
            int code = ((Number) error.getOrDefault("code", statusCode)).intValue();
            // /consume, /seen, /cache/* use "message"; the job queue,
            // sorted set, and stream endpoints use ErrorResponse{"error": ...}
            // from api/http.go.
            Object message = error.getOrDefault("message", error.getOrDefault("error", "Error " + code));
            throw new TollMeshException(ErrorCode.fromCode(code), String.valueOf(message));
        } catch (IOException e) {
            throw new TollMeshException(ErrorCode.fromCode(statusCode), "HTTP " + statusCode, e);
        }
    }

    @Override
    public void close() {
        httpClient.dispatcher().executorService().shutdown();
    }
}
