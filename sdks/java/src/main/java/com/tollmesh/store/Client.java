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

    private <T> T post(String endpoint, Map<String, Object> body, Class<T> responseType) throws TollMeshException {
        try {
            String json = mapper.writeValueAsString(body);
            RequestBody requestBody = RequestBody.create(json, JSON);

            Request request = new Request.Builder()
                    .url(baseUrl + endpoint)
                    .post(requestBody)
                    .build();

            return executeRequest(request, responseType);
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
        try (Response response = httpClient.newCall(request).execute()) {
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
