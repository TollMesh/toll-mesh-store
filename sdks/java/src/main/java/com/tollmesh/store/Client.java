package com.tollmesh.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
        this.mapper = new ObjectMapper();
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
    public CacheValue cacheGet(String namespace, String key) throws TollMeshException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("namespace", namespace);
        body.put("key", key);

        return post("/cache/get", body, CacheValue.class);
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
            String message = (String) error.getOrDefault("message", "Error " + code);
            throw new TollMeshException(ErrorCode.fromCode(code), message);
        } catch (IOException e) {
            throw new TollMeshException(ErrorCode.fromCode(statusCode), "HTTP " + statusCode, e);
        }
    }

    @Override
    public void close() {
        httpClient.dispatcher().executorService().shutdown();
    }
}
