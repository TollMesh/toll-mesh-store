package com.tollmesh.store;

import java.util.concurrent.*;
import java.time.Duration;

/**
 * Async TollMeshCache Client using CompletableFuture
 */
public class AsyncClient {
    private final Client client;
    private final ExecutorService executor;

    /**
     * Create async client
     */
    public AsyncClient(ClientConfig config) {
        this.client = new Client(config);
        this.executor = Executors.newFixedThreadPool(config.getConnectionPoolSize());
    }

    /**
     * Async rate limit check
     */
    public CompletableFuture<ConsumeResult> consumeAsync(String key, int limit, Duration window) {
        return CompletableFuture.supplyAsync(
            () -> {
                try {
                    return client.consume(key, limit, window);
                } catch (TollMeshException e) {
                    throw new CompletionException(e);
                }
            },
            executor
        );
    }

    /**
     * Async replay protection check
     */
    public CompletableFuture<SeenResult> seenAsync(String key, Duration ttl) {
        return CompletableFuture.supplyAsync(
            () -> {
                try {
                    return client.seen(key, ttl);
                } catch (TollMeshException e) {
                    throw new CompletionException(e);
                }
            },
            executor
        );
    }

    /**
     * Async cache get
     */
    public CompletableFuture<CacheValue> cacheGetAsync(String namespace, String key) {
        return CompletableFuture.supplyAsync(
            () -> {
                try {
                    return client.cacheGet(namespace, key);
                } catch (TollMeshException e) {
                    throw new CompletionException(e);
                }
            },
            executor
        );
    }

    /**
     * Async cache set
     */
    public CompletableFuture<Void> cacheSetAsync(String namespace, String key, String value, Duration ttl) {
        return CompletableFuture.runAsync(
            () -> {
                try {
                    client.cacheSet(namespace, key, value, ttl);
                } catch (TollMeshException e) {
                    throw new CompletionException(e);
                }
            },
            executor
        );
    }

    /**
     * Async health check
     */
    public CompletableFuture<HealthResponse> healthAsync() {
        return CompletableFuture.supplyAsync(
            () -> {
                try {
                    return client.health();
                } catch (TollMeshException e) {
                    throw new CompletionException(e);
                }
            },
            executor
        );
    }

    /**
     * Close client
     */
    public void close() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }
        client.close();
    }
}
