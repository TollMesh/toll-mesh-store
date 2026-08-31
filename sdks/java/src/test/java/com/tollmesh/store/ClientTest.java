package com.tollmesh.store;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import java.time.Duration;

public class ClientTest {
    private ClientConfig config;

    @BeforeEach
    public void setUp() {
        config = new ClientConfig()
            .setHost("localhost")
            .setPort(8080);
    }

    @Test
    public void testClientCreation() {
        Client client = new Client(config);
        assertNotNull(client);
        client.close();
    }

    @Test
    public void testConfigBuilder() {
        ClientConfig cfg = new ClientConfig()
            .setHost("api.example.com")
            .setPort(443)
            .setScheme("https");

        assertEquals("api.example.com", cfg.getHost());
        assertEquals(443, cfg.getPort());
        assertEquals("https", cfg.getScheme());
        assertEquals("https://api.example.com:443", cfg.getBaseUrl());
    }

    @Test
    public void testConsumeResult() {
        ConsumeResult result = new ConsumeResult(true, 99, 1234567890L);
        assertTrue(result.isOk());
        assertEquals(99, result.getRemaining());
        assertEquals(1234567890L, result.getResetAt());
    }

    @Test
    public void testSeenResult() {
        SeenResult result = new SeenResult(true);
        assertTrue(result.isSeen());
    }

    @Test
    public void testCacheValue() {
        CacheValue cache = new CacheValue("test-value", true);
        assertEquals("test-value", cache.getValue());
        assertTrue(cache.isExists());
    }

    @Test
    public void testHealthResponse() {
        HealthResponse health = new HealthResponse("healthy", "node-1", 3);
        assertTrue(health.isHealthy());
        assertEquals("node-1", health.getNode());
        assertEquals(3, health.getPeers());
    }
}
