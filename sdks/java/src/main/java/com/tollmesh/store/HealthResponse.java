package com.tollmesh.store;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Server health response
 */
public class HealthResponse {
    @JsonProperty("status")
    private String status;

    @JsonProperty("node")
    private String node;

    @JsonProperty("peers")
    private int peers;

    @JsonProperty("stats")
    private Map<String, Object> stats;

    /**
     * Create empty response
     */
    public HealthResponse() {}

    /**
     * Create response
     */
    public HealthResponse(String status, String node, int peers) {
        this.status = status;
        this.node = node;
        this.peers = peers;
        this.stats = new HashMap<>();
    }

    /**
     * Node health status: "healthy", "degraded", "unhealthy"
     */
    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    /**
     * Node ID/name
     */
    public String getNode() {
        return node;
    }

    public void setNode(String node) {
        this.node = node;
    }

    /**
     * Number of connected peers
     */
    public int getPeers() {
        return peers;
    }

    public void setPeers(int peers) {
        this.peers = peers;
    }

    /**
     * Operational statistics
     */
    public Map<String, Object> getStats() {
        return stats;
    }

    public void setStats(Map<String, Object> stats) {
        this.stats = stats;
    }

    /**
     * Get uptime in seconds
     */
    public long getUptimeSeconds() {
        if (stats == null) return 0;
        Object val = stats.get("uptime_seconds");
        return val instanceof Number ? ((Number) val).longValue() : 0;
    }

    /**
     * Get total requests
     */
    public long getRequestsTotal() {
        if (stats == null) return 0;
        Object val = stats.get("requests_total");
        return val instanceof Number ? ((Number) val).longValue() : 0;
    }

    /**
     * Get P99 latency in milliseconds
     */
    public double getLatencyP99Ms() {
        if (stats == null) return 0;
        Object val = stats.get("latency_p99_ms");
        return val instanceof Number ? ((Number) val).doubleValue() : 0;
    }

    /**
     * Get cache hit rate (0-1)
     */
    public double getCacheHitRate() {
        if (stats == null) return 0;
        Object val = stats.get("cache_hit_rate");
        return val instanceof Number ? ((Number) val).doubleValue() : 0;
    }

    public boolean isHealthy() {
        return "healthy".equals(status);
    }

    public boolean isDegraded() {
        return "degraded".equals(status);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HealthResponse that = (HealthResponse) o;
        return peers == that.peers &&
               Objects.equals(status, that.status) &&
               Objects.equals(node, that.node) &&
               Objects.equals(stats, that.stats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, node, peers, stats);
    }

    @Override
    public String toString() {
        return "HealthResponse{" +
               "status='" + status + '\'' +
               ", node='" + node + '\'' +
               ", peers=" + peers +
               ", stats=" + stats +
               '}';
    }
}
