package com.tollmesh.store;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

/**
 * Result of a rate limit consumption check
 */
public class ConsumeResult {
    @JsonProperty("ok")
    private boolean ok;

    @JsonProperty("remaining")
    private int remaining;

    @JsonProperty("reset_at")
    private long resetAt;

    @JsonProperty("error")
    private String error;

    /**
     * Create empty result
     */
    public ConsumeResult() {}

    /**
     * Create result with values
     */
    public ConsumeResult(boolean ok, int remaining, long resetAt) {
        this.ok = ok;
        this.remaining = remaining;
        this.resetAt = resetAt;
    }

    /**
     * Whether request is allowed
     */
    public boolean isOk() {
        return ok;
    }

    public void setOk(boolean ok) {
        this.ok = ok;
    }

    /**
     * Tokens remaining after this request
     */
    public int getRemaining() {
        return remaining;
    }

    public void setRemaining(int remaining) {
        this.remaining = remaining;
    }

    /**
     * Unix timestamp (milliseconds) when limit resets
     */
    public long getResetAt() {
        return resetAt;
    }

    public void setResetAt(long resetAt) {
        this.resetAt = resetAt;
    }

    /**
     * Error message if operation failed
     */
    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConsumeResult that = (ConsumeResult) o;
        return ok == that.ok &&
               remaining == that.remaining &&
               resetAt == that.resetAt &&
               Objects.equals(error, that.error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ok, remaining, resetAt, error);
    }

    @Override
    public String toString() {
        return "ConsumeResult{" +
               "ok=" + ok +
               ", remaining=" + remaining +
               ", resetAt=" + resetAt +
               ", error='" + error + '\'' +
               '}';
    }
}
