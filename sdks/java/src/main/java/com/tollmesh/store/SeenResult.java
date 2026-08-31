package com.tollmesh.store;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

/**
 * Result of a replay protection check
 */
public class SeenResult {
    @JsonProperty("seen")
    private boolean seen;

    @JsonProperty("error")
    private String error;

    /**
     * Create empty result
     */
    public SeenResult() {}

    /**
     * Create result
     */
    public SeenResult(boolean seen) {
        this.seen = seen;
    }

    /**
     * Whether nonce was already seen (replay detected)
     */
    public boolean isSeen() {
        return seen;
    }

    public void setSeen(boolean seen) {
        this.seen = seen;
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
        SeenResult that = (SeenResult) o;
        return seen == that.seen && Objects.equals(error, that.error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(seen, error);
    }

    @Override
    public String toString() {
        return "SeenResult{" + "seen=" + seen + ", error='" + error + '\'' + '}';
    }
}
