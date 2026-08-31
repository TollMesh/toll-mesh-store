package com.tollmesh.store;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

/**
 * Value retrieved from distributed cache
 */
public class CacheValue {
    @JsonProperty("value")
    private String value;

    @JsonProperty("exists")
    private boolean exists;

    @JsonProperty("error")
    private String error;

    /**
     * Create empty result
     */
    public CacheValue() {}

    /**
     * Create result
     */
    public CacheValue(String value, boolean exists) {
        this.value = value;
        this.exists = exists;
    }

    /**
     * Cached value (or null if not found)
     */
    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    /**
     * Whether key exists and is not expired
     */
    public boolean isExists() {
        return exists;
    }

    public void setExists(boolean exists) {
        this.exists = exists;
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
        CacheValue that = (CacheValue) o;
        return exists == that.exists &&
               Objects.equals(value, that.value) &&
               Objects.equals(error, that.error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, exists, error);
    }

    @Override
    public String toString() {
        return "CacheValue{" +
               "value='" + value + '\'' +
               ", exists=" + exists +
               ", error='" + error + '\'' +
               '}';
    }
}
