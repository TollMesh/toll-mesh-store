package com.tollmesh.store;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response wrapper for stream length queries
 */
public class XLenResponse {
    @JsonProperty("length")
    private long length;

    public long getLength() { return length; }
}
