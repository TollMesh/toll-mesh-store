package com.tollmesh.store;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response wrapper for sorted-set cardinality queries
 */
public class ZCardResponse {
    @JsonProperty("card")
    private long card;

    public long getCard() { return card; }
}
