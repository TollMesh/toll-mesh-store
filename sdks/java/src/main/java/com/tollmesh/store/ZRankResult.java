package com.tollmesh.store;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Result of a sorted-set rank lookup
 */
public class ZRankResult {
    @JsonProperty("rank")
    private Long rank;

    @JsonProperty("exists")
    private boolean exists;

    public Long getRank() { return rank; }
    public boolean isExists() { return exists; }
}
