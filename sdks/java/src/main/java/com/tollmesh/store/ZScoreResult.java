package com.tollmesh.store;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Result of a sorted-set score lookup
 */
public class ZScoreResult {
    @JsonProperty("score")
    private Double score;

    @JsonProperty("exists")
    private boolean exists;

    public Double getScore() { return score; }
    public boolean isExists() { return exists; }
}
