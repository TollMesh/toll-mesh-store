package com.tollmesh.store;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A member of a sorted set, with its score
 */
public class SortedSetMember {
    @JsonProperty("member")
    private String member;

    @JsonProperty("score")
    private double score;

    @JsonProperty("timestamp")
    private long timestamp;

    @JsonProperty("node")
    private String node;

    public String getMember() { return member; }
    public double getScore() { return score; }
    public long getTimestamp() { return timestamp; }
    public String getNode() { return node; }
}
