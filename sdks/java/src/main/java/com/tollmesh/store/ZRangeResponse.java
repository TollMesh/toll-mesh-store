package com.tollmesh.store;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * Response wrapper for sorted-set range queries
 */
public class ZRangeResponse {
    @JsonProperty("members")
    private List<SortedSetMember> members;

    public List<SortedSetMember> getMembers() { return members; }
}
