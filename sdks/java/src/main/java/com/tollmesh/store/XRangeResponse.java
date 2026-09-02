package com.tollmesh.store;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * Response wrapper for stream range and consumer-group read queries
 */
public class XRangeResponse {
    @JsonProperty("entries")
    private List<StreamEntry> entries;

    public List<StreamEntry> getEntries() { return entries; }
}
