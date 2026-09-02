package com.tollmesh.store;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

/**
 * An entry appended to a stream
 */
public class StreamEntry {
    @JsonProperty("id")
    private String id;

    @JsonProperty("timestamp")
    private long timestamp;

    @JsonProperty("fields")
    private Map<String, String> fields;

    @JsonProperty("node")
    private String node;

    @JsonProperty("sequence")
    private long sequence;

    public String getId() { return id; }
    public long getTimestamp() { return timestamp; }
    public Map<String, String> getFields() { return fields; }
    public String getNode() { return node; }
    public long getSequence() { return sequence; }
}
