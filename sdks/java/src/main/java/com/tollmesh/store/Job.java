package com.tollmesh.store;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A job in a distributed job queue
 */
public class Job {
    @JsonProperty("id")
    private String id;

    @JsonProperty("queue")
    private String queue;

    @JsonProperty("payload")
    private String payload;

    @JsonProperty("status")
    private String status;

    @JsonProperty("priority")
    private int priority;

    @JsonProperty("retry_count")
    private int retryCount;

    @JsonProperty("max_retries")
    private int maxRetries;

    @JsonProperty("result")
    private String result;

    @JsonProperty("error")
    private String error;

    @JsonProperty("created_at")
    private long createdAt;

    @JsonProperty("deadline_at")
    private long deadlineAt;

    public String getId() { return id; }
    public String getQueue() { return queue; }
    public String getPayload() { return payload; }
    public String getStatus() { return status; }
    public int getPriority() { return priority; }
    public int getRetryCount() { return retryCount; }
    public int getMaxRetries() { return maxRetries; }
    public String getResult() { return result; }
    public String getError() { return error; }
    public long getCreatedAt() { return createdAt; }
    public long getDeadlineAt() { return deadlineAt; }
}
