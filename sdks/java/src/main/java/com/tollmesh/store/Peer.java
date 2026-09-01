package com.tollmesh.store;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Peer {
    @JsonProperty("id")
    private String id;

    @JsonProperty("address")
    private String address;

    @JsonProperty("status")
    private String status;

    public Peer() {}

    public Peer(String id, String address, String status) {
        this.id = id;
        this.address = address;
        this.status = status;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
