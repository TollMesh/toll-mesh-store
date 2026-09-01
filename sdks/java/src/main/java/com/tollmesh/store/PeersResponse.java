package com.tollmesh.store;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class PeersResponse {
    @JsonProperty("peers")
    private List<Peer> peers;

    @JsonProperty("error")
    private String error;

    public PeersResponse() {}

    public PeersResponse(List<Peer> peers) {
        this.peers = peers;
    }

    public List<Peer> getPeers() {
        return peers;
    }

    public void setPeers(List<Peer> peers) {
        this.peers = peers;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }
}
