package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/TollMesh/toll-mesh-store/coordination"
	"github.com/TollMesh/toll-mesh-store/core"
)

// HTTPServer provides REST API for MeshStore operations
type HTTPServer struct {
	store       core.Store
	coordinator *coordination.GossipCoordinator
	mux         *http.ServeMux
	server      *http.Server
}

// ConsumeRequest represents a rate limit request
type ConsumeRequest struct {
	Key    string `json:"key"`
	Limit  int    `json:"limit"`
	Window int    `json:"window"` // milliseconds
}

// ConsumeResponse represents a rate limit response
type ConsumeResponse struct {
	OK        bool   `json:"ok"`
	Remaining int    `json:"remaining"`
	ResetAt   int64  `json:"reset_at"`
	Error     string `json:"error,omitempty"`
}

// SeenRequest represents a replay protection request
type SeenRequest struct {
	Key string `json:"key"`
	TTL int    `json:"ttl"` // milliseconds
}

// SeenResponse represents a replay protection response
type SeenResponse struct {
	Seen  bool   `json:"seen"`
	Error string `json:"error,omitempty"`
}

// CacheRequest represents a cache operation request
type CacheRequest struct {
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
	Value     string `json:"value,omitempty"`
	TTL       int    `json:"ttl"` // milliseconds
}

// CacheResponse represents a cache operation response
type CacheResponse struct {
	Value  string `json:"value,omitempty"`
	Exists bool   `json:"exists"`
	Error  string `json:"error,omitempty"`
}

// HealthResponse represents health check response
type HealthResponse struct {
	Status string                 `json:"status"`
	Node   string                 `json:"node"`
	Peers  int                    `json:"peers"`
	Stats  map[string]interface{} `json:"stats,omitempty"`
}

// NewHTTPServer creates a new HTTP API server
func NewHTTPServer(addr string, store core.Store, coordinator *coordination.GossipCoordinator) *HTTPServer {
	hs := &HTTPServer{
		store:       store,
		coordinator: coordinator,
		mux:         http.NewServeMux(),
	}

	// Register handlers
	hs.mux.HandleFunc("/health", hs.handleHealth)
	hs.mux.HandleFunc("/consume", hs.handleConsume)
	hs.mux.HandleFunc("/seen", hs.handleSeen)
	hs.mux.HandleFunc("/cache/get", hs.handleCacheGet)
	hs.mux.HandleFunc("/cache/set", hs.handleCacheSet)
	hs.mux.HandleFunc("/peers", hs.handlePeers)

	hs.server = &http.Server{
		Addr:    addr,
		Handler: hs.mux,
	}

	return hs
}

// Start starts the HTTP server
func (hs *HTTPServer) Start() error {
	return hs.server.ListenAndServe()
}

// Stop gracefully shuts down the HTTP server
func (hs *HTTPServer) Stop() error {
	return hs.server.Close()
}

// handleHealth handles health check requests
func (hs *HTTPServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	peers := hs.coordinator.GetPeers()
	response := HealthResponse{
		Status: "healthy",
		Node:   hs.coordinator.GetStats()["node_id"].(string),
		Peers:  len(peers),
		Stats:  hs.coordinator.GetStats(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleConsume handles rate limit requests
func (hs *HTTPServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req ConsumeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	result, err := hs.store.Consume(r.Context(), req.Key, req.Limit, time.Duration(req.Window)*time.Millisecond)
	response := ConsumeResponse{
		OK:        result.OK,
		Remaining: result.Remaining,
		ResetAt:   result.ResetAt,
	}

	if err != nil {
		response.Error = err.Error()
		w.WriteHeader(http.StatusInternalServerError)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleSeen handles replay protection requests
func (hs *HTTPServer) handleSeen(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req SeenRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	seen, err := hs.store.Seen(r.Context(), req.Key, time.Duration(req.TTL)*time.Millisecond)
	response := SeenResponse{
		Seen: seen,
	}

	if err != nil {
		response.Error = err.Error()
		w.WriteHeader(http.StatusInternalServerError)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleCacheGet handles cache get requests
func (hs *HTTPServer) handleCacheGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ns := r.URL.Query().Get("namespace")
	key := r.URL.Query().Get("key")

	if ns == "" || key == "" {
		http.Error(w, "Missing namespace or key", http.StatusBadRequest)
		return
	}

	value, exists, err := hs.store.Get(r.Context(), ns, key)
	response := CacheResponse{
		Value:  string(value),
		Exists: exists,
	}

	if err != nil {
		response.Error = err.Error()
		w.WriteHeader(http.StatusInternalServerError)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleCacheSet handles cache set requests
func (hs *HTTPServer) handleCacheSet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req CacheRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	err := hs.store.Set(r.Context(), req.Namespace, req.Key, []byte(req.Value), time.Duration(req.TTL)*time.Millisecond)
	response := CacheResponse{
		Exists: true,
	}

	if err != nil {
		response.Error = err.Error()
		w.WriteHeader(http.StatusInternalServerError)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handlePeers handles peer list requests
func (hs *HTTPServer) handlePeers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	peers := hs.coordinator.GetPeers()
	peerList := make([]map[string]interface{}, len(peers))

	for i, peer := range peers {
		peerList[i] = map[string]interface{}{
			"id":      peer.ID,
			"address": peer.Address,
			"port":    peer.Port,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"peers": peerList,
		"count": len(peers),
	})
}
