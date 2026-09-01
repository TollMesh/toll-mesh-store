package api

import (
	"encoding/json"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/toll-mesh/store/coordination"
	"github.com/toll-mesh/store/core"
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

	// Job Queues
	hs.mux.HandleFunc("/queue/enqueue", hs.handleEnqueue)
	hs.mux.HandleFunc("/queue/claim", hs.handleClaimJob)
	hs.mux.HandleFunc("/queue/complete", hs.handleCompleteJob)
	hs.mux.HandleFunc("/queue/fail", hs.handleFailJob)
	hs.mux.HandleFunc("/queue/status", hs.handleJobStatus)
	hs.mux.HandleFunc("/queue/stats", hs.handleQueueStats)

	// Sorted Sets
	hs.mux.HandleFunc("/zset/add", hs.handleZAdd)
	hs.mux.HandleFunc("/zset/remove", hs.handleZRem)
	hs.mux.HandleFunc("/zset/score", hs.handleZScore)
	hs.mux.HandleFunc("/zset/rank", hs.handleZRank)
	hs.mux.HandleFunc("/zset/revrank", hs.handleZRevRank)
	hs.mux.HandleFunc("/zset/range", hs.handleZRange)
	hs.mux.HandleFunc("/zset/revrange", hs.handleZRevRange)
	hs.mux.HandleFunc("/zset/card", hs.handleZCard)

	// Streams
	hs.mux.HandleFunc("/stream/add", hs.handleXAdd)
	hs.mux.HandleFunc("/stream/range", hs.handleXRange)
	hs.mux.HandleFunc("/stream/len", hs.handleXLen)
	hs.mux.HandleFunc("/stream/group/create", hs.handleXGroupCreate)
	hs.mux.HandleFunc("/stream/group/read", hs.handleXReadGroup)
	hs.mux.HandleFunc("/stream/group/ack", hs.handleXAck)

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

// ===== Job Queues =====

// EnqueueRequest represents a job enqueue request
type EnqueueRequest struct {
	Queue      string `json:"queue"`
	Payload    string `json:"payload"`
	Priority   int    `json:"priority"`
	MaxRetries int    `json:"max_retries"`
	Deadline   int64  `json:"deadline"` // milliseconds, 0 = none
}

// ErrorResponse is a generic error envelope
type ErrorResponse struct {
	Error string `json:"error"`
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, ErrorResponse{Error: err.Error()})
}

func (hs *HTTPServer) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req EnqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	// A zero deadline means "expires immediately" to the underlying queue
	// (DeadlineAt = now + deadline), not "no deadline" -- there is no
	// unlimited-deadline concept in this queue. An unset/omitted deadline
	// field from a client should not silently create a dead-on-arrival
	// job, so default it the same way queue.DefaultJobOptions() does.
	deadline := time.Duration(req.Deadline) * time.Millisecond
	if deadline <= 0 {
		deadline = 24 * time.Hour
	}

	job, err := hs.store.Enqueue(r.Context(), req.Queue, []byte(req.Payload), req.Priority, req.MaxRetries, deadline)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, http.StatusOK, job)
}

// ClaimRequest represents a job claim request
type ClaimRequest struct {
	Queue    string `json:"queue"`
	WorkerID string `json:"worker_id"`
}

func (hs *HTTPServer) handleClaimJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req ClaimRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	job, err := hs.store.ClaimJob(r.Context(), req.Queue, req.WorkerID)
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}

	writeJSON(w, http.StatusOK, job)
}

// CompleteRequest represents a job completion request
type CompleteRequest struct {
	Queue  string `json:"queue"`
	JobID  string `json:"job_id"`
	Result string `json:"result"`
}

func (hs *HTTPServer) handleCompleteJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req CompleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	if err := hs.store.CompleteJob(r.Context(), req.Queue, req.JobID, []byte(req.Result)); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// FailRequest represents a job failure request
type FailRequest struct {
	Queue string `json:"queue"`
	JobID string `json:"job_id"`
	Error string `json:"error"`
}

func (hs *HTTPServer) handleFailJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req FailRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	if err := hs.store.FailJob(r.Context(), req.Queue, req.JobID, req.Error); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (hs *HTTPServer) handleJobStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	queueName := r.URL.Query().Get("queue")
	jobID := r.URL.Query().Get("job_id")
	if queueName == "" || jobID == "" {
		http.Error(w, "Missing queue or job_id", http.StatusBadRequest)
		return
	}

	job, err := hs.store.GetJobStatus(r.Context(), queueName, jobID)
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}

	writeJSON(w, http.StatusOK, job)
}

func (hs *HTTPServer) handleQueueStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	queueName := r.URL.Query().Get("queue")
	if queueName == "" {
		http.Error(w, "Missing queue", http.StatusBadRequest)
		return
	}

	stats, err := hs.store.GetQueueStats(r.Context(), queueName)
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}

	writeJSON(w, http.StatusOK, stats)
}

// ===== Sorted Sets =====

// ZAddRequest represents a sorted set add request
type ZAddRequest struct {
	Key    string  `json:"key"`
	Member string  `json:"member"`
	Score  float64 `json:"score"`
}

func (hs *HTTPServer) handleZAdd(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req ZAddRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	if err := hs.store.ZAdd(r.Context(), req.Key, req.Member, req.Score); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// ZMemberRequest identifies a single member of a sorted set
type ZMemberRequest struct {
	Key    string `json:"key"`
	Member string `json:"member"`
}

func (hs *HTTPServer) handleZRem(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req ZMemberRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	if err := hs.store.ZRem(r.Context(), req.Key, req.Member); err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (hs *HTTPServer) handleZScore(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	member := r.URL.Query().Get("member")

	score, exists := hs.store.ZScore(r.Context(), key, member)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"score":  score,
		"exists": exists,
	})
}

func (hs *HTTPServer) handleZRank(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	member := r.URL.Query().Get("member")

	rank, exists := hs.store.ZRank(r.Context(), key, member)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"rank":   rank,
		"exists": exists,
	})
}

func (hs *HTTPServer) handleZRevRank(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	member := r.URL.Query().Get("member")

	rank, exists := hs.store.ZRevRank(r.Context(), key, member)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"rank":   rank,
		"exists": exists,
	})
}

func parseFloatQuery(r *http.Request, name string, fallback float64) float64 {
	v := r.URL.Query().Get(name)
	if v == "" {
		return fallback
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return fallback
	}
	return f
}

func parseIntQuery(r *http.Request, name string, fallback int64) int64 {
	v := r.URL.Query().Get(name)
	if v == "" {
		return fallback
	}
	i, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return fallback
	}
	return i
}

func (hs *HTTPServer) handleZRange(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	min := parseFloatQuery(r, "min", math.Inf(-1))
	max := parseFloatQuery(r, "max", math.Inf(1))
	limit := parseIntQuery(r, "limit", 100)

	members := hs.store.ZRange(r.Context(), key, min, max, limit)
	writeJSON(w, http.StatusOK, map[string]interface{}{"members": members})
}

func (hs *HTTPServer) handleZRevRange(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	max := parseFloatQuery(r, "max", math.Inf(1))
	min := parseFloatQuery(r, "min", math.Inf(-1))
	limit := parseIntQuery(r, "limit", 100)

	members := hs.store.ZRevRange(r.Context(), key, max, min, limit)
	writeJSON(w, http.StatusOK, map[string]interface{}{"members": members})
}

func (hs *HTTPServer) handleZCard(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	card := hs.store.ZCard(r.Context(), key)
	writeJSON(w, http.StatusOK, map[string]interface{}{"card": card})
}

// ===== Streams =====

// XAddRequest represents a stream append request
type XAddRequest struct {
	Stream string            `json:"stream"`
	Fields map[string]string `json:"fields"`
}

func (hs *HTTPServer) handleXAdd(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req XAddRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	entry, err := hs.store.XAdd(r.Context(), req.Stream, req.Fields)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, http.StatusOK, entry)
}

func (hs *HTTPServer) handleXRange(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	streamName := r.URL.Query().Get("stream")
	start := r.URL.Query().Get("start")
	if start == "" {
		start = "0"
	}
	end := r.URL.Query().Get("end")
	if end == "" {
		end = "-"
	}
	limit := parseIntQuery(r, "limit", 100)

	entries := hs.store.XRange(r.Context(), streamName, start, end, limit)
	writeJSON(w, http.StatusOK, map[string]interface{}{"entries": entries})
}

func (hs *HTTPServer) handleXLen(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	streamName := r.URL.Query().Get("stream")
	length := hs.store.XLen(r.Context(), streamName)
	writeJSON(w, http.StatusOK, map[string]interface{}{"length": length})
}

// XGroupCreateRequest represents a consumer group creation request
type XGroupCreateRequest struct {
	Stream string `json:"stream"`
	Group  string `json:"group"`
}

func (hs *HTTPServer) handleXGroupCreate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req XGroupCreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	if err := hs.store.XGroupCreate(r.Context(), req.Stream, req.Group); err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// XReadGroupRequest represents a consumer group read request
type XReadGroupRequest struct {
	Stream   string `json:"stream"`
	Group    string `json:"group"`
	Consumer string `json:"consumer"`
	Limit    int64  `json:"limit"`
}

func (hs *HTTPServer) handleXReadGroup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req XReadGroupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	if req.Limit <= 0 {
		req.Limit = 100
	}

	entries, err := hs.store.XReadGroup(r.Context(), req.Stream, req.Group, req.Consumer, req.Limit)
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{"entries": entries})
}

// XAckRequest represents a consumer group acknowledgement request
type XAckRequest struct {
	Stream   string `json:"stream"`
	Group    string `json:"group"`
	Consumer string `json:"consumer"`
	ID       string `json:"id"`
}

func (hs *HTTPServer) handleXAck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req XAckRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	if err := hs.store.XAck(r.Context(), req.Stream, req.Group, req.Consumer, req.ID); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}
