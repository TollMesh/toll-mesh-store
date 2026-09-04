package api

import (
	"encoding/json"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/toll-mesh/store/coordination"
	"github.com/toll-mesh/store/core"
	"github.com/toll-mesh/store/ranking"
	"github.com/toll-mesh/store/scripting"
	"github.com/toll-mesh/store/search"
	"github.com/toll-mesh/store/transactions"
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

	// Gossip replication transport (node-to-node, not part of the SDK-facing API)
	hs.mux.HandleFunc("/internal/state", hs.handleInternalState)
	hs.mux.HandleFunc("/internal/peers/join", hs.handleInternalPeersJoin)

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

	// Pub/Sub
	hs.mux.HandleFunc("/pubsub/subscribe", hs.handleSubscribe)
	hs.mux.HandleFunc("/pubsub/unsubscribe", hs.handleUnsubscribe)
	hs.mux.HandleFunc("/pubsub/publish", hs.handlePublish)
	hs.mux.HandleFunc("/pubsub/poll", hs.handlePoll)
	hs.mux.HandleFunc("/pubsub/topics", hs.handleGetTopics)
	hs.mux.HandleFunc("/pubsub/subscribers", hs.handleGetSubscribers)
	hs.mux.HandleFunc("/pubsub/stats", hs.handlePubSubStats)

	// Transactions
	hs.mux.HandleFunc("/txn/begin", hs.handleBeginTxn)
	hs.mux.HandleFunc("/txn/operation", hs.handleAddTxnOperation)
	hs.mux.HandleFunc("/txn/commit", hs.handleCommitTxn)
	hs.mux.HandleFunc("/txn/rollback", hs.handleRollbackTxn)
	hs.mux.HandleFunc("/txn/status", hs.handleTxnStatus)

	// Persistence
	hs.mux.HandleFunc("/persistence/snapshot", hs.handleCreateSnapshot)
	hs.mux.HandleFunc("/persistence/snapshot/latest", hs.handleGetLatestSnapshot)
	hs.mux.HandleFunc("/persistence/restore", hs.handleRestoreSnapshot)
	hs.mux.HandleFunc("/persistence/stats", hs.handlePersistenceStats)

	// Scripting (Pipelines)
	hs.mux.HandleFunc("/pipeline/register", hs.handleRegisterPipeline)
	hs.mux.HandleFunc("/pipeline/execute", hs.handleExecutePipeline)
	hs.mux.HandleFunc("/pipeline/execute-inline", hs.handleExecuteInlinePipeline)
	hs.mux.HandleFunc("/pipeline/get", hs.handleGetPipeline)
	hs.mux.HandleFunc("/pipeline/list", hs.handleListPipelines)
	hs.mux.HandleFunc("/pipeline/delete", hs.handleDeletePipeline)

	// Scripting (WASM -- real arbitrary Go code via TinyGo + wazero)
	hs.mux.HandleFunc("/script/compile", hs.handleCompileScript)
	hs.mux.HandleFunc("/script/execute", hs.handleExecuteScript)
	hs.mux.HandleFunc("/script/execute-inline", hs.handleExecuteInlineScript)
	hs.mux.HandleFunc("/script/get", hs.handleGetScript)
	hs.mux.HandleFunc("/script/list", hs.handleListScripts)
	hs.mux.HandleFunc("/script/delete", hs.handleDeleteScript)

	// Search
	hs.mux.HandleFunc("/search/index", hs.handleIndexDocument)
	hs.mux.HandleFunc("/search/bm25", hs.handleSearchBM25)
	hs.mux.HandleFunc("/search/vector", hs.handleSearchVector)
	hs.mux.HandleFunc("/search/hybrid", hs.handleSearchHybrid)
	hs.mux.HandleFunc("/search/delete", hs.handleDeleteSearchDocument)

	// Ranking
	hs.mux.HandleFunc("/rank", hs.handleRank)

	// Metrics
	hs.mux.HandleFunc("/metrics", hs.handleMetrics)
	hs.mux.HandleFunc("/metrics/prometheus", hs.handlePrometheusMetrics)

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

// handleInternalState serves this node's replicated CRDT state (rate
// limiters, replay protection, cache) for a peer's gossip round to fetch
// and merge. Not part of the SDK-facing API -- see MeshStore.GetState.
func (hs *HTTPServer) handleInternalState(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(hs.store.GetState())
}

// PeerJoinRequest is how a node announces itself to another node's cluster.
type PeerJoinRequest struct {
	// Address/Port are the joining node's own HTTP API address -- the same
	// address gossip will later fetch /internal/state from.
	Address string `json:"address"`
	Port    int    `json:"port"`
}

// handleInternalPeersJoin registers the caller as a peer and returns this
// node's current peer list, so a newly-joining node learns about the rest
// of the cluster in one request instead of needing every existing member
// to reach out to it individually.
func (hs *HTTPServer) handleInternalPeersJoin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req PeerJoinRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Address == "" || req.Port == 0 {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	peerID := req.Address + ":" + strconv.Itoa(req.Port)
	if err := hs.coordinator.AddPeer(&core.Node{ID: peerID, Address: req.Address, Port: req.Port}); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	peers := hs.coordinator.GetPeers()
	peerList := make([]map[string]interface{}, 0, len(peers))
	for _, peer := range peers {
		if peer.ID == peerID {
			continue
		}
		peerList = append(peerList, map[string]interface{}{
			"id":      peer.ID,
			"address": peer.Address,
			"port":    peer.Port,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"peers": peerList})
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

// ===== Pub/Sub =====

// SubscribeRequest represents a pub/sub subscription request
type SubscribeRequest struct {
	SubscriberID string `json:"subscriber_id"`
	Topic        string `json:"topic"`
	Pattern      string `json:"pattern,omitempty"`
}

func (hs *HTTPServer) handleSubscribe(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req SubscribeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	if err := hs.store.Subscribe(r.Context(), req.SubscriberID, req.Topic, req.Pattern); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// UnsubscribeRequest represents a pub/sub unsubscribe request
type UnsubscribeRequest struct {
	SubscriberID string `json:"subscriber_id"`
	Topic        string `json:"topic"`
}

func (hs *HTTPServer) handleUnsubscribe(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req UnsubscribeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	if err := hs.store.Unsubscribe(r.Context(), req.SubscriberID, req.Topic); err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// PublishRequest represents a pub/sub publish request
type PublishRequest struct {
	Topic     string `json:"topic"`
	Publisher string `json:"publisher"`
	Payload   string `json:"payload"`
}

func (hs *HTTPServer) handlePublish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req PublishRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	count, err := hs.store.Publish(r.Context(), req.Topic, req.Publisher, []byte(req.Payload))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"delivered_count": count})
}

// PollRequest represents a pub/sub poll request
type PollRequest struct {
	SubscriberID string `json:"subscriber_id"`
	Limit        int    `json:"limit"`
	TimeoutMs    int64  `json:"timeout_ms"`
}

func (hs *HTTPServer) handlePoll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req PollRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	if req.Limit <= 0 {
		req.Limit = 10
	}
	timeout := time.Duration(req.TimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	messages, err := hs.store.PollMessages(r.Context(), req.SubscriberID, req.Limit, timeout)
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"messages": messages})
}

func (hs *HTTPServer) handleGetTopics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"topics": hs.store.GetTopics(r.Context())})
}

func (hs *HTTPServer) handleGetSubscribers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	topic := r.URL.Query().Get("topic")
	writeJSON(w, http.StatusOK, map[string]interface{}{"subscribers": hs.store.GetTopicSubscribers(r.Context(), topic)})
}

func (hs *HTTPServer) handlePubSubStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, hs.store.GetPubSubStats(r.Context()))
}

// ===== Transactions =====

// BeginTxnRequest represents a transaction-begin request
type BeginTxnRequest struct {
	TxnID string `json:"txn_id"`
}

func (hs *HTTPServer) handleBeginTxn(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req BeginTxnRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	txn, err := hs.store.BeginTransaction(r.Context(), req.TxnID)
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, txn)
}

// TxnOperationRequest represents an operation to queue within a transaction
type TxnOperationRequest struct {
	TxnID     string `json:"txn_id"`
	Type      string `json:"type"`
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
	Value     string `json:"value,omitempty"`
}

func (hs *HTTPServer) handleAddTxnOperation(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req TxnOperationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	op := transactions.Operation{
		Type:      transactions.OperationType(req.Type),
		Namespace: req.Namespace,
		Key:       req.Key,
		Value:     req.Value,
	}
	if err := hs.store.AddTransactionOperation(r.Context(), req.TxnID, op); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (hs *HTTPServer) handleCommitTxn(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req BeginTxnRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	if err := hs.store.CommitTransaction(r.Context(), req.TxnID); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (hs *HTTPServer) handleRollbackTxn(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req BeginTxnRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	if err := hs.store.RollbackTransaction(r.Context(), req.TxnID); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (hs *HTTPServer) handleTxnStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	txnID := r.URL.Query().Get("txn_id")
	status, err := hs.store.GetTransactionStatus(r.Context(), txnID)
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"status": status})
}

// ===== Persistence =====

func (hs *HTTPServer) handleCreateSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := hs.store.CreateSnapshot(r.Context()); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (hs *HTTPServer) handleGetLatestSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	snap, err := hs.store.GetLatestSnapshot(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if snap == nil {
		writeJSON(w, http.StatusNotFound, ErrorResponse{Error: "no snapshot available"})
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

func (hs *HTTPServer) handleRestoreSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := hs.store.RestoreFromLatestSnapshot(r.Context()); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (hs *HTTPServer) handlePersistenceStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, hs.store.GetPersistenceStats(r.Context()))
}

// ===== Scripting (Pipelines) =====

// RegisterPipelineRequest represents a pipeline registration request
type RegisterPipelineRequest struct {
	Name  string           `json:"name"`
	Steps []scripting.Step `json:"steps"`
}

func (hs *HTTPServer) handleRegisterPipeline(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req RegisterPipelineRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	p := &scripting.Pipeline{Name: req.Name, Steps: req.Steps}
	if err := hs.store.RegisterPipeline(r.Context(), p); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// ExecutePipelineRequest represents a named-pipeline execution request
type ExecutePipelineRequest struct {
	Name string `json:"name"`
}

func (hs *HTTPServer) handleExecutePipeline(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req ExecutePipelineRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	result, err := hs.store.ExecutePipeline(r.Context(), req.Name)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, result)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// ExecuteInlinePipelineRequest represents an ad-hoc pipeline execution request
type ExecuteInlinePipelineRequest struct {
	Steps []scripting.Step `json:"steps"`
}

func (hs *HTTPServer) handleExecuteInlinePipeline(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req ExecuteInlinePipelineRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	result, err := hs.store.ExecuteInlinePipeline(r.Context(), req.Steps)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, result)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (hs *HTTPServer) handleGetPipeline(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	name := r.URL.Query().Get("name")
	p, err := hs.store.GetPipeline(r.Context(), name)
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, p)
}

func (hs *HTTPServer) handleListPipelines(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"pipelines": hs.store.ListPipelines(r.Context())})
}

func (hs *HTTPServer) handleDeletePipeline(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req ExecutePipelineRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	if err := hs.store.DeletePipeline(r.Context(), req.Name); err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// ===== Scripting (WASM) =====

// CompileScriptRequest represents a WASM script compile request
type CompileScriptRequest struct {
	Name   string `json:"name"`
	Source string `json:"source"`
}

func (hs *HTTPServer) handleCompileScript(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req CompileScriptRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	script, err := hs.store.CompileScript(r.Context(), req.Name, req.Source)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, script)
}

// ExecuteScriptRequest represents a registered-script execution request
type ExecuteScriptRequest struct {
	Name  string `json:"name"`
	Input string `json:"input"`
}

func (hs *HTTPServer) handleExecuteScript(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req ExecuteScriptRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	output, err := hs.store.ExecuteScript(r.Context(), req.Name, req.Input)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"error": err.Error(), "output": output})
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"output": output})
}

// ExecuteInlineScriptRequest represents an ad-hoc script execution request
type ExecuteInlineScriptRequest struct {
	Source string `json:"source"`
	Input  string `json:"input"`
}

func (hs *HTTPServer) handleExecuteInlineScript(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req ExecuteInlineScriptRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	output, err := hs.store.ExecuteInlineScript(r.Context(), req.Source, req.Input)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"error": err.Error(), "output": output})
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"output": output})
}

func (hs *HTTPServer) handleGetScript(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	name := r.URL.Query().Get("name")
	script, err := hs.store.GetScript(r.Context(), name)
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, script)
}

func (hs *HTTPServer) handleListScripts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"scripts": hs.store.ListScripts(r.Context())})
}

func (hs *HTTPServer) handleDeleteScript(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req ExecuteScriptRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	if err := hs.store.DeleteScript(r.Context(), req.Name); err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// ===== Search =====

// IndexDocumentRequest represents a search-index request
type IndexDocumentRequest struct {
	ID       string                 `json:"id"`
	Content  string                 `json:"content"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
	Vector   []float32              `json:"vector,omitempty"`
}

func (hs *HTTPServer) handleIndexDocument(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req IndexDocumentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	doc := &search.Document{ID: req.ID, Content: req.Content, Metadata: req.Metadata, Vector: req.Vector}
	if err := hs.store.IndexDocument(r.Context(), doc); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (hs *HTTPServer) handleSearchBM25(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	query := r.URL.Query().Get("query")
	topK := int(parseIntQuery(r, "topk", 10))
	writeJSON(w, http.StatusOK, map[string]interface{}{"results": hs.store.SearchBM25(r.Context(), query, topK)})
}

// SearchVectorRequest represents a vector-search request
type SearchVectorRequest struct {
	Vector []float32 `json:"vector"`
	TopK   int       `json:"topk"`
}

func (hs *HTTPServer) handleSearchVector(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req SearchVectorRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	if req.TopK <= 0 {
		req.TopK = 10
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"results": hs.store.SearchVector(r.Context(), req.Vector, req.TopK)})
}

// SearchHybridRequest represents a hybrid-search request
type SearchHybridRequest struct {
	Query  string    `json:"query"`
	Vector []float32 `json:"vector"`
	TopK   int       `json:"topk"`
}

func (hs *HTTPServer) handleSearchHybrid(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req SearchHybridRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	if req.TopK <= 0 {
		req.TopK = 10
	}
	results := hs.store.SearchHybrid(r.Context(), req.Query, req.Vector, req.TopK)
	writeJSON(w, http.StatusOK, map[string]interface{}{"results": results})
}

// DeleteSearchDocumentRequest represents a search-index delete request
type DeleteSearchDocumentRequest struct {
	ID string `json:"id"`
}

func (hs *HTTPServer) handleDeleteSearchDocument(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req DeleteSearchDocumentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	if err := hs.store.DeleteSearchDocument(r.Context(), req.ID); err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// ===== Ranking =====

// RankRequest represents a ranking request
type RankRequest struct {
	Items    []ranking.RankedItem `json:"items"`
	Strategy string               `json:"strategy"`
	Boosts   map[string]float32   `json:"boosts,omitempty"`
}

func (hs *HTTPServer) handleRank(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req RankRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	result := hs.store.Rank(r.Context(), req.Items, req.Strategy, req.Boosts)
	writeJSON(w, http.StatusOK, map[string]interface{}{"items": result})
}

// ===== Metrics =====

func (hs *HTTPServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, hs.store.GetMetrics(r.Context()))
}

func (hs *HTTPServer) handlePrometheusMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	w.Write([]byte(hs.store.GetPrometheusMetrics(r.Context())))
}
