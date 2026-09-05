package api

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/toll-mesh/store/coordination"
	"github.com/toll-mesh/store/core"
	"github.com/toll-mesh/store/ranking"
	"github.com/toll-mesh/store/scripting"
	"github.com/toll-mesh/store/store"
)

func newTestServer(t *testing.T) *HTTPServer {
	t.Helper()
	config := &core.ClusterConfig{
		NodeName: "node1",
		BindAddr: "127.0.0.1",
		BindPort: 9000,
		DataDir:  t.TempDir(),
	}
	ms, err := store.NewMeshStore(config)
	if err != nil {
		t.Fatalf("NewMeshStore failed: %v", err)
	}
	t.Cleanup(func() { ms.Close() })

	coordinator := coordination.NewGossipCoordinator(config, 0)
	return NewHTTPServer(":0", ms, coordinator, "", "")
}

func postJSON(t *testing.T, hs *HTTPServer, path string, body interface{}, out interface{}) int {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	req := httptest.NewRequest("POST", path, bytes.NewReader(b))
	rec := httptest.NewRecorder()
	hs.mux.ServeHTTP(rec, req)
	if out != nil {
		if err := json.NewDecoder(rec.Body).Decode(out); err != nil {
			t.Fatalf("decode response failed: %v (status=%d)", err, rec.Code)
		}
	}
	return rec.Code
}

func getJSON(t *testing.T, hs *HTTPServer, path string, out interface{}) int {
	t.Helper()
	req := httptest.NewRequest("GET", path, nil)
	rec := httptest.NewRecorder()
	hs.mux.ServeHTTP(rec, req)
	if out != nil {
		if err := json.NewDecoder(rec.Body).Decode(out); err != nil {
			t.Fatalf("decode response failed: %v (status=%d)", err, rec.Code)
		}
	}
	return rec.Code
}

func TestHTTP_JobQueue_EndToEnd(t *testing.T) {
	hs := newTestServer(t)

	var job map[string]interface{}
	status := postJSON(t, hs, "/queue/enqueue", EnqueueRequest{
		Queue: "tasks", Payload: "hello", Priority: 5, MaxRetries: 3,
	}, &job)
	if status != 200 {
		t.Fatalf("enqueue failed: status=%d body=%v", status, job)
	}
	jobID, _ := job["id"].(string)
	if jobID == "" {
		t.Fatalf("expected job id in response, got %v", job)
	}

	var claimed map[string]interface{}
	status = postJSON(t, hs, "/queue/claim", ClaimRequest{Queue: "tasks", WorkerID: "w1"}, &claimed)
	if status != 200 {
		t.Fatalf("claim failed: status=%d body=%v", status, claimed)
	}
	if claimed["id"] != jobID {
		t.Errorf("expected to claim %v, got %v", jobID, claimed["id"])
	}

	var completeResp map[string]bool
	status = postJSON(t, hs, "/queue/complete", CompleteRequest{Queue: "tasks", JobID: jobID, Result: "done"}, &completeResp)
	if status != 200 || !completeResp["ok"] {
		t.Fatalf("complete failed: status=%d body=%v", status, completeResp)
	}

	var stats map[string]interface{}
	status = getJSON(t, hs, "/queue/stats?queue=tasks", &stats)
	if status != 200 {
		t.Fatalf("stats failed: status=%d", status)
	}
	if stats["total_jobs"] != float64(1) {
		t.Errorf("expected 1 total job, got %v", stats["total_jobs"])
	}
}

func TestHTTP_SortedSet_EndToEnd(t *testing.T) {
	hs := newTestServer(t)

	for _, m := range []struct {
		member string
		score  float64
	}{
		{"alice", 100}, {"bob", 200}, {"carol", 50},
	} {
		var resp map[string]bool
		status := postJSON(t, hs, "/zset/add", ZAddRequest{Key: "board", Member: m.member, Score: m.score}, &resp)
		if status != 200 || !resp["ok"] {
			t.Fatalf("zadd failed for %s: status=%d body=%v", m.member, status, resp)
		}
	}

	var rankResp map[string]interface{}
	status := getJSON(t, hs, "/zset/rank?key=board&member=carol", &rankResp)
	if status != 200 || rankResp["rank"] != float64(0) || rankResp["exists"] != true {
		t.Errorf("expected carol rank 0, got status=%d body=%v", status, rankResp)
	}

	var revRangeResp map[string]interface{}
	status = getJSON(t, hs, "/zset/revrange?key=board&max=1000&min=-1000&limit=2", &revRangeResp)
	if status != 200 {
		t.Fatalf("revrange failed: status=%d", status)
	}
	members, ok := revRangeResp["members"].([]interface{})
	if !ok || len(members) != 2 {
		t.Fatalf("expected 2 members, got %v", revRangeResp)
	}
	first := members[0].(map[string]interface{})
	if first["member"] != "bob" {
		t.Errorf("expected top member bob (highest score), got %v", first["member"])
	}

	var cardResp map[string]interface{}
	status = getJSON(t, hs, "/zset/card?key=board", &cardResp)
	if status != 200 || cardResp["card"] != float64(3) {
		t.Errorf("expected card 3, got status=%d body=%v", status, cardResp)
	}
}

func TestHTTP_Stream_EndToEnd(t *testing.T) {
	hs := newTestServer(t)

	var addResp map[string]interface{}
	status := postJSON(t, hs, "/stream/add", XAddRequest{Stream: "events", Fields: map[string]string{"type": "login"}}, &addResp)
	if status != 200 {
		t.Fatalf("xadd failed: status=%d body=%v", status, addResp)
	}
	entryID, _ := addResp["id"].(string)
	if entryID == "" {
		t.Fatalf("expected entry id, got %v", addResp)
	}

	var groupResp map[string]bool
	status = postJSON(t, hs, "/stream/group/create", XGroupCreateRequest{Stream: "events", Group: "analytics"}, &groupResp)
	if status != 200 || !groupResp["ok"] {
		t.Fatalf("group create failed: status=%d body=%v", status, groupResp)
	}

	var readResp map[string]interface{}
	status = postJSON(t, hs, "/stream/group/read", XReadGroupRequest{Stream: "events", Group: "analytics", Consumer: "c1", Limit: 10}, &readResp)
	if status != 200 {
		t.Fatalf("read group failed: status=%d body=%v", status, readResp)
	}
	entries, ok := readResp["entries"].([]interface{})
	if !ok || len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %v", readResp)
	}

	var ackResp map[string]bool
	status = postJSON(t, hs, "/stream/group/ack", XAckRequest{Stream: "events", Group: "analytics", Consumer: "c1", ID: entryID}, &ackResp)
	if status != 200 || !ackResp["ok"] {
		t.Fatalf("ack failed: status=%d body=%v", status, ackResp)
	}

	var lenResp map[string]interface{}
	status = getJSON(t, hs, "/stream/len?stream=events", &lenResp)
	if status != 200 || lenResp["length"] != float64(1) {
		t.Errorf("expected length 1, got status=%d body=%v", status, lenResp)
	}
}

func TestHTTP_PubSub_EndToEnd(t *testing.T) {
	hs := newTestServer(t)

	var subResp map[string]bool
	status := postJSON(t, hs, "/pubsub/subscribe", SubscribeRequest{SubscriberID: "sub-1", Topic: "news"}, &subResp)
	if status != 200 || !subResp["ok"] {
		t.Fatalf("subscribe failed: status=%d body=%v", status, subResp)
	}

	var pubResp map[string]interface{}
	status = postJSON(t, hs, "/pubsub/publish", PublishRequest{Topic: "news", Publisher: "pub-1", Payload: "hello"}, &pubResp)
	if status != 200 || pubResp["delivered_count"] != float64(1) {
		t.Fatalf("publish failed: status=%d body=%v", status, pubResp)
	}

	var pollResp map[string]interface{}
	status = postJSON(t, hs, "/pubsub/poll", PollRequest{SubscriberID: "sub-1", Limit: 10, TimeoutMs: 1000}, &pollResp)
	if status != 200 {
		t.Fatalf("poll failed: status=%d", status)
	}
	messages, ok := pollResp["messages"].([]interface{})
	if !ok || len(messages) != 1 {
		t.Fatalf("expected 1 message, got %v", pollResp)
	}
}

func TestHTTP_Transaction_CommitApplies(t *testing.T) {
	hs := newTestServer(t)

	var beginResp map[string]interface{}
	status := postJSON(t, hs, "/txn/begin", BeginTxnRequest{TxnID: "txn-1"}, &beginResp)
	if status != 200 {
		t.Fatalf("begin failed: status=%d body=%v", status, beginResp)
	}

	var opResp map[string]bool
	status = postJSON(t, hs, "/txn/operation", TxnOperationRequest{
		TxnID: "txn-1", Type: "set", Namespace: "ns", Key: "k", Value: "v",
	}, &opResp)
	if status != 200 || !opResp["ok"] {
		t.Fatalf("add operation failed: status=%d body=%v", status, opResp)
	}

	var commitResp map[string]bool
	status = postJSON(t, hs, "/txn/commit", BeginTxnRequest{TxnID: "txn-1"}, &commitResp)
	if status != 200 || !commitResp["ok"] {
		t.Fatalf("commit failed: status=%d body=%v", status, commitResp)
	}

	var cacheResp map[string]interface{}
	status = getJSON(t, hs, "/cache/get?namespace=ns&key=k", &cacheResp)
	if status != 200 || cacheResp["exists"] != true || cacheResp["value"] != "v" {
		t.Fatalf("expected committed value visible, got status=%d body=%v", status, cacheResp)
	}
}

func TestHTTP_Persistence_SnapshotAndRestore(t *testing.T) {
	hs := newTestServer(t)

	var setResp map[string]bool
	postJSON(t, hs, "/cache/set", CacheRequest{Namespace: "ns", Key: "k", Value: "v"}, &setResp)

	var snapResp map[string]bool
	status := postJSON(t, hs, "/persistence/snapshot", nil, &snapResp)
	if status != 200 || !snapResp["ok"] {
		t.Fatalf("create snapshot failed: status=%d body=%v", status, snapResp)
	}

	var latestResp map[string]interface{}
	status = getJSON(t, hs, "/persistence/snapshot/latest", &latestResp)
	if status != 200 {
		t.Fatalf("get latest snapshot failed: status=%d", status)
	}
}

func TestHTTP_Pipeline_InlineExecution(t *testing.T) {
	hs := newTestServer(t)

	var result map[string]interface{}
	status := postJSON(t, hs, "/pipeline/execute-inline", ExecuteInlinePipelineRequest{
		Steps: []scripting.Step{
			{Op: "set", Args: map[string]interface{}{"namespace": "ns", "key": "k", "value": "hi"}},
		},
	}, &result)
	if status != 200 {
		t.Fatalf("pipeline execution failed: status=%d body=%v", status, result)
	}

	var cacheResp map[string]interface{}
	status = getJSON(t, hs, "/cache/get?namespace=ns&key=k", &cacheResp)
	if status != 200 || cacheResp["value"] != "hi" {
		t.Fatalf("expected pipeline set to be visible, got %v", cacheResp)
	}
}

func TestHTTP_Search_EndToEnd(t *testing.T) {
	hs := newTestServer(t)

	var indexResp map[string]bool
	status := postJSON(t, hs, "/search/index", IndexDocumentRequest{ID: "1", Content: "distributed systems"}, &indexResp)
	if status != 200 || !indexResp["ok"] {
		t.Fatalf("index failed: status=%d body=%v", status, indexResp)
	}

	var searchResp map[string]interface{}
	status = getJSON(t, hs, "/search/bm25?query=distributed&topk=10", &searchResp)
	if status != 200 {
		t.Fatalf("search failed: status=%d", status)
	}
	results, ok := searchResp["results"].([]interface{})
	if !ok || len(results) != 1 {
		t.Fatalf("expected 1 result, got %v", searchResp)
	}
}

func TestHTTP_Rank(t *testing.T) {
	hs := newTestServer(t)

	var result map[string]interface{}
	status := postJSON(t, hs, "/rank", RankRequest{
		Items:    []ranking.RankedItem{{ID: "a", Score: 1}, {ID: "b", Score: 3}},
		Strategy: "bm25",
	}, &result)
	if status != 200 {
		t.Fatalf("rank failed: status=%d body=%v", status, result)
	}
	items, ok := result["items"].([]interface{})
	if !ok || len(items) != 2 {
		t.Fatalf("expected 2 ranked items, got %v", result)
	}
	first := items[0].(map[string]interface{})
	if first["ID"] != "b" {
		t.Errorf("expected 'b' to rank first, got %v", first["ID"])
	}
}

func TestHTTP_Metrics(t *testing.T) {
	hs := newTestServer(t)
	postJSON(t, hs, "/consume", ConsumeRequest{Key: "k", Limit: 10, Window: 60000}, nil)

	var metricsResp map[string]interface{}
	status := getJSON(t, hs, "/metrics", &metricsResp)
	if status != 200 {
		t.Fatalf("metrics failed: status=%d", status)
	}
	if metricsResp["consume_total"] != float64(1) {
		t.Errorf("expected 1 consume recorded, got %v", metricsResp["consume_total"])
	}

	req := httptest.NewRequest("GET", "/metrics/prometheus", nil)
	rec := httptest.NewRecorder()
	hs.mux.ServeHTTP(rec, req)
	if rec.Code != 200 || rec.Body.Len() == 0 {
		t.Errorf("expected non-empty prometheus output, got status=%d len=%d", rec.Code, rec.Body.Len())
	}
}

const httpEchoScript = `
package main

import (
	"bufio"
	"fmt"
	"os"
)

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()
	fmt.Printf("echo: %s\n", scanner.Text())
}
`

func TestHTTP_WasmScript_EndToEnd(t *testing.T) {
	hs := newTestServer(t)

	var compileResp map[string]interface{}
	status := postJSON(t, hs, "/script/compile", CompileScriptRequest{Name: "echo", Source: httpEchoScript}, &compileResp)
	if status != 200 {
		t.Skipf("WASM scripting unavailable (tinygo not installed?): status=%d body=%v", status, compileResp)
	}

	var execResp map[string]interface{}
	status = postJSON(t, hs, "/script/execute", ExecuteScriptRequest{Name: "echo", Input: "hi"}, &execResp)
	if status != 200 {
		t.Fatalf("execute failed: status=%d body=%v", status, execResp)
	}
	if execResp["output"] != "echo: hi\n" {
		t.Errorf("unexpected output: %v", execResp["output"])
	}

	var listResp map[string]interface{}
	status = getJSON(t, hs, "/script/list", &listResp)
	if status != 200 {
		t.Fatalf("list failed: status=%d", status)
	}
	scripts, ok := listResp["scripts"].([]interface{})
	if !ok || len(scripts) != 1 {
		t.Errorf("expected 1 script, got %v", listResp)
	}
}
