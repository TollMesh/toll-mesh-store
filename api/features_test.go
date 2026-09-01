package api

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/toll-mesh/store/coordination"
	"github.com/toll-mesh/store/core"
	"github.com/toll-mesh/store/store"
)

func newTestServer(t *testing.T) *HTTPServer {
	t.Helper()
	config := &core.ClusterConfig{
		NodeName: "node1",
		BindAddr: "127.0.0.1",
		BindPort: 9000,
	}
	ms, err := store.NewMeshStore(config)
	if err != nil {
		t.Fatalf("NewMeshStore failed: %v", err)
	}
	t.Cleanup(func() { ms.Close() })

	coordinator := coordination.NewGossipCoordinator(config, 0)
	return NewHTTPServer(":0", ms, coordinator)
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
