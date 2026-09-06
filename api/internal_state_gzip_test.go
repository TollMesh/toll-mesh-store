package api

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/toll-mesh/store/core"
)

// TestInternalStateGzipsWhenClientSupportsIt is the regression test for
// gossip's scalability fix: handleInternalState must compress its
// response when the requester's Accept-Encoding says it can decode gzip
// (which every client in this codebase's transport does automatically via
// Go's net/http.Transport, as long as it never sets its own
// Accept-Encoding header -- true for gossip's client, PeerManager's
// health-check client, and the join-request client). Also verifies a
// requester that does NOT advertise gzip support still gets a plain,
// directly-decodable JSON response -- real content negotiation, not an
// unconditional gzip that would break a plain curl/http.Get caller.
func TestInternalStateGzipsWhenClientSupportsIt(t *testing.T) {
	hs := newTestServer(t)
	server := httptest.NewServer(hs.mux)
	defer server.Close()

	// Populate enough state that compression has something real to do.
	ctx := context.Background()
	for i := 0; i < 200; i++ {
		if err := hs.store.Set(ctx, "ns", fmt.Sprintf("key-%d", i), []byte("some-representative-cache-value-payload-of-moderate-length-for-compression"), time.Hour); err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	// A client that advertises gzip support (mirroring what Go's
	// transport does automatically) must get a compressed, correctly
	// labeled response.
	req, _ := http.NewRequest(http.MethodGet, server.URL+"/internal/state", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.Header.Get("Content-Encoding") != "gzip" {
		t.Fatalf("expected Content-Encoding: gzip, got %q", resp.Header.Get("Content-Encoding"))
	}

	compressedBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading compressed body failed: %v", err)
	}

	gz, err := gzip.NewReader(bytes.NewReader(compressedBody))
	if err != nil {
		t.Fatalf("response was not valid gzip: %v", err)
	}
	decompressed, err := io.ReadAll(gz)
	if err != nil {
		t.Fatalf("decompressing response failed: %v", err)
	}

	var state core.MeshStoreState
	if err := json.Unmarshal(decompressed, &state); err != nil {
		t.Fatalf("decompressed body was not valid MeshStoreState JSON: %v", err)
	}
	if len(state.Cache["ns"]) != 200 {
		t.Fatalf("expected 200 cache entries in decompressed state, got %d", len(state.Cache["ns"]))
	}

	if len(compressedBody) >= len(decompressed) {
		t.Errorf("compressed body (%d bytes) was not smaller than decompressed (%d bytes)", len(compressedBody), len(decompressed))
	}

	// A client that does NOT advertise gzip support must get a plain,
	// directly-decodable response -- no unconditional compression.
	plainResp, err := http.Get(server.URL + "/internal/state")
	if err != nil {
		t.Fatalf("plain request failed: %v", err)
	}
	defer plainResp.Body.Close()
	if plainResp.Header.Get("Content-Encoding") == "gzip" {
		t.Error("server gzipped a response for a client that never advertised gzip support")
	}
	var plainState core.MeshStoreState
	if err := json.NewDecoder(plainResp.Body).Decode(&plainState); err != nil {
		t.Fatalf("plain response was not directly-decodable JSON: %v", err)
	}
}
