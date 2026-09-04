// Package api (this file): HTTP round-trip benchmarks over a real loopback
// TCP connection (httptest.NewServer), not in-process handler calls.
//
// Run each benchmark with a bounded iteration count rather than a time
// budget, e.g.:
//
//	go test ./api/... -bench BenchmarkHTTPConsume$ -benchtime=3000x -run '^$'
//
// A time-based -benchtime=2s (the default-ish choice) can generate enough
// short-lived loopback connections in a couple of seconds to exhaust
// macOS's default ephemeral port range (49152-65535, ~16k ports) before
// TIME_WAIT sockets from earlier requests have expired, failing later
// requests with "connect: can't assign requested address". That's a local
// OS/network-stack artifact of hammering loopback this hard, not a bug in
// the server -- but it means benchmark runs need a bounded request count,
// and back-to-back benchmark functions in the same run may need a brief
// pause between them for the port range to recover.
package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/toll-mesh/store/coordination"
	"github.com/toll-mesh/store/core"
	"github.com/toll-mesh/store/store"
)

// drainAndClose fully reads a response body before closing it. Go's
// http.Client can only return a connection to its keep-alive pool once the
// body has been read to EOF; closing without draining discards the
// connection instead, forcing a brand new TCP connection (and ephemeral
// port) per request -- which exhausts the local port range and starts
// failing with "can't assign requested address" within a couple of
// seconds at any real request rate. This is not specific to these
// benchmarks; it's the standard fix for this well-known Go HTTP client
// pitfall.
func drainAndClose(resp *http.Response) {
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}

// newBenchServer starts a real TCP listener (httptest.NewServer, not
// mux.ServeHTTP in-process) so these benchmarks measure an actual HTTP
// round trip -- the thing docs/vs-redis.md's "every write goes through an
// HTTP round-trip" claim is about -- not just handler-function call
// overhead.
func newBenchServer(b *testing.B) (*httptest.Server, *http.Client) {
	b.Helper()
	config := &core.ClusterConfig{NodeName: "bench-node", DataDir: b.TempDir()}
	ms, err := store.NewMeshStore(config)
	if err != nil {
		b.Fatalf("NewMeshStore failed: %v", err)
	}
	b.Cleanup(func() { ms.Close() })

	coordinator := coordination.NewGossipCoordinator(config, 0)
	hs := NewHTTPServer(":0", ms, coordinator)
	server := httptest.NewServer(hs.mux)
	b.Cleanup(server.Close)

	// MaxIdleConnsPerHost defaults to 2, which starves connection reuse
	// under any real concurrency and forces a fresh TCP connection (and
	// ephemeral port) per request once more than 2 requests are in flight
	// -- the same port-exhaustion failure draining the body alone doesn't
	// fix once parallelism is added.
	transport := &http.Transport{MaxIdleConnsPerHost: 200}
	client := &http.Client{Timeout: 5 * time.Second, Transport: transport}
	b.Cleanup(transport.CloseIdleConnections)
	return server, client
}

// BenchmarkHTTPConsume measures a full real HTTP round trip (loopback TCP,
// JSON marshal/unmarshal, handler dispatch, GCounter increment) for
// /consume -- the rate-limiting endpoint, one of the simplest writes in
// the system, so this is close to a floor rather than a typical-case
// number for anything with more work per request.
func BenchmarkHTTPConsume(b *testing.B) {
	server, client := newBenchServer(b)
	body, _ := json.Marshal(ConsumeRequest{Key: "bench-key", Limit: 1 << 30, Window: 60000})

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		resp, err := client.Post(server.URL+"/consume", "application/json", bytes.NewReader(body))
		if err != nil {
			b.Fatalf("request failed: %v", err)
		}
		drainAndClose(resp)
	}
}

// BenchmarkHTTPCacheSetGet measures a real HTTP round trip for the two
// cache operations back to back (set then get), representative of a
// cache-aside read-through pattern.
func BenchmarkHTTPCacheSetGet(b *testing.B) {
	server, client := newBenchServer(b)
	setBody, _ := json.Marshal(CacheRequest{Namespace: "bench", Key: "k", Value: "some-representative-value-payload"})

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		resp, err := client.Post(server.URL+"/cache/set", "application/json", bytes.NewReader(setBody))
		if err != nil {
			b.Fatalf("set request failed: %v", err)
		}
		drainAndClose(resp)

		resp, err = client.Get(server.URL + "/cache/get?namespace=bench&key=k")
		if err != nil {
			b.Fatalf("get request failed: %v", err)
		}
		drainAndClose(resp)
	}
}

// BenchmarkHTTPConsumeParallel measures /consume throughput under
// concurrent clients, since a real deployment serves many requests at
// once, not one at a time.
func BenchmarkHTTPConsumeParallel(b *testing.B) {
	server, client := newBenchServer(b)
	body, _ := json.Marshal(ConsumeRequest{Key: "bench-key", Limit: 1 << 30, Window: 60000})

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp, err := client.Post(server.URL+"/consume", "application/json", bytes.NewReader(body))
			if err != nil {
				b.Fatalf("request failed: %v", err)
			}
			drainAndClose(resp)
		}
	})
}
