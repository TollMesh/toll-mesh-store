package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/toll-mesh/store/coordination"
	"github.com/toll-mesh/store/core"
	"github.com/toll-mesh/store/store"
)

// newAuthTestServer is like newTestServer (features_test.go) but with a
// real TCP listener (httptest.NewServer, not mux.ServeHTTP in-process) and
// configurable apiKey/clusterSecret, since authMiddleware wraps the whole
// server, not the mux directly.
func newAuthTestServer(t *testing.T, apiKey, clusterSecret string) *httptest.Server {
	t.Helper()
	config := &core.ClusterConfig{NodeName: "node1", DataDir: t.TempDir()}
	ms, err := store.NewMeshStore(config)
	if err != nil {
		t.Fatalf("NewMeshStore failed: %v", err)
	}
	t.Cleanup(func() { ms.Close() })

	coordinator := coordination.NewGossipCoordinator(config, 0)
	hs := NewHTTPServer(":0", ms, coordinator, apiKey, clusterSecret)
	server := httptest.NewServer(hs.authMiddleware(hs.mux))
	t.Cleanup(server.Close)
	return server
}

// TestAPIKeyAuth is the regression test for a real, severe gap: every SDK
// has sent an X-API-Key header since it was written, but nothing on the
// server ever checked it -- every request succeeded regardless of whether
// a key was sent or correct, making the "api_key" config option in all 7
// SDKs entirely decorative. Confirmed via a repo-wide grep (zero matches
// for any API-key check anywhere in api/http.go) before writing this fix.
func TestAPIKeyAuth(t *testing.T) {
	server := newAuthTestServer(t, "secret-key", "")

	get := func(headerValue string) int {
		req, _ := http.NewRequest(http.MethodGet, server.URL+"/health", nil)
		if headerValue != "" {
			req.Header.Set("X-API-Key", headerValue)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}

	// /health must stay open even with an API key configured, for basic
	// monitoring/load-balancer checks that can't be expected to know a
	// secret.
	if code := get(""); code != http.StatusOK {
		t.Errorf("/health with no key = %d, want 200 (health should stay unauthenticated)", code)
	}

	// A protected endpoint, on the other hand, must reject a missing or
	// wrong key and accept the correct one.
	checkProtected := func(path string, headerValue string, wantCode int) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodGet, server.URL+path, nil)
		if headerValue != "" {
			req.Header.Set("X-API-Key", headerValue)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request to %s failed: %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != wantCode {
			t.Errorf("%s with key=%q = %d, want %d", path, headerValue, resp.StatusCode, wantCode)
		}
	}

	checkProtected("/cache/get?namespace=ns&key=k", "", http.StatusUnauthorized)
	checkProtected("/cache/get?namespace=ns&key=k", "wrong-key", http.StatusUnauthorized)
	checkProtected("/cache/get?namespace=ns&key=k", "secret-key", http.StatusOK)
}

// TestNoAuthWhenNotConfigured confirms the zero-config default (apiKey ==
// "") stays fully open, matching every other test in this package that
// constructs a server without an API key.
func TestNoAuthWhenNotConfigured(t *testing.T) {
	server := newAuthTestServer(t, "", "")

	resp, err := http.Get(server.URL + "/cache/get?namespace=ns&key=k")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("with no apiKey configured, got %d, want 200 (should be open)", resp.StatusCode)
	}
}

// TestClusterSecretProtectsInternalEndpoints is the regression test for
// the /internal/* gossip endpoints added this session, which had no
// protection at all: any reachable client could read a node's full
// replicated state via GET /internal/state, or insert itself into the
// cluster via POST /internal/peers/join.
func TestClusterSecretProtectsInternalEndpoints(t *testing.T) {
	server := newAuthTestServer(t, "", "cluster-secret")

	check := func(headerValue string, wantCode int) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodGet, server.URL+"/internal/state", nil)
		if headerValue != "" {
			req.Header.Set("X-Cluster-Secret", headerValue)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != wantCode {
			t.Errorf("/internal/state with cluster-secret=%q = %d, want %d", headerValue, resp.StatusCode, wantCode)
		}
	}

	check("", http.StatusUnauthorized)
	check("wrong-secret", http.StatusUnauthorized)
	check("cluster-secret", http.StatusOK)

	// A regular API key (even correct, if one were configured) must not
	// substitute for the cluster secret on /internal/* -- these are
	// deliberately separate credentials for separate trust boundaries
	// (SDK client vs. cluster peer).
	req, _ := http.NewRequest(http.MethodGet, server.URL+"/internal/state", nil)
	req.Header.Set("X-API-Key", "cluster-secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("/internal/state with only X-API-Key set (no X-Cluster-Secret) = %d, want 401", resp.StatusCode)
	}
}
