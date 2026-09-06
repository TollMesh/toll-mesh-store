// Command loadtest drives a mixed, concurrent, sustained workload against
// a real, separately-running tollmeshcache server process over real HTTP
// connections -- unlike api/http_bench_test.go's Go benchmarks, which
// exercise a single endpoint at a time via httptest.NewServer in the same
// test binary. This is meant to catch what single-endpoint,
// in-process benchmarks structurally can't: cross-feature contention,
// sustained-duration resource leaks (goroutines, file descriptors,
// connections), and behavior against a real OS process boundary.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type result struct {
	op       string
	err      bool
	duration time.Duration
}

func main() {
	addr := flag.String("addr", "http://127.0.0.1:8080", "base URL of a running tollmeshcache server")
	apiKey := flag.String("api-key", "", "X-API-Key header value, if the server requires one")
	duration := flag.Duration("duration", 20*time.Second, "how long to run the mixed workload")
	concurrency := flag.Int("concurrency", 50, "number of concurrent worker goroutines")
	flag.Parse()

	client := &http.Client{
		Timeout:   5 * time.Second,
		Transport: &http.Transport{MaxIdleConnsPerHost: *concurrency * 2},
	}

	fmt.Printf("loadtest: %s, %d workers, %s duration\n", *addr, *concurrency, *duration)

	var initialGoroutines = runtime.NumGoroutine()

	resultsCh := make(chan result, 10000)
	var wg sync.WaitGroup
	var requestCount int64

	stop := make(chan struct{})
	time.AfterFunc(*duration, func() { close(stop) })

	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(workerID) + time.Now().UnixNano()))
			for {
				select {
				case <-stop:
					return
				default:
				}
				op, err, d := runOneOp(client, *addr, *apiKey, workerID, rng)
				resultsCh <- result{op: op, err: err, duration: d}
				atomic.AddInt64(&requestCount, 1)
			}
		}(w)
	}

	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	byOp := make(map[string][]time.Duration)
	errsByOp := make(map[string]int)
	var totalErrs int
	for r := range resultsCh {
		byOp[r.op] = append(byOp[r.op], r.duration)
		if r.err {
			errsByOp[r.op]++
			totalErrs++
		}
	}

	finalGoroutines := runtime.NumGoroutine()

	total := atomic.LoadInt64(&requestCount)
	fmt.Printf("\n=== Results ===\n")
	fmt.Printf("total requests: %d (%.0f req/s)\n", total, float64(total)/duration.Seconds())
	fmt.Printf("total errors:   %d (%.2f%%)\n", totalErrs, 100*float64(totalErrs)/float64(total))
	fmt.Printf("loadtest's own goroutines: %d -> %d (informational only; this process, not the server)\n\n", initialGoroutines, finalGoroutines)

	ops := make([]string, 0, len(byOp))
	for op := range byOp {
		ops = append(ops, op)
	}
	sort.Strings(ops)

	fmt.Printf("%-12s %8s %10s %10s %10s %10s\n", "op", "count", "errors", "p50", "p95", "p99")
	for _, op := range ops {
		durations := byOp[op]
		sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
		p50 := percentile(durations, 50)
		p95 := percentile(durations, 95)
		p99 := percentile(durations, 99)
		fmt.Printf("%-12s %8d %10d %10s %10s %10s\n", op, len(durations), errsByOp[op], p50, p95, p99)
	}

	if totalErrs > 0 {
		os.Exit(1)
	}
}

func percentile(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := (len(sorted) * p) / 100
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// runOneOp picks one operation from a fixed, realistic mix and executes
// it against the live server, returning the operation name, whether it
// failed, and how long it took.
func runOneOp(client *http.Client, addr, apiKey string, workerID int, rng *rand.Rand) (string, bool, time.Duration) {
	key := fmt.Sprintf("loadtest-key-%d-%d", workerID, rng.Intn(200))

	// Weighted so cache reads/writes and rate limiting (the two
	// heaviest-used primitives in practice) dominate, but every major
	// feature added this session gets real, continuous traffic too.
	var op string
	var call func() bool
	roll := rng.Intn(100)
	switch {
	case roll < 30:
		op = "consume"
		call = func() bool {
			return doPost(client, addr, apiKey, "/consume", map[string]interface{}{
				"key": key, "limit": 1000000, "window": 60000,
			})
		}
	case roll < 55:
		op = "cache_set"
		call = func() bool {
			return doPost(client, addr, apiKey, "/cache/set", map[string]interface{}{
				"namespace": "loadtest", "key": key, "value": "some-representative-payload", "ttl": 3600000,
			})
		}
	case roll < 75:
		op = "cache_get"
		call = func() bool {
			return doGet(client, addr, apiKey, fmt.Sprintf("/cache/get?namespace=loadtest&key=%s", key))
		}
	case roll < 85:
		op = "zadd"
		call = func() bool {
			return doPost(client, addr, apiKey, "/zset/add", map[string]interface{}{
				"key": "loadtest-zset", "member": key, "score": rng.Float64() * 1000,
			})
		}
	case roll < 92:
		op = "enqueue"
		call = func() bool {
			return doPost(client, addr, apiKey, "/queue/enqueue", map[string]interface{}{
				"queue": "loadtest-queue", "payload": "cGF5bG9hZA==", "priority": 5, "max_retries": 3, "deadline": 3600000,
			})
		}
	case roll < 97:
		op = "publish"
		call = func() bool {
			return doPost(client, addr, apiKey, "/pubsub/publish", map[string]interface{}{
				"topic": "loadtest-topic", "publisher": "loadtest", "payload": "hello",
			})
		}
	default:
		op = "metrics"
		call = func() bool { return doGet(client, addr, apiKey, "/metrics") }
	}

	start := time.Now()
	ok := call()
	return op, !ok, time.Since(start)
}

func doPost(client *http.Client, addr, apiKey, path string, body map[string]interface{}) bool {
	data, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPost, addr+path, bytes.NewReader(data))
	if err != nil {
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	if apiKey != "" {
		req.Header.Set("X-API-Key", apiKey)
	}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer drainAndClose(resp)
	return resp.StatusCode >= 200 && resp.StatusCode < 300
}

func doGet(client *http.Client, addr, apiKey, path string) bool {
	req, err := http.NewRequest(http.MethodGet, addr+path, nil)
	if err != nil {
		return false
	}
	if apiKey != "" {
		req.Header.Set("X-API-Key", apiKey)
	}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer drainAndClose(resp)
	return resp.StatusCode >= 200 && resp.StatusCode < 300
}

func drainAndClose(resp *http.Response) {
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}
