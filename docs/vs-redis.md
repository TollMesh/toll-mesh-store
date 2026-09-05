---
layout: default
title: vs Redis
nav_order: 11
---

# TollMeshCache vs Redis

An honest comparison, current as of this project's actual tested state — not aspirational.

---

## What's Actually Verified Working

Fourteen capabilities are wired end-to-end (Go backend → HTTP API → all 7 SDKs) and have been run against a live server as part of building them:

| Feature | TollMeshCache | Redis |
|---------|---------------|-------|
| Rate limiting | Yes (CRDT `GCounter`) | Yes (requires `INCR`+`EXPIRE` or a module) |
| Replay protection | Yes (CRDT `GSet`) | Yes (requires `SETNX`+`EXPIRE`) |
| Key-value cache with TTL | Yes | Yes |
| Job queues | Yes (priority, retry, dead-letter) | Not built in (commonly layered on Streams or Lists) |
| Sorted sets | Yes (skip list, CRDT conflict resolution) | Yes (skip list) |
| Streams with consumer groups | Yes | Yes |
| Pub/Sub | Yes (poll-based delivery, not a persistent connection) | Yes (`PUBLISH`/`SUBSCRIBE` over RESP) |
| Transactions | Yes (queue ops, atomic commit/rollback) | Yes (`MULTI`/`EXEC`, no rollback on runtime errors) |
| Persistence | Yes (WAL + snapshot, checksummed) | Yes (RDB + AOF, 15+ years battle-tested) |
| Scripting | Yes — real arbitrary Go code, compiled by TinyGo to WASI WASM, executed in a sandboxed wazero runtime | Yes (Lua via `EVAL`) |
| Search (BM25 + vector, hybrid) | Yes | No (requires RediSearch module) |
| Ranking | Yes | No (build it yourself) |
| Metrics (JSON + Prometheus) | Yes | Via `INFO` command or exporter |

For all fourteen, correctness is backed by real tests (Go unit tests for the backend, plus live HTTP integration tests run against every SDK, not just "it compiles").

**Authentication is real too, as of a security review this session** — worth calling out on its own, since it wasn't true for most of this project's history. Every SDK has sent an `X-API-Key` header since it was written, but the server never checked it: every request succeeded regardless of whether a key was sent or correct. Fixed — see [API Reference: Authentication](api-reference.md#authentication) — and while fixing it, found and fixed four SDKs that couldn't actually have authenticated even if the server had been checking: Rust and Java accepted an `api_key` config value and silently never sent it anywhere, and PHP only sent it on POST requests, never GET (so `cache_get`, `health`, and every other read-only call went out unauthenticated). All 7 SDKs are now live-verified end-to-end against a real server with an API key enabled.

---

## Scripting: a genuinely different design from Redis, on purpose

Redis scripting is Lua via `EVAL`/`EVALSHA`. This project deliberately does not depend on Redis or a Redis-derived component anywhere, including for scripting, so instead of embedding a Lua VM, a script here **is Go source code**: compiled server-side by the [TinyGo](https://tinygo.org) toolchain to a WASI WebAssembly module, then executed in a sandboxed [wazero](https://wazero.io) runtime (pure Go, no cgo), with a hard execution timeout and memory limit enforced per call.

This mirrors the shape of Redis's `SCRIPT LOAD` + `EVALSHA` split — compile once (slow, real seconds, since it invokes an external compiler), execute many times (fast, single-digit milliseconds, since it reuses the already-compiled module) — but the execution surface is genuinely arbitrary Go, not a restricted scripting language, sandboxed by WASM isolation rather than a Lua interpreter's built-in restrictions. An infinite loop or any script exceeding its timeout is force-terminated without affecting the server process, other scripts, or other cached compiled modules.

For cases that don't need arbitrary code — composing the server's own built-in operations (`get`, `set`, `zadd`, `enqueue`, ...) into a multi-step sequence — there's also a separate, simpler **Pipeline** primitive with no code-execution surface at all.

---

## Performance

A first real, reproducible benchmark pass now exists (`api/http_bench_test.go`, `persistence/wal_bench_test.go`; run with `go test ./api/... -bench BenchmarkHTTP -benchtime=3000x -run '^$'` and similarly for `./persistence/...`) — measured on one Apple M2 laptop, not production hardware, and not a substitute for benchmarking your own deployment target. Any *other* throughput or latency numbers you might see elsewhere in this project's history that aren't backed by a runnable benchmark in the repo should still not be trusted.

What the numbers say, and what can be said honestly beyond them:
- A real HTTP round trip (loopback TCP, not an in-process handler call) to `/consume` — one of the simplest writes in the system — takes ~45μs sequential, ~15.6μs amortized under concurrent load (~64k ops/sec aggregate on 8 cores). `/cache/set` + `/cache/get` back to back takes ~88-93μs. This is a real floor under latency compared to Redis's RESP protocol, which keeps a persistent connection open and doesn't pay TCP/HTTP overhead per command; TollMeshCache has no persistent-connection or pipelining protocol yet.
- `SortedSet.Rank`/`RevRank` are now O(log n), matching Redis's `ZRANK` (`Insert`, `Delete`, and range queries were already O(log n)). This required adding per-pointer span tracking to the skip list, which also surfaced and fixed a real correctness bug: `Delete` previously navigated by member name alone even though the list is ordered by `(score, member)`, so a member whose name sorted differently than its score position could silently fail to be removed (confirmed live, and now covered by a randomized regression test — see `sortedset/skiplist_test.go`).
- WASM script execution, once compiled, is fast (single-digit milliseconds observed live across all 7 SDKs) because the compiled module is cached and reused; compilation itself takes real seconds (TinyGo invokes an actual Go-to-WASM compiler process) and should be treated the same way you'd treat `SCRIPT LOAD` — infrequent, not on the hot path.
- Every successful `Set`/`Consume`/`Seen` now logs to the WAL (`PersistenceEngine.LogOperation`, ~4.4μs sequential, ~3.7μs under concurrent writers), and a fresh process automatically recovers full state on startup by loading the latest snapshot and replaying every WAL entry after it — verified live by hard-killing a running server (`SIGKILL`, no graceful shutdown) and confirming a new process on the same data directory recovered cache, replay-protection, and exact rate-limiter counts correctly. This was not true until recently: for most of this project's history "write-ahead log" was aspirational — the write path never called it, so only an explicit `create_snapshot` protected anything, and nothing replayed automatically on restart. (The package used to also contain a second, entirely unused persistence implementation — `WriteAheadLog`/`SnapshotManager`/`RecoveryManager` — with zero references outside its own tests; it's been removed rather than left as confusing dead code.) Recovery time and write-path overhead at real (non-benchmark) scale still haven't been measured; Redis's RDB/AOF persistence remains battle-tested over 15+ years in production at massive scale in a way this cannot yet claim.

---

## Architecture

**TollMeshCache**: peer-to-peer, CRDT-based. Each node holds full state; nodes gossip over the same HTTP API every SDK uses and merge incoming state via each primitive's real CRDT merge (`GCounter`/`GSet`, and cache's per-key LWW-register). No coordinator, no leader election. This is a real, meaningful structural difference from Redis, and — unlike a lot of what's in this document's history — it's now genuinely working, not just unit-tested in isolation: verified live against real, separately launched OS processes, including concurrent writes to the same key on different nodes converging correctly and correct counter aggregation across the cluster. Seven primitives replicate today: the original three (rate limiting, replay protection, cache) plus, as of this session, Sorted Sets, Streams, Pipelines, and Search. Sorted Sets was straightforward — `SortedSet.Merge` already had a real CRDT conflict resolution built for local testing, just not wired to gossip yet. Streams needed a real fix first: entry IDs weren't actually globally unique across nodes (a plain per-node sequence counter, easy to collide across two nodes writing in the same millisecond), which would have silently corrupted a union-merge — fixed by putting the node into the ID. Pipelines was close to Cache's shape (a named registry, LWW per entry) and mostly reused that pattern, with one open gap: pipeline deletion doesn't replicate (no tombstone yet). Search was the same shape as Pipelines (a per-document LWW register), but needed a real pre-existing bug fixed first — re-indexing a document under an ID that was already indexed double-counted every BM25 statistic instead of replacing the old contribution — and shares the same open gap: document deletion doesn't replicate. The other six (Job Queues, Pub/Sub, Transactions, Persistence, WASM Scripting, Ranking, Metrics) still live entirely within a single `MeshStore` instance's in-memory maps (plus, for Persistence, its own WAL/snapshot files), with no gossip replication wired to them, and most don't yet have a per-entry version concept to build that on top of. See [Architecture: Gossip and state sync](architecture.md#gossip-and-state-sync) for the details of all three, including a subtle cache durability bug a live crash test caught.

**Redis**: single-writer master with optional replicas, or Redis Cluster for horizontal scale via hash-slot sharding. Mature, well-understood failure modes, an enormous ecosystem (Sentinel, Cluster, modules, every major language's client libraries), and production deployments at massive scale.

---

## Honest Verdict

Redis is not being outperformed or outclassed here, and no credible claim to that effect should be made. Redis is a mature, extremely fast, heavily battle-tested system with a huge feature surface and 15+ years of production hardening. This project does not have verified multi-node operation under real load, a formal performance benchmark, or anywhere near Redis's operational track record.

What TollMeshCache offers that's genuinely different: a peer-to-peer CRDT model with no central coordinator, real (not Lua-derived) arbitrary-code scripting via TinyGo/WASM sandboxing, and identical client APIs across 7 languages, for fourteen capabilities that are now wired end-to-end and tested rather than aspirational — including persistence, pub/sub, and transactions, which were previously listed here as "exists as code but isn't usable yet."

Use TollMeshCache if the no-central-coordinator architecture or the WASM-sandboxed scripting model specifically matters to your use case, and you've verified the supported features cover what you need. Use Redis for anything requiring a proven production track record, multi-node operation at scale, or raw throughput.
