---
layout: default
title: vs Redis
nav_order: 11
---

# TollMeshCache vs Redis

An honest comparison, current as of this project's actual tested state — not aspirational.

---

## What's Actually Verified Working

These six capabilities are wired end-to-end (Go backend → HTTP API → all 7 SDKs) and have been run against a live server as part of building them:

| Feature | TollMeshCache | Redis |
|---------|---------------|-------|
| Rate limiting | Yes (CRDT `GCounter`) | Yes (requires `INCR`+`EXPIRE` or a module) |
| Replay protection | Yes (CRDT `GSet`) | Yes (requires `SETNX`+`EXPIRE`) |
| Key-value cache with TTL | Yes | Yes |
| Job queues | Yes (priority, retry, dead-letter) | Not built in (commonly layered on Streams or Lists) |
| Sorted sets | Yes (skip list, CRDT conflict resolution) | Yes (skip list) |
| Streams with consumer groups | Yes | Yes |

For all six, correctness is backed by real tests (Go unit tests for the backend, plus live HTTP integration tests for every SDK) — not just "it compiles."

---

## What Exists as Code But Isn't Usable Yet

Pub/Sub, Transactions, Persistence, Lua Scripting, Search, Ranking, and Metrics all have Go packages in this repository — roughly 2,700 lines combined — but **none of them are connected to `MeshStore` or the HTTP API**, and **none have any test files**. This was the exact same problem Job Queues, Sorted Sets, and Streams had before this round of work: real code, zero way for any client to reach it, and (unlike those three) no tests proving the logic even works in isolation. If you see these listed as supported elsewhere in older docs, that was inaccurate — they are not usable from any SDK today.

---

## Performance

No benchmark has been run against this system. Any specific throughput or latency numbers you might see elsewhere in this project's history were not measured — they were invented, and have been removed. If performance is a factor in your decision, benchmark it yourself before relying on it; don't take anyone's word for it, including this document's.

What can be said honestly:
- Every write goes through an HTTP round-trip (no persistent-connection or pipelining protocol yet), which puts a real floor under latency compared to Redis's RESP protocol.
- `SortedSet.Rank`/`RevRank` are O(n) in the current implementation (a full skip-list "span" implementation, which is what makes Redis's `ZRANK` O(log n), was not completed — see the sortedset package for details). `Insert`, `Delete`, and range queries are O(log n).
- Everything is in-memory with no durability path wired up (see Persistence, above) — a process restart loses all data. Redis has RDB/AOF persistence, battle-tested over 15+ years.

---

## Architecture

**TollMeshCache**: peer-to-peer, CRDT-based. Each node holds full state; nodes gossip and merge via Lamport-clock conflict resolution. No coordinator, no leader election. This is a real, meaningful structural difference from Redis — but it has only been exercised in single-node tests in this codebase. Multi-node convergence (`coordination/state_sync.go`, `coordination/gossip.go`) exists and has its own test suite, but has not been verified end-to-end with the Job Queue/Sorted Set/Stream features layered on top of it — those features currently live entirely within a single `MeshStore` instance's in-memory maps, with no gossip replication wired to them specifically.

**Redis**: single-writer master with optional replicas, or Redis Cluster for horizontal scale via hash-slot sharding. Mature, well-understood failure modes, an enormous ecosystem (Sentinel, Cluster, modules, every major language's client libraries), and production deployments at massive scale.

---

## Honest Verdict

Redis is not being outperformed or outclassed here, and no credible claim to that effect should be made. Redis is a mature, extremely fast, heavily battle-tested system with a huge feature surface, and this project does not yet have persistence, pub/sub, transactions, or verified multi-node operation under real load.

What TollMeshCache offers that's genuinely different: a peer-to-peer CRDT model with no central coordinator, and identical client APIs across 7 languages, for six specific capabilities (rate limiting, replay protection, caching, job queues, sorted sets, streams) that are now real and tested rather than aspirational.

Use TollMeshCache if the no-central-coordinator architecture specifically matters to your use case and you've verified the six supported features cover what you need. Use Redis for everything else, especially anything requiring persistence, pub/sub, transactions, high throughput, or a track record.
