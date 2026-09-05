---
layout: default
title: Architecture
nav_order: 12
---

# Architecture

Design and consistency model. See [vs Redis](vs-redis.md) for how this compares to a single-writer system, and [Go Backend](go.md) for the package-level breakdown.

---

## Peer-to-peer, no coordinator

Every node runs a full `MeshStore` — there is no leader, no coordinator process, and no single node whose failure takes down writes. Reads and writes are served locally by whichever node receives them; consistency across nodes is achieved after the fact by gossip, not enforced before the fact by consensus. This is the central structural trade-off of the whole system: availability and write latency are prioritized over strong consistency, in the same family as Cassandra or Riak rather than a single-writer system like standalone Redis.

## Conflict resolution: CRDTs + Lamport clocks

State that can be modified from multiple nodes concurrently is represented as a Conflict-free Replicated Data Type, chosen so that merging two nodes' versions of the same value is always well-defined and always converges to the same result regardless of merge order:

- **`GCounter`** (grow-only counter) — backs rate limiting. Each node tracks its own increments; the logical value is the sum across all nodes. Merging two `GCounter`s is just taking the per-node max, so merging is commutative, associative, and idempotent — safe to apply out of order or more than once.
- **`GSet`** (grow-only set) — backs replay protection. A nonce is either in the set or not; merging is a set union.
- Ordering, where it matters (e.g. sorted set score updates, stream entries), uses **Lamport clocks** — a logical counter that establishes a causal `happens-before` ordering across nodes without requiring synchronized wall-clock time.

Everything built on top of these primitives inherits the same convergence guarantee: apply updates in any order, on any subset of nodes, and every node ends up in the same state once gossip has propagated. What you give up is strong consistency — a read immediately after a write on a different node can briefly see stale state until the next gossip round.

## Gossip and state sync

`coordination/gossip.go` implements real peer-to-peer gossip, riding on the same HTTP API every SDK uses rather than a separate wire protocol: each node periodically picks a random peer, `GET`s its `/internal/state`, and merges the result into local state via `MeshStore.MergeState`, which applies each primitive's own CRDT merge (`GCounter.Merge`, `GSet.Merge`) so the merge is commutative, associative, and idempotent regardless of round order. A node joins a cluster with `-join <peer-http-addr>`, which does a one-time `POST /internal/peers/join` handshake that also returns the peer's own peer list, so joining through a single existing member discovers the rest of an already-formed cluster.

This replaces what used to be here: for most of this project's history, `performGossip` was a literal no-op placeholder (its own comment said as much), and there was no way to even join two real node processes into a cluster. That's now fixed for the three original CRDT-backed primitives — verified against three separately launched OS processes, not just tests, including a concurrent-write scenario and correct `GCounter` aggregation across the cluster.

**Current limitation, stated plainly:** the Job Queue, Pub/Sub, Transactions, Persistence, Pipelines, WASM Scripting, Search, Ranking, and Metrics features all live inside a single `MeshStore` instance's in-memory maps (and, for Persistence, its own WAL/snapshot files on that node's local disk) and are **not** part of `MeshStoreState` — only the original three primitives (rate limiting, replay protection, cache) plus, as of this session, Sorted Sets and Streams replicate across nodes today. `coordination/state_sync.go`'s Merkle-tree-based diffing is unrelated to this transport and remains unused outside its own unit tests; the working path is the simpler full-state pull described above, not a Merkle diff.

Sorted Sets was the first of the ten feature groups added after the original three primitives to gain replication, and the easiest: `SortedSet.Merge` already implemented a real `(score, timestamp, node)` CRDT conflict resolution for local testing purposes, so extending it to gossip was a matter of wiring a wire-format snapshot (`SortedSet.Snapshot`/`MergeSnapshot`, including tombstoned members so deletes propagate, not just additions) through `GetState`/`MergeState`, not designing new conflict-resolution logic.

Streams was the second, and needed one real fix first: entry IDs were `<timestamp>-<sequence>`, where `sequence` is a plain per-`Stream`-instance counter — two different nodes' independent `Stream` objects for the same stream name both start at 0 and increment with no coordination between them, so two nodes producing entries in the same millisecond (ordinary under real write volume) could produce the identical ID for two different entries. Harmless single-node, but fatal for a union-merge, since the ID is exactly the identity a merge uses to decide "have I seen this already". Fixed by adding the node to the ID (`<timestamp>-<sequence>-<node>`) — safe because nothing in the codebase parses an ID's structure, every use is an opaque string lookup, confirmed via a repo-wide search before making the change. With globally-unique IDs, merging is a plain set union (entries are immutable once appended, unlike Sorted Sets or Cache, so there's no per-entry conflict to resolve) — the one thing that needs care is that `Range`/`GetFirst`/`GetLast` are positional over the entry slice, not ID-derived, so merged entries are inserted in the correct chronological position, not just appended.

The remaining eight feature groups don't have this head start — most have no per-entry version/conflict-resolution concept at all yet, so replicating them will mean designing that first, not just wiring up gossip.

Cache is a real per-key LWW-register CRDT: each entry carries the wall-clock time it was written and its writer's node ID, and `MergeState` adopts a peer's entry for a key only when it's strictly newer than the local one (the writer's node ID breaks an exact tie) — two nodes concurrently writing the *same* key converge to whichever write actually happened later, on every node, regardless of gossip order. This also required separating "when this node wrote a WAL entry" from "the cache entry's actual CRDT version": a value learned via gossip carries its *original* writer's timestamp for versioning purposes, which can be earlier than this node's own most recent snapshot, while the WAL entry recording it is still logged at "now" for the WAL's own recovery-cutoff bookkeeping — conflating the two made a gossip-learned value vanish on the receiving node's own next restart, reverting it to that node's older local write (confirmed live against two real server processes before and after the fix).

## Storage layout

Each node's live state is entirely in-memory (Go maps and the CRDT types above), guarded by `sync.RWMutex`. Every successful rate-limit consume, replay-protection check, and cache write now logs to a write-ahead log automatically — no explicit call needed — and a new process recovers automatically on startup: it loads the most recent snapshot (if any) and replays every WAL entry logged after it, reconstructing state exactly as it was before the process stopped, including a hard crash (`SIGKILL`, no graceful shutdown), which is the case this has actually been verified against. `create_snapshot` remains available to explicitly compact the WAL into a point-in-time snapshot — see [API Reference: Persistence](api-reference.md#persistence) — but it is no longer the only thing standing between a restart and total data loss for these three primitives. The eight newer feature groups (Job Queues, Sorted Sets, Streams, Pub/Sub, Transactions, Pipelines, WASM Scripting, Search, Ranking, Metrics) are not covered by this at all and still lose all state on restart.

## Sandboxed scripting

WASM Scripting deliberately does not run untrusted code in-process. A script is compiled server-side by an external TinyGo process into a WASI WebAssembly module, then executed inside a dedicated [wazero](https://wazero.io) sandbox with an enforced memory limit and execution timeout, isolated from the host Go process's memory and from other running scripts. See [vs Redis: Scripting](vs-redis.md#scripting-a-genuinely-different-design-from-redis-on-purpose) for why this exists instead of an embedded Lua VM.

## HTTP as the transport

All access — from every one of the 7 SDKs — goes through a single HTTP API (`api/http.go`). There is no persistent-connection or pipelining protocol like Redis's RESP; every operation is one HTTP request/response round-trip. This is simple and easy to reason about, but it puts a real floor under per-operation latency compared to a protocol designed to keep a connection open and pipeline commands — see [vs Redis: Performance](vs-redis.md#performance).
