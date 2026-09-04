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

`coordination/gossip.go` implements peer-to-peer gossip: each node periodically exchanges state with a subset of peers rather than broadcasting to all of them, so the cost of propagation stays low as the cluster grows. `coordination/state_sync.go` builds a Merkle tree over each node's state so two nodes can cheaply detect *which* keys differ (comparing tree hashes) without transferring full state on every sync round, then reconcile only the differing entries.

**Current limitation, stated plainly:** the Job Queue, Sorted Set, Stream, Pub/Sub, Transactions, Persistence, Pipelines, WASM Scripting, Search, Ranking, and Metrics features all live inside a single `MeshStore` instance's in-memory maps (and, for Persistence, its own WAL/snapshot files on that node's local disk). Gossip and state sync are real, tested subsystems, but they are not currently wired to replicate these specific features across nodes — only the original CRDT-based primitives (rate limiting, replay protection, cache) have been verified to converge across a multi-node cluster. Treat every feature added this round as single-node until multi-node replication is explicitly extended to cover it.

## Storage layout

Each node's live state is entirely in-memory (Go maps and the CRDT types above), guarded by `sync.RWMutex`. Durability is opt-in via the Persistence feature: a checksummed write-ahead log records every mutating operation as it happens, and `create_snapshot` captures a full point-in-time copy of CRDT state to disk that a future `restore_from_latest_snapshot` can replay from — see [API Reference: Persistence](api-reference.md#persistence). Without ever calling those, a process restart loses all data; this is not automatic.

## Sandboxed scripting

WASM Scripting deliberately does not run untrusted code in-process. A script is compiled server-side by an external TinyGo process into a WASI WebAssembly module, then executed inside a dedicated [wazero](https://wazero.io) sandbox with an enforced memory limit and execution timeout, isolated from the host Go process's memory and from other running scripts. See [vs Redis: Scripting](vs-redis.md#scripting-a-genuinely-different-design-from-redis-on-purpose) for why this exists instead of an embedded Lua VM.

## HTTP as the transport

All access — from every one of the 7 SDKs — goes through a single HTTP API (`api/http.go`). There is no persistent-connection or pipelining protocol like Redis's RESP; every operation is one HTTP request/response round-trip. This is simple and easy to reason about, but it puts a real floor under per-operation latency compared to a protocol designed to keep a connection open and pipeline commands — see [vs Redis: Performance](vs-redis.md#performance).
