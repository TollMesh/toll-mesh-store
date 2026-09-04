---
layout: default
title: API Reference
nav_order: 10
---

# API Reference

Complete HTTP API and SDK method reference for TollMeshCache.

All examples below use verified, working method signatures — every method listed here has been run against a live server as part of implementing it.

---

## Rate Limiting & Replay Protection & Cache

### `consume(key, limit, window)` — Rate Limiting

Consume tokens from a distributed rate limiter (`POST /consume`).

**Parameters:**
- `key` (string): Rate limit bucket key
- `limit` (integer): Maximum tokens in the window
- `window` (duration): Time window

**Returns:** `{ ok, remaining, reset_at }`

```python
result = client.consume('user-123', limit=100, window=timedelta(minutes=1))
if not result['ok']:
    raise RateLimitError("Rate limited")
```

### `seen(key, ttl)` — Replay Protection

Check if a nonce/request ID has been seen before (`POST /seen`).

**Returns:** `{ seen }`

```python
if client.seen('nonce-abc123', ttl=timedelta(minutes=5))['seen']:
    raise ReplayError("Replay detected!")
```

### `cache_get(namespace, key)` / `cache_set(namespace, key, value, ttl)`

Distributed cache operations (`POST /cache/get`, `POST /cache/set`).

```python
value, exists = client.cache_get('users', 'user-123')
if not exists:
    client.cache_set('users', 'user-123', fetch_user(), ttl=timedelta(hours=1))
```

### `health()` / `get_peers()`

Cluster status (`GET /health`, `GET /peers`).

---

## Job Queues

Distributed task processing with exactly-once completion semantics, priority ordering, and automatic dead-lettering on max retries.

### `enqueue(queue, payload, priority, max_retries, deadline)`

`POST /queue/enqueue`

**Parameters:**
- `queue` (string): Queue name
- `payload` (string): Job payload
- `priority` (int, default 5): 0–10, higher runs first
- `max_retries` (int, default 3)
- `deadline` (duration, default 24h): Job expires if unclaimed past this

**Returns:** the created `Job` — `{ id, queue, payload, status, priority, retry_count, max_retries, result, error, created_at, updated_at, deadline_at }`

```python
job = client.enqueue('tasks', 'process-order-42', priority=8)
```

### `claim(queue, worker_id)`

`POST /queue/claim` — claims the next available pending job (FIFO among equal priority; higher priority first). Raises if none claimable.

```python
job = client.claim('tasks', 'worker-1')
```

### `complete(queue, job_id, result)` / `fail(queue, job_id, error)`

`POST /queue/complete`, `POST /queue/fail` — mark a **claimed** job as completed or failed. `fail` triggers retry (back to pending) up to `max_retries`, then dead-letters the job. Completing or failing a job that was never claimed (still pending) is an error.

```python
client.complete('tasks', job['id'], 'done')
# or, on failure:
client.fail('tasks', job['id'], 'downstream timeout')
```

### `job_status(queue, job_id)` / `queue_stats(queue)`

`GET /queue/status`, `GET /queue/stats` — look up a job, or aggregate queue stats (`total_jobs`, `pending`, `processing`, `active_workers`, `dead_letter_size`).

---

## Sorted Sets

CRDT-based sorted sets with composite `(score, timestamp, node)` conflict resolution and skip-list storage. See [vs Redis](vs-redis.md) for the performance characteristics of each operation.

### `zadd(key, score, member)`

`POST /zset/add` — insert or update a member's score.

```python
client.zadd('leaderboard', 100, 'alice')
client.zadd('leaderboard', 150, 'bob')
```

### `zrem(key, member)`

`POST /zset/remove` — soft-delete (tombstone) a member.

### `zscore(key, member)` / `zrank(key, member)` / `zrevrank(key, member)`

`GET /zset/score`, `GET /zset/rank`, `GET /zset/revrank` — each returns `(value, exists)`. `zrank` is ascending (0 = lowest score), `zrevrank` is descending (0 = highest score).

```python
score, exists = client.zscore('leaderboard', 'alice')
rank, exists = client.zrank('leaderboard', 'alice')       # ascending
rev_rank, exists = client.zrevrank('leaderboard', 'alice')  # descending
```

### `zrange(key, min, max, limit)` / `zrevrange(key, max, min, limit)`

`GET /zset/range`, `GET /zset/revrange` — range queries by score. `zrange` is ascending and takes `(min, max)`; `zrevrange` is descending and takes `(max, min)` first, matching Redis's own `ZREVRANGEBYSCORE` calling convention.

```python
lowest_10 = client.zrange('leaderboard', limit=10)
top_10 = client.zrevrange('leaderboard', limit=10)
```

### `zcard(key)`

`GET /zset/card` — number of active (non-tombstoned) members.

---

## Streams

Append-only event logs with consumer-group coordination.

### `xadd(stream, fields)`

`POST /stream/add` — append an entry. Returns the created entry: `{ id, timestamp, fields, node, sequence }`. IDs are `<timestamp>-<sequence>`, strictly increasing.

```python
entry = client.xadd('events', {'type': 'login', 'user': 'alice'})
```

### `xrange(stream, start, end, limit)` / `xlen(stream)`

`GET /stream/range`, `GET /stream/len` — read a range of entries (`start="0"` = beginning, `end="-"` = most recent) or get the entry count.

### `xgroup_create(stream, group)`

`POST /stream/group/create` — create a named consumer group on a stream.

### `xreadgroup(group, consumer, stream, limit)`

`POST /stream/group/read` — read entries for a consumer. The **first** call for a given `consumer` name auto-registers it in the group, starting from the beginning of the stream. Entries are delivered but not consumed from the group's perspective until acknowledged — an unacked entry is re-delivered on the next read, so at-least-once processing is the default; track processed IDs yourself to avoid reprocessing, then call `xack`.

```python
client.xgroup_create('events', 'analytics')
entries = client.xreadgroup('analytics', 'worker-1', 'events')
for entry in entries:
    process(entry['fields'])
    client.xack('events', 'analytics', 'worker-1', entry['id'])
```

### `xack(stream, group, consumer, entry_id)`

`POST /stream/group/ack` — advance the consumer's offset to `entry_id`. Everything up to and including it is treated as processed.

---

## Pub/Sub

Topic-based messaging with polling delivery (no long-lived connection required — a subscriber calls `poll` to drain what arrived since its last poll).

### `subscribe(subscriber_id, topic, pattern)` / `unsubscribe(subscriber_id, topic)`

`POST /pubsub/subscribe`, `POST /pubsub/unsubscribe` — register or remove interest in a topic. `pattern` is an optional glob-style filter.

### `publish(topic, publisher, payload)`

`POST /pubsub/publish` — deliver a message to every current subscriber of `topic`. A topic with no subscribers yet still accepts publishes (matching Redis's `PUBLISH` semantics) — the message is simply delivered to nobody. Returns the number of subscribers it was delivered to.

```python
delivered = client.publish('orders', 'checkout-service', '{"order_id": 42}')
```

### `poll(subscriber_id, limit, timeout_ms)`

`POST /pubsub/poll` — return up to `limit` messages queued for `subscriber_id` since its last poll, waiting up to `timeout_ms` if none are immediately available (long-poll). This is how a subscriber actually receives messages — a Go channel can't cross an HTTP request/response boundary, so delivery is pull-based rather than push-based.

```python
client.subscribe('worker-1', 'orders')
messages = client.poll('worker-1', limit=10, timeout_ms=5000)
```

### `get_topics()` / `get_topic_subscribers(topic)` / `pubsub_stats()`

`GET /pubsub/topics`, `GET /pubsub/subscribers`, `GET /pubsub/stats` — introspection.

---

## Transactions

Multi-operation atomic commits — either every queued operation applies or none do.

### `begin_transaction(txn_id)`

`POST /txn/begin` — open a transaction under a caller-chosen ID.

### `add_transaction_operation(txn_id, type, namespace, key, value)`

`POST /txn/operation` — queue an operation (currently `set`) onto an open transaction. Nothing is applied to the cache yet.

### `commit_transaction(txn_id)` / `rollback_transaction(txn_id)`

`POST /txn/commit`, `POST /txn/rollback` — commit atomically applies every queued operation to the live cache; rollback discards them. A transaction can only be committed or rolled back once.

```python
client.begin_transaction('txn-1')
client.add_transaction_operation('txn-1', 'set', 'accounts', 'alice', '90')
client.add_transaction_operation('txn-1', 'set', 'accounts', 'bob', '110')
client.commit_transaction('txn-1')
```

### `transaction_status(txn_id)`

`GET /txn/status` — one of `open`, `committed`, `rolled_back`.

---

## Persistence

Write-ahead log plus point-in-time snapshots of live `MeshStore` state (CRDT counters and sets), for crash recovery.

### `create_snapshot()`

`POST /persistence/snapshot` — capture the current state of every CRDT in the store to disk.

### `get_latest_snapshot()`

`GET /persistence/snapshot/latest` — metadata for the most recent snapshot. Returns `null`/`nil`/`None` (SDK-dependent) rather than raising if no snapshot has been created yet.

### `restore_from_latest_snapshot()`

`POST /persistence/restore` — replace live state with the most recent snapshot. Used on startup/recovery, not during normal operation.

### `persistence_stats()`

`GET /persistence/stats` — WAL size, snapshot count, last snapshot time.

---

## Pipelines

Safe, sandboxed composition of the server's own built-in operations (`get`, `set`, `zadd`, `zscore`, `enqueue`, `xadd`) into a named, multi-step sequence, with each step able to save its result under a name and reference an earlier step's saved value as an argument. This has no code-execution surface at all — it's the right tool when what you need is "do these five things in order," not arbitrary logic. For arbitrary logic, see **WASM Scripting** below.

### `register_pipeline(name, steps)` / `execute_pipeline(name)` / `execute_inline_pipeline(steps)`

`POST /pipeline/register`, `POST /pipeline/execute`, `POST /pipeline/execute-inline` — register a named pipeline for reuse, run a registered one, or run a one-off list of steps without registering it.

```python
client.register_pipeline('checkout', [
    {'op': 'get', 'args': {'namespace': 'inventory', 'key': 'sku-1'}, 'save_as': 'stock'},
    {'op': 'zadd', 'args': {'key': 'sales', 'member': 'sku-1', 'score': 1}},
])
result = client.execute_pipeline('checkout')
```

### `get_pipeline(name)` / `list_pipelines()` / `delete_pipeline(name)`

`GET /pipeline/get`, `GET /pipeline/list`, `POST /pipeline/delete` — management.

---

## WASM Scripting

Real arbitrary-code execution: a script is **Go source code**, compiled by the [TinyGo](https://tinygo.org) toolchain to a WASI WebAssembly module, then run in a sandboxed [wazero](https://wazero.io) runtime (pure Go, no cgo) with a hard execution timeout and memory limit. Input is delivered on the module's stdin; its result is read from stdout. This mirrors Redis's `SCRIPT LOAD` + `EVALSHA` split: compilation is slow (TinyGo takes real seconds, so client SDKs use a longer timeout for it — see note below) and happens once via `compile_script`; execution reuses the already-compiled module and is cheap — typically single-digit milliseconds — so it can happen many times via `execute_script`.

> **Client timeout note:** `compile_script` and `execute_inline_script` invoke TinyGo server-side, which the server allows up to 60 seconds for. Every SDK's default HTTP timeout is ~5 seconds for everything else, so `compile_script`/`execute_inline_script` use an extended client-side timeout automatically — you don't need to configure this yourself. `execute_script` runs against an already-compiled module and stays on the normal short timeout.

### `compile_script(name, source)`

`POST /script/compile` — compile Go source to WASM and register it under `name`, replacing any existing script with that name.

```python
source = '''package main

import (
    "bufio"
    "fmt"
    "os"
)

func main() {
    scanner := bufio.NewScanner(os.Stdin)
    scanner.Scan()
    fmt.Printf("echo: %s\\n", scanner.Text())
}
'''
client.compile_script('echo', source)
```

### `execute_script(name, input)`

`POST /script/execute` — run a registered script, feeding `input` on stdin, returning what it wrote to stdout.

```python
output = client.execute_script('echo', 'hello')  # "echo: hello\n"
```

### `execute_inline_script(source, input)`

`POST /script/execute-inline` — compile and immediately run source without registering it. Pays the full compile cost every call; prefer `compile_script` + `execute_script` for anything called more than once.

### `get_script(name)` / `list_scripts()` / `delete_script(name)`

`GET /script/get`, `GET /script/list`, `POST /script/delete` — management. `get_script` includes `executions` (call count) and `last_error`, if any.

**Sandboxing:** each execution runs in its own WASM module instance with a configured memory limit; an infinite loop or any script exceeding the execution timeout is force-terminated without affecting other scripts, cached compiled modules, or the server process.

---

## Search

Hybrid lexical (BM25) and vector (cosine similarity) document search.

### `index_document(id, content, metadata, vector)`

`POST /search/index` — index a document for BM25 search (via `content`), vector search (via `vector`), or both.

### `search_bm25(query, top_k)` / `search_vector(vector, top_k)` / `search_hybrid(query, vector, top_k)`

`GET /search/bm25`, `POST /search/vector`, `POST /search/hybrid` — `search_hybrid` combines both signals into one ranked result set.

```python
client.index_document('doc-1', 'distributed cache with CRDT conflict resolution', vector=[0.1, 0.2, 0.3])
results = client.search_hybrid('crdt cache', vector=[0.1, 0.2, 0.3], top_k=5)
```

### `delete_search_document(id)`

`POST /search/delete`.

---

## Ranking

Reorder a list of items by strategy (`bm25` or others), with optional per-field score boosts.

### `rank(items, strategy, boosts)`

`POST /rank`.

```python
ranked = client.rank(candidate_items, strategy='bm25', boosts={'title': 2.0})
```

---

## Metrics

### `get_metrics()`

`GET /metrics` — structured JSON metrics (request counts, latencies, per-feature counters) for this node.

### `get_prometheus_metrics()`

`GET /metrics/prometheus` — the same metrics in Prometheus text-exposition format, for scraping. Unlike every other method, this returns a raw string, not parsed JSON — every SDK bypasses its normal JSON-decoding path for this one call.

---

## Error Codes

All SDKs standardize on these error codes for the rate-limiting/replay/cache endpoints:

| Code | Name | Description |
|------|------|-------------|
| 0 | `OK` | Success |
| 400 | `INVALID_REQUEST` | Invalid parameters |
| 401 | `UNAUTHORIZED` | API key invalid |
| 404 | `NOT_FOUND` | Resource not found |
| 429 | `RATE_LIMITED` | Rate limit exceeded |
| 500 | `INTERNAL_ERROR` | Server error |
| 503 | `UNAVAILABLE` | Service unavailable |

The Job Queue, Sorted Set, Stream, Pub/Sub, Transactions, Persistence, Pipelines, WASM Scripting, Search, Ranking, and Metrics endpoints return a plain `{ "error": "<message>" }` body on failure with a matching HTTP status (400/404/409/500) rather than a numeric code — check the error message text.

---

## Configuration Options

### Common to All SDKs

```
host            String          Default: localhost
port            Integer         Default: 8080
timeout         Duration        Default: 5 seconds
verify_ssl      Boolean         Default: true
api_key         String          Default: none
scheme          String          Default: http
max_retries     Integer         Default: 3
```

### Examples

**Python:**
```python
config = ClientConfig(host='api.example.com', port=8080, timeout=10.0, api_key='sk-xxx')
```

**Node.js:**
```typescript
const client = new Client({ host: 'api.example.com', port: 8080, timeout: 10000, apiKey: 'sk-xxx' });
```

**Java:**
```java
ClientConfig config = new ClientConfig().setHost("api.example.com").setPort(8080);
```
