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

The Job Queue, Sorted Set, and Stream endpoints return a plain `{ "error": "<message>" }` body on failure with a matching HTTP status (400/404/409/500) rather than a numeric code — check the error message text.

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
