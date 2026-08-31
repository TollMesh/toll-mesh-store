# API Reference

Complete API documentation for all SDKs.

## Endpoints

All SDKs provide the same 4 core operations:

### 1. `consume(key, limit, window)` - Rate Limiting

Consume tokens from a distributed rate limiter.

**Parameters:**
- `key` (string): Rate limit bucket key (e.g., "user-123", "api-key-xyz")
- `limit` (integer): Maximum tokens in the window
- `window` (duration): Time window (e.g., 60 seconds, 1 minute)

**Returns:**
```json
{
  "ok": true,
  "remaining": 42,
  "reset_at": 1693498200
}
```

**Error Codes:**
- `429` - Rate limit exceeded
- `400` - Invalid parameters
- `500` - Server error

**Usage:**

```python
# Python
result = client.consume('user-123', limit=100, window=timedelta(minutes=1))
if not result['ok']:
    raise RateLimitError("Rate limited")
```

```typescript
// Node.js
const result = await client.consume('user-123', 100, 60000);
if (!result.ok) throw new RateLimitError("Rate limited");
```

```java
// Java
ConsumeResult result = client.consume("user-123", 100, Duration.ofMinutes(1));
if (!result.ok) throw new RateLimitException("Rate limited");
```

---

### 2. `seen(key, ttl)` - Replay Protection

Check if a nonce/request ID has been seen before (replay detection).

**Parameters:**
- `key` (string): Nonce, request ID, or transaction hash
- `ttl` (duration): How long to remember this key

**Returns:**
```json
{
  "seen": false
}
```

**Usage:**

```python
# Python
if client.seen('nonce-abc123', ttl=timedelta(minutes=5))['seen']:
    raise ReplayError("Replay detected!")
```

```typescript
// Node.js
if ((await client.seen('nonce-abc123', 300000)).seen) {
  throw new ReplayError("Replay detected!");
}
```

```java
// Java
if (client.seen("nonce-abc123", Duration.ofMinutes(5)).seen) {
  throw new ReplayException("Replay detected!");
}
```

---

### 3. `cache_set(namespace, key, value, ttl)` - Store in Cache

Store a value in the distributed cache.

**Parameters:**
- `namespace` (string): Cache partition (e.g., "users", "sessions")
- `key` (string): Cache key
- `value` (bytes/string): Value to cache
- `ttl` (duration): Expiration time

**Returns:**
```json
{
  "ok": true
}
```

**Usage:**

```python
# Python
client.cache_set(
    namespace='users',
    key='user-123',
    value=json.dumps(user_data),
    ttl=timedelta(hours=1)
)
```

```typescript
// Node.js
await client.cacheSet(
  'users',
  'user-123',
  JSON.stringify(userData),
  3600000
);
```

```java
// Java
client.cacheSet(
  "users",
  "user-123",
  userData.getBytes(),
  Duration.ofHours(1)
);
```

---

### 4. `cache_get(namespace, key)` - Retrieve from Cache

Retrieve a value from the distributed cache.

**Parameters:**
- `namespace` (string): Cache partition
- `key` (string): Cache key

**Returns:**
```json
{
  "value": "base64-encoded-value",
  "exists": true
}
```

**Error Codes:**
- `404` - Key not found (but exists flag is false, not an error)
- `500` - Server error

**Usage:**

```python
# Python
value, exists = client.cache_get('users', 'user-123')
if exists:
    user_data = json.loads(value)
```

```typescript
// Node.js
const [value, exists] = await client.cacheGet('users', 'user-123');
if (exists) {
  const userData = JSON.parse(value);
}
```

```java
// Java
CacheValue cached = client.cacheGet("users", "user-123");
if (cached.exists) {
  UserData userData = parseJson(cached.value);
}
```

---

### 5. `health()` - Health Check

Check cluster health and node status.

**Returns:**
```json
{
  "status": "healthy",
  "node": "node-1",
  "peers": 3,
  "uptime_seconds": 86400,
  "stats": {
    "rate_limits_checked": 10000,
    "replays_detected": 5,
    "cache_hits": 8500
  }
}
```

**Usage:**

```python
# Python
health = client.health()
if health['status'] == 'healthy':
    print(f"Cluster OK, {health['peers']} peers connected")
```

---

### 6. `get_peers()` - List Cluster Peers

Get list of connected cluster nodes.

**Returns:**
```json
{
  "peers": [
    {
      "id": "node-1",
      "host": "10.0.0.1",
      "port": 8080,
      "uptime_seconds": 86400
    },
    {
      "id": "node-2",
      "host": "10.0.0.2",
      "port": 8080,
      "uptime_seconds": 76800
    }
  ]
}
```

---

## Error Codes

All SDKs standardize on these error codes:

| Code | Name | Description |
|------|------|-------------|
| 0 | `OK` | Success |
| 400 | `INVALID_REQUEST` | Invalid parameters |
| 401 | `UNAUTHORIZED` | API key invalid |
| 404 | `NOT_FOUND` | Resource not found |
| 429 | `RATE_LIMITED` | Rate limit exceeded |
| 500 | `INTERNAL_ERROR` | Server error |
| 503 | `UNAVAILABLE` | Service unavailable |

---

## Configuration Options

### Common to All SDKs

```
host            String          Default: localhost
port            Integer         Default: 8080
timeout         Duration        Default: 5 seconds
verify_ssl      Boolean         Default: true
api_key         String          Default: none
http_scheme     String          Default: http
max_retries     Integer         Default: 3
retry_backoff   Float           Default: 1.0
pool_size       Integer         Default: 10
```

### Examples

**Python:**
```python
config = ClientConfig(
    host='api.example.com',
    port=8080,
    timeout=10.0,
    api_key='sk-xxx',
    max_retries=5
)
```

**Node.js:**
```typescript
const config = new ClientConfig({
  host: 'api.example.com',
  port: 8080,
  timeout: 10000,
  apiKey: 'sk-xxx',
  maxRetries: 5
});
```

**Java:**
```java
ClientConfig config = new ClientConfig()
  .setHost("api.example.com")
  .setPort(8080)
  .setTimeout(10000)
  .setApiKey("sk-xxx")
  .setMaxRetries(5);
```

---

## Rate Limiting Strategy

**Token Bucket Algorithm:**
- Client can consume up to `limit` tokens per `window`
- Tokens regenerate at a constant rate
- Burst requests allowed up to limit
- Perfect for API rate limiting, request throttling

**Example: 100 requests per minute**
```
consume("api-key-123", limit=100, window=1min)
```

Each call consumes 1 token. After 60 seconds, tokens reset.

---

## Replay Protection Strategy

**Distributed Nonce Tracking:**
- Each unique nonce is tracked globally
- If nonce appears twice → Replay detected
- Use transaction IDs, request IDs, or UUIDs
- Automatic cleanup after TTL expiration

**Example: Protect payment**
```
request_id = uuid.uuid4()
if seen(request_id, ttl=24h):
    return error("Duplicate transaction")
```

---

## Caching Strategy

**Cache-Aside Pattern:**
1. Check cache first
2. If miss, fetch from source
3. Store in cache with TTL
4. Return to client

**Example:**
```python
namespace = "users"
key = "user-123"

# Try cache
value, exists = client.cache_get(namespace, key)
if exists:
    return deserialize(value)

# Cache miss - fetch from DB
user = db.get_user(123)

# Store in cache
client.cache_set(namespace, key, serialize(user), ttl=1h)
return user
```
