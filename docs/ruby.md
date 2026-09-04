---
layout: default
title: Ruby SDK
nav_order: 7
---

# Ruby SDK

## Installation

Add to `Gemfile`:

```ruby
gem 'tollmeshcache'
```

Then run:

```bash
bundle install
```

Or install directly:

```bash
gem install tollmeshcache
```

## Quick Start

```ruby
require 'tollmeshcache'

config = TollMeshCache::ClientConfig.new(
  host: 'localhost',
  port: 8080
)

client = TollMeshCache::Client.new(config)

# Rate limiting
result = client.consume('user-123', 100, 60000)
if result[:ok]
  puts 'Request allowed'
end
```

## Features

- ✅ HTTPClient gem for reliable connections
- ✅ Idiomatic Ruby patterns
- ✅ Exception-based error handling
- ✅ Connection pooling
- ✅ Hash-based response objects

## API Reference

> This section covers rate limiting, replay protection, and caching. This SDK also has full, identical-API support for Job Queues, Sorted Sets, Streams, Pub/Sub, Transactions, Persistence, Pipelines, WASM Scripting, Search, Ranking, and Metrics — see the [full API Reference](api-reference.md) for all of them, with live-verified examples.

### Rate Limiting
```ruby
result = client.consume(key, limit, window_ms)
# Returns hash: { ok: boolean, remaining: integer, reset_at: integer }
```

### Replay Protection
```ruby
result = client.seen(key, ttl_ms)
# Returns hash: { seen: boolean }
```

### Caching
```ruby
client.cache_set(namespace, key, value, ttl_ms)
value, exists = client.cache_get(namespace, key)
```

### Health
```ruby
health = client.health
peers = client.get_peers
```

## Error Handling

```ruby
require 'tollmeshcache'

begin
  result = client.consume('key', 100, 60000)
rescue TollMeshCache::RateLimitError => e
  puts "Rate limited: #{e.message}"
rescue TollMeshCache::TollMeshError => e
  puts "Error #{e.code}: #{e.message}"
end
```

## Configuration

```ruby
config = TollMeshCache::ClientConfig.new(
  host: 'localhost',
  port: 8080,
  timeout: 5,
  verify_ssl: true,
  api_key: 'optional-key',
  http_scheme: 'http',
  max_retries: 3,
  connection_pool_size: 10
)

client = TollMeshCache::Client.new(config)
```

## Testing

```bash
bundle install --with development
rspec
```

## Examples

See `examples/` for:
- `rate_limiting.rb` - Rate limiting patterns
