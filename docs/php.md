# PHP SDK

## Installation

Add via Composer:

```bash
composer require toll-mesh/cache
```

## Quick Start

```php
<?php
require_once 'vendor/autoload.php';

use TollMesh\Cache\Client;
use TollMesh\Cache\ClientConfig;

$config = new ClientConfig([
  'host' => 'localhost',
  'port' => 8080,
]);

$client = new Client($config);

// Rate limiting
$result = $client->consume('user-123', 100, 60000);
if ($result['ok']) {
  echo "Request allowed\n";
}
?>
```

## Features

- ✅ Guzzle HTTP client
- ✅ PSR-compliant
- ✅ Connection pooling
- ✅ Comprehensive error handling
- ✅ Array-based response objects

## API Reference

### Rate Limiting
```php
$result = $client->consume($key, $limit, $windowMs);
// Returns array: ['ok' => bool, 'remaining' => int, 'reset_at' => int]
```

### Replay Protection
```php
$result = $client->seen($key, $ttlMs);
// Returns array: ['seen' => bool]
```

### Caching
```php
$client->cacheSet($namespace, $key, $value, $ttlMs);
list($value, $exists) = $client->cacheGet($namespace, $key);
```

### Health
```php
$health = $client->health();
$peers = $client->getPeers();
```

## Error Handling

```php
<?php
use TollMesh\Cache\Client;
use TollMesh\Cache\TollMeshException;
use TollMesh\Cache\RateLimitException;

try {
  $result = $client->consume('key', 100, 60000);
} catch (RateLimitException $e) {
  echo "Rate limited: " . $e->getMessage() . "\n";
} catch (TollMeshException $e) {
  echo "Error {$e->getCode()}: {$e->getMessage()}\n";
}
?>
```

## Configuration

```php
$config = new ClientConfig([
  'host' => 'localhost',
  'port' => 8080,
  'timeout' => 5.0,
  'verify_ssl' => true,
  'api_key' => 'optional-key',
  'http_scheme' => 'http',
  'max_retries' => 3,
  'connection_pool_size' => 10,
]);

$client = new Client($config);
```

## Testing

```bash
composer install --with-dev
vendor/bin/phpunit
```

## Examples

See `examples/` for:
- `rate_limiting.php` - Rate limiting patterns

Run example:

```bash
php sdks/php/examples/rate_limiting.php
```

## PSR Compliance

The SDK follows PSR standards:
- **PSR-1** - Basic coding standard
- **PSR-2** - Coding style guide
- **PSR-3** - Logger interface
- **PSR-4** - Autoloading standard
- **PSR-7** - HTTP message interfaces (via Guzzle)
- **PSR-18** - HTTP client interfaces
