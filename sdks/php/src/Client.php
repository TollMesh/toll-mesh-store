<?php

namespace TollMesh\Cache;

use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Exception\RequestException;

class ClientConfig
{
    public string $host = 'localhost';
    public int $port = 8080;
    public float $timeout = 5.0;
    public bool $verify_ssl = true;
    public ?string $api_key = null;
    public string $scheme = 'http';

    public function getBaseUrl(): string
    {
        return "{$this->scheme}://{$this->host}:{$this->port}";
    }
}

class Client
{
    private ClientConfig $config;
    private GuzzleClient $http;

    public function __construct(?ClientConfig $config = null)
    {
        $this->config = $config ?? new ClientConfig();
        $this->http = new GuzzleClient([
            'base_uri' => $this->config->getBaseUrl(),
            'timeout' => $this->config->timeout,
            'verify' => $this->config->verify_ssl,
        ]);
    }

    public function consume(string $key, int $limit, int $window_ms): array
    {
        $body = [
            'key' => $key,
            'limit' => $limit,
            'window' => $window_ms,
        ];
        return $this->post('/consume', $body);
    }

    public function seen(string $key, int $ttl_ms): array
    {
        $body = ['key' => $key, 'ttl' => $ttl_ms];
        return $this->post('/seen', $body);
    }

    public function cacheGet(string $namespace, string $key): array
    {
        $body = ['namespace' => $namespace, 'key' => $key];
        return $this->post('/cache/get', $body);
    }

    public function cacheSet(string $namespace, string $key, string $value, ?int $ttl_ms = null): void
    {
        $body = [
            'namespace' => $namespace,
            'key' => $key,
            'value' => $value,
        ];
        if ($ttl_ms !== null) {
            $body['ttl'] = $ttl_ms;
        }
        $this->post('/cache/set', $body);
    }

    public function health(): array
    {
        return $this->get('/health');
    }

    public function getPeers(): array
    {
        $response = $this->get('/peers');
        return $response['peers'] ?? [];
    }

    // ===== Job Queues =====

    public function enqueue(string $queue, string $payload, int $priority = 5, int $maxRetries = 3, ?int $deadlineMs = null): array
    {
        $body = ['queue' => $queue, 'payload' => $payload, 'priority' => $priority, 'max_retries' => $maxRetries];
        if ($deadlineMs !== null) {
            $body['deadline'] = $deadlineMs;
        }
        return $this->post('/queue/enqueue', $body);
    }

    public function claim(string $queue, string $workerId): array
    {
        return $this->post('/queue/claim', ['queue' => $queue, 'worker_id' => $workerId]);
    }

    public function complete(string $queue, string $jobId, string $result = ''): void
    {
        $this->post('/queue/complete', ['queue' => $queue, 'job_id' => $jobId, 'result' => $result]);
    }

    public function failJob(string $queue, string $jobId, string $error): void
    {
        $this->post('/queue/fail', ['queue' => $queue, 'job_id' => $jobId, 'error' => $error]);
    }

    public function jobStatus(string $queue, string $jobId): array
    {
        return $this->get('/queue/status', ['queue' => $queue, 'job_id' => $jobId]);
    }

    public function queueStats(string $queue): array
    {
        return $this->get('/queue/stats', ['queue' => $queue]);
    }

    // ===== Sorted Sets =====

    public function zadd(string $key, float $score, string $member): void
    {
        $this->post('/zset/add', ['key' => $key, 'member' => $member, 'score' => $score]);
    }

    public function zrem(string $key, string $member): void
    {
        $this->post('/zset/remove', ['key' => $key, 'member' => $member]);
    }

    public function zscore(string $key, string $member): array
    {
        $response = $this->get('/zset/score', ['key' => $key, 'member' => $member]);
        return [$response['score'] ?? null, $response['exists'] ?? false];
    }

    public function zrank(string $key, string $member): array
    {
        $response = $this->get('/zset/rank', ['key' => $key, 'member' => $member]);
        return [$response['rank'] ?? null, $response['exists'] ?? false];
    }

    public function zrevrank(string $key, string $member): array
    {
        $response = $this->get('/zset/revrank', ['key' => $key, 'member' => $member]);
        return [$response['rank'] ?? null, $response['exists'] ?? false];
    }

    public function zrange(string $key, float $min = -INF, float $max = INF, int $limit = 100): array
    {
        $response = $this->get('/zset/range', ['key' => $key, 'min' => $min, 'max' => $max, 'limit' => $limit]);
        return $response['members'] ?? [];
    }

    /**
     * Get members with scores in [min, max], descending order (highest first).
     * Following Redis's ZREVRANGEBYSCORE convention, max comes before min.
     */
    public function zrevrange(string $key, float $max = INF, float $min = -INF, int $limit = 100): array
    {
        $response = $this->get('/zset/revrange', ['key' => $key, 'max' => $max, 'min' => $min, 'limit' => $limit]);
        return $response['members'] ?? [];
    }

    public function zcard(string $key): int
    {
        $response = $this->get('/zset/card', ['key' => $key]);
        return $response['card'] ?? 0;
    }

    // ===== Streams =====

    public function xadd(string $stream, array $fields): array
    {
        return $this->post('/stream/add', ['stream' => $stream, 'fields' => $fields]);
    }

    public function xrange(string $stream, string $start = '0', string $end = '-', int $limit = 100): array
    {
        $response = $this->get('/stream/range', ['stream' => $stream, 'start' => $start, 'end' => $end, 'limit' => $limit]);
        return $response['entries'] ?? [];
    }

    public function xlen(string $stream): int
    {
        $response = $this->get('/stream/len', ['stream' => $stream]);
        return $response['length'] ?? 0;
    }

    public function xgroupCreate(string $stream, string $group): void
    {
        $this->post('/stream/group/create', ['stream' => $stream, 'group' => $group]);
    }

    /**
     * Read unacknowledged entries for a consumer in a group. First call for
     * a given consumer registers it in the group. Entries remain
     * re-deliverable until acknowledged with xack().
     */
    public function xreadgroup(string $group, string $consumer, string $stream, int $limit = 100): array
    {
        $response = $this->post('/stream/group/read', [
            'stream' => $stream,
            'group' => $group,
            'consumer' => $consumer,
            'limit' => $limit,
        ]);
        return $response['entries'] ?? [];
    }

    public function xack(string $stream, string $group, string $consumer, string $entryId): void
    {
        $this->post('/stream/group/ack', ['stream' => $stream, 'group' => $group, 'consumer' => $consumer, 'id' => $entryId]);
    }

    private function post(string $endpoint, array $body): array
    {
        try {
            $response = $this->http->post($endpoint, [
                'json' => $body,
                'headers' => [
                    'User-Agent' => 'tollmeshcache-php/1.0.0',
                    'X-API-Key' => $this->config->api_key ?? '',
                ],
            ]);

            return json_decode($response->getBody()->getContents(), true) ?? [];
        } catch (RequestException $e) {
            throw new \Exception("Request failed: " . $e->getMessage());
        }
    }

    private function get(string $endpoint, array $query = []): array
    {
        try {
            $response = $this->http->get($endpoint, [
                'query' => $query,
                'headers' => ['User-Agent' => 'tollmeshcache-php/1.0.0'],
            ]);

            return json_decode($response->getBody()->getContents(), true) ?? [];
        } catch (RequestException $e) {
            throw new \Exception("Request failed: " . $e->getMessage());
        }
    }
}
