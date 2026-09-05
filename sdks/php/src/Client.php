<?php

namespace TollMesh\Cache;

use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Exception\RequestException;

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
        // /cache/get is a GET endpoint taking query params (see
        // api/http.go handleCacheGet), not a POST with a JSON body.
        return $this->get('/cache/get', ['namespace' => $namespace, 'key' => $key]);
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

    // ===== Pub/Sub =====

    public function subscribe(string $subscriberId, string $topic, string $pattern = ''): void
    {
        $this->post('/pubsub/subscribe', ['subscriber_id' => $subscriberId, 'topic' => $topic, 'pattern' => $pattern]);
    }

    public function unsubscribe(string $subscriberId, string $topic): void
    {
        $this->post('/pubsub/unsubscribe', ['subscriber_id' => $subscriberId, 'topic' => $topic]);
    }

    public function publish(string $topic, string $publisher, string $payload): int
    {
        $response = $this->post('/pubsub/publish', ['topic' => $topic, 'publisher' => $publisher, 'payload' => $payload]);
        return $response['delivered_count'] ?? 0;
    }

    public function poll(string $subscriberId, int $limit = 10, int $timeoutMs = 5000): array
    {
        $response = $this->post('/pubsub/poll', ['subscriber_id' => $subscriberId, 'limit' => $limit, 'timeout_ms' => $timeoutMs]);
        return $response['messages'] ?? [];
    }

    public function getTopics(): array
    {
        return $this->get('/pubsub/topics')['topics'] ?? [];
    }

    public function getTopicSubscribers(string $topic): array
    {
        return $this->get('/pubsub/subscribers', ['topic' => $topic])['subscribers'] ?? [];
    }

    public function pubsubStats(): array
    {
        return $this->get('/pubsub/stats');
    }

    // ===== Transactions =====

    public function beginTransaction(string $txnId): array
    {
        return $this->post('/txn/begin', ['txn_id' => $txnId]);
    }

    public function addTransactionOperation(string $txnId, string $type, string $namespace, string $key, string $value = ''): void
    {
        $this->post('/txn/operation', ['txn_id' => $txnId, 'type' => $type, 'namespace' => $namespace, 'key' => $key, 'value' => $value]);
    }

    public function commitTransaction(string $txnId): void
    {
        $this->post('/txn/commit', ['txn_id' => $txnId]);
    }

    public function rollbackTransaction(string $txnId): void
    {
        $this->post('/txn/rollback', ['txn_id' => $txnId]);
    }

    public function transactionStatus(string $txnId): string
    {
        return $this->get('/txn/status', ['txn_id' => $txnId])['status'] ?? '';
    }

    // ===== Persistence =====

    public function createSnapshot(): void
    {
        $this->post('/persistence/snapshot', []);
    }

    public function getLatestSnapshot(): ?array
    {
        try {
            return $this->get('/persistence/snapshot/latest');
        } catch (\Exception $e) {
            return null;
        }
    }

    public function restoreFromLatestSnapshot(): void
    {
        $this->post('/persistence/restore', []);
    }

    public function persistenceStats(): array
    {
        return $this->get('/persistence/stats');
    }

    // ===== Scripting: Pipelines (safe operation composition) =====

    public function registerPipeline(string $name, array $steps): void
    {
        $this->post('/pipeline/register', ['name' => $name, 'steps' => $steps]);
    }

    public function executePipeline(string $name): array
    {
        return $this->post('/pipeline/execute', ['name' => $name]);
    }

    public function executeInlinePipeline(array $steps): array
    {
        return $this->post('/pipeline/execute-inline', ['steps' => $steps]);
    }

    public function getPipeline(string $name): array
    {
        return $this->get('/pipeline/get', ['name' => $name]);
    }

    public function listPipelines(): array
    {
        return $this->get('/pipeline/list')['pipelines'] ?? [];
    }

    public function deletePipeline(string $name): void
    {
        $this->post('/pipeline/delete', ['name' => $name]);
    }

    // ===== Scripting: WASM (real arbitrary Go code execution) =====

    public function compileScript(string $name, string $source): array
    {
        return $this->post('/script/compile', ['name' => $name, 'source' => $source]);
    }

    public function executeScript(string $name, string $input = ''): string
    {
        $response = $this->post('/script/execute', ['name' => $name, 'input' => $input]);
        return $response['output'] ?? '';
    }

    public function executeInlineScript(string $source, string $input = ''): string
    {
        $response = $this->post('/script/execute-inline', ['source' => $source, 'input' => $input]);
        return $response['output'] ?? '';
    }

    public function getScript(string $name): array
    {
        return $this->get('/script/get', ['name' => $name]);
    }

    public function listScripts(): array
    {
        return $this->get('/script/list')['scripts'] ?? [];
    }

    public function deleteScript(string $name): void
    {
        $this->post('/script/delete', ['name' => $name]);
    }

    // ===== Search =====

    public function indexDocument(string $id, string $content, ?array $metadata = null, ?array $vector = null): void
    {
        $body = ['id' => $id, 'content' => $content];
        if ($metadata !== null) {
            $body['metadata'] = $metadata;
        }
        if ($vector !== null) {
            $body['vector'] = $vector;
        }
        $this->post('/search/index', $body);
    }

    public function searchBM25(string $query, int $topK = 10): array
    {
        return $this->get('/search/bm25', ['query' => $query, 'topk' => $topK])['results'] ?? [];
    }

    public function searchVector(array $vector, int $topK = 10): array
    {
        return $this->post('/search/vector', ['vector' => $vector, 'topk' => $topK])['results'] ?? [];
    }

    public function searchHybrid(string $query, array $vector, int $topK = 10): array
    {
        return $this->post('/search/hybrid', ['query' => $query, 'vector' => $vector, 'topk' => $topK])['results'] ?? [];
    }

    public function deleteSearchDocument(string $id): void
    {
        $this->post('/search/delete', ['id' => $id]);
    }

    // ===== Ranking =====

    public function rank(array $items, string $strategy = 'bm25', ?array $boosts = null): array
    {
        $body = ['items' => $items, 'strategy' => $strategy];
        if ($boosts !== null) {
            $body['boosts'] = $boosts;
        }
        return $this->post('/rank', $body)['items'] ?? [];
    }

    // ===== Metrics =====

    public function getMetrics(): array
    {
        return $this->get('/metrics');
    }

    public function getPrometheusMetrics(): string
    {
        $response = $this->http->get('/metrics/prometheus', [
            'headers' => ['User-Agent' => 'tollmeshcache-php/1.0.0'],
        ]);
        return (string) $response->getBody();
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
