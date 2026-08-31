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

    private function get(string $endpoint): array
    {
        try {
            $response = $this->http->get($endpoint, [
                'headers' => ['User-Agent' => 'tollmeshcache-php/1.0.0'],
            ]);

            return json_decode($response->getBody()->getContents(), true) ?? [];
        } catch (RequestException $e) {
            throw new \Exception("Request failed: " . $e->getMessage());
        }
    }
}
