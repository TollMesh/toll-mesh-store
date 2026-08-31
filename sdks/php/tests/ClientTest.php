<?php

namespace TollMesh\Cache\Tests;

use PHPUnit\Framework\TestCase;
use TollMesh\Cache\Client;
use TollMesh\Cache\ClientConfig;

class ClientTest extends TestCase
{
    private Client $client;
    private ClientConfig $config;

    protected function setUp(): void
    {
        $this->config = new ClientConfig();
        $this->config->host = 'localhost';
        $this->config->port = 8080;
        $this->client = new Client($this->config);
    }

    public function testConfigBaseUrl(): void
    {
        $this->assertEquals('http://localhost:8080', $this->config->getBaseUrl());
    }

    public function testConfigWithScheme(): void
    {
        $config = new ClientConfig();
        $config->scheme = 'https';
        $config->host = 'api.example.com';
        $config->port = 443;

        $this->assertEquals('https://api.example.com:443', $config->getBaseUrl());
    }

    public function testConsumeReturnsArray(): void
    {
        $result = $this->client->consume('user-123', 100, 60000);
        $this->assertIsArray($result);
        $this->assertArrayHasKey('ok', $result);
        $this->assertArrayHasKey('remaining', $result);
        $this->assertArrayHasKey('reset_at', $result);
    }

    public function testSeenReturnsArray(): void
    {
        $result = $this->client->seen('nonce-123', 300000);
        $this->assertIsArray($result);
        $this->assertArrayHasKey('seen', $result);
    }

    public function testCacheGetReturnsArray(): void
    {
        $result = $this->client->cacheGet('namespace', 'key');
        $this->assertIsArray($result);
        $this->assertArrayHasKey('exists', $result);
    }

    public function testHealthReturnsArray(): void
    {
        $result = $this->client->health();
        $this->assertIsArray($result);
        $this->assertArrayHasKey('status', $result);
        $this->assertArrayHasKey('node', $result);
        $this->assertArrayHasKey('peers', $result);
    }
}
