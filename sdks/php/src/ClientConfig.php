<?php

namespace TollMesh\Cache;

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
