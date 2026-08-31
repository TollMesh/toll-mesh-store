<?php

require_once __DIR__ . '/../vendor/autoload.php';

use TollMesh\Cache\Client;
use TollMesh\Cache\ClientConfig;

$config = new ClientConfig();
$config->host = 'localhost';
$config->port = 8080;

$client = new Client($config);

echo str_repeat('=', 60) . "\n";
echo "TollMeshCache - Rate Limiting Example (PHP)\n";
echo str_repeat('=', 60) . "\n";

try {
    // Example 1: Basic rate limiting
    echo "\n1. Basic Rate Limiting (100 req/min)\n";
    echo str_repeat('-', 60) . "\n";

    for ($i = 0; $i < 3; $i++) {
        $result = $client->consume('user-123', 100, 60000);
        echo "Request " . ($i + 1) . ":\n";
        echo "  Status: " . ($result['ok'] ? 'ALLOWED' : 'LIMITED') . "\n";
        echo "  Remaining: " . $result['remaining'] . "\n";
    }

    // Example 2: Tier-based
    echo "\n2. Tier-Based Rate Limiting\n";
    echo str_repeat('-', 60) . "\n";

    $tiers = ['free' => 10, 'pro' => 100, 'enterprise' => 1000];
    foreach ($tiers as $tier => $limit) {
        $result = $client->consume("user-tier-{$tier}", $limit, 60000);
        $status = $result['ok'] ? '✓ OK' : '✗ LIMITED';
        printf("%s: %s (%d remaining)\n",
            strtoupper(str_pad($tier, 12)),
            $status,
            $result['remaining']
        );
    }

    // Example 3: Health check
    echo "\n3. Server Health\n";
    echo str_repeat('-', 60) . "\n";

    $health = $client->health();
    echo "Status: " . $health['status'] . "\n";
    echo "Node: " . $health['node'] . "\n";
    echo "Peers: " . $health['peers'] . "\n";

} catch (Exception $e) {
    echo "Error: " . $e->getMessage() . "\n";
} finally {
    echo "\n" . str_repeat('=', 60) . "\n";
    echo "Example complete!\n";
}
