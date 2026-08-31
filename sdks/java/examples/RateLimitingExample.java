package com.tollmesh.store.examples;

import com.tollmesh.store.*;
import java.time.Duration;

public class RateLimitingExample {
    public static void main(String[] args) {
        ClientConfig config = new ClientConfig()
            .setHost("localhost")
            .setPort(8080);

        try (Client client = new Client(config)) {
            System.out.println("=".repeat(60));
            System.out.println("TollMeshCache - Rate Limiting Example");
            System.out.println("=".repeat(60));

            // Example 1: Basic rate limiting
            System.out.println("\n1. Basic Rate Limiting (100 requests per minute)");
            System.out.println("-".repeat(60));

            for (int i = 0; i < 3; i++) {
                ConsumeResult result = client.consume(
                    "user-123",
                    100,
                    Duration.ofMinutes(1)
                );

                System.out.println("Request " + (i + 1) + ":");
                System.out.println("  Status: " + (result.isOk() ? "ALLOWED" : "LIMITED"));
                System.out.println("  Remaining: " + result.getRemaining());
            }

            // Example 2: API tier rate limiting
            System.out.println("\n2. Tier-Based Rate Limiting");
            System.out.println("-".repeat(60));

            String[] tiers = {"free", "pro", "enterprise"};
            int[] limits = {10, 100, 1000};

            for (int i = 0; i < tiers.length; i++) {
                ConsumeResult result = client.consume(
                    "user-tier-" + tiers[i],
                    limits[i],
                    Duration.ofMinutes(1)
                );

                String status = result.isOk() ? "✓ OK" : "✗ LIMITED";
                System.out.printf("%s: %s (%d remaining)%n",
                    String.format("%-12s", tiers[i].toUpperCase()),
                    status,
                    result.getRemaining()
                );
            }

            // Example 3: Health check
            System.out.println("\n3. Server Health Status");
            System.out.println("-".repeat(60));

            HealthResponse health = client.health();
            System.out.println("Status: " + health.getStatus());
            System.out.println("Node: " + health.getNode());
            System.out.println("Peers: " + health.getPeers());

            System.out.println("\n" + "=".repeat(60));
            System.out.println("Example complete!");
            System.out.println("=".repeat(60));

        } catch (Exception e) {
            System.err.println("ERROR: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
