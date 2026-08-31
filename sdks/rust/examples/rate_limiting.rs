use tollmeshcache::{Client, ClientConfig};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("TollMeshCache - Rate Limiting Example (Rust)");
    println!("{}", "=".repeat(60));

    let config = ClientConfig::new()
        .host("localhost")
        .port(8080);

    let client = Client::new(config)?;

    // Example 1: Basic rate limiting
    println!("\n1. Basic Rate Limiting (100 req/min)");
    println!("{}", "-".repeat(60));

    for i in 0..3 {
        let result = client.consume("user-123", 100, Duration::from_secs(60)).await?;
        println!("Request {}:", i + 1);
        println!("  Status: {}", if result.ok { "ALLOWED" } else { "LIMITED" });
        println!("  Remaining: {}", result.remaining);
    }

    // Example 2: Tier-based
    println!("\n2. Tier-Based Rate Limiting");
    println!("{}", "-".repeat(60));

    let tiers = vec![("free", 10), ("pro", 100), ("enterprise", 1000)];
    for (tier, limit) in tiers {
        let result = client
            .consume(&format!("user-tier-{}", tier), limit, Duration::from_secs(60))
            .await?;
        let status = if result.ok { "✓ OK" } else { "✗ LIMITED" };
        println!("{:12}: {} ({} remaining)", tier.to_uppercase(), status, result.remaining);
    }

    // Example 3: Health check
    println!("\n3. Server Health");
    println!("{}", "-".repeat(60));

    let health = client.health().await?;
    println!("Status: {}", health.status);
    println!("Node: {}", health.node);
    println!("Peers: {}", health.peers);

    println!("\n{}", "=".repeat(60));
    println!("Example complete!");
    Ok(())
}
