use tollmeshcache::{Client, ClientConfig};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("TollMeshCache - Replay Protection Example (Rust)");
    println!("{}", "=".repeat(60));

    let config = ClientConfig::new()
        .host("localhost")
        .port(8080);

    let client = Client::new(config)?;

    println!("\n1. First-time nonce");
    println!("{}", "-".repeat(60));

    let nonce = "request-nonce-abc123";
    let result = client.seen(nonce, Duration::from_secs(300)).await?;
    println!("Nonce {:?}: seen = {} ({})", nonce, result.seen, if result.seen { "REPLAY!" } else { "first time, OK" });

    println!("\n2. Replaying the same nonce");
    println!("{}", "-".repeat(60));

    let result = client.seen(nonce, Duration::from_secs(300)).await?;
    println!("Nonce {:?}: seen = {} ({})", nonce, result.seen, if result.seen { "REPLAY!" } else { "first time, OK" });

    println!("\n{}", "=".repeat(60));
    println!("Example complete!");
    Ok(())
}
