use tollmeshcache::{Client, ClientConfig};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("TollMeshCache - Caching Example (Rust)");
    println!("{}", "=".repeat(60));

    let config = ClientConfig::new()
        .host("localhost")
        .port(8080);

    let client = Client::new(config)?;

    println!("\n1. Cache-aside: miss, then set, then hit");
    println!("{}", "-".repeat(60));

    let namespace = "users";
    let key = "user-123";

    let miss = client.cache_get(namespace, key).await?;
    println!("Before set: exists = {}", miss.exists);

    client
        .cache_set(namespace, key, "{\"name\":\"alice\"}", Some(Duration::from_secs(3600)))
        .await?;
    println!("Set {}/{}", namespace, key);

    let hit = client.cache_get(namespace, key).await?;
    println!("After set: exists = {}, value = {:?}", hit.exists, hit.value);

    println!("\n{}", "=".repeat(60));
    println!("Example complete!");
    Ok(())
}
