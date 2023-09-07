use mini_redis::{client, Result};

#[tokio::main]
async fn main() -> Result<()> {
    // Open a connection to the mini-redis address.
    let mut client = client::connect("127.0.0.1:6379").await?;

    // Set the key "hello" with the value "world"
    client.set("hello", "world".into()).await?;

    // Get the key "hello"
    let result = client.get("hello").await?;

    println!("Got value from the server; result={:?}", result);

    Ok(())
}
