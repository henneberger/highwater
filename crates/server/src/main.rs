#[tokio::main]
async fn main() -> anyhow::Result<()> {
    highwater_server::run().await
}
