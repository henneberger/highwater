#[tokio::main]
async fn main() -> anyhow::Result<()> {
    temporal_code_server::run().await
}
