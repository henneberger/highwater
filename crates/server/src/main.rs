#[tokio::main(worker_threads = 4)]
async fn main() -> anyhow::Result<()> {
    highwater_server::run().await
}
