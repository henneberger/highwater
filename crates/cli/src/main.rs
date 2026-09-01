mod api;
mod arguments;
mod commands;

#[tokio::main]
async fn main() {
    if let Err(error) = commands::run(std::env::args().skip(1).collect()).await {
        eprintln!("error: {error:#}");
        std::process::exit(1);
    }
}
