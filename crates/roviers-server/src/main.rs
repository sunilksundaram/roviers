use clap::Parser;
use roviers_core::RoviersEngine;
use roviers_server::{create_app, spawn_worker_pool};
use std::net::SocketAddr;
use std::sync::Arc;

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Roviers: High-throughput Broker & Outcome Vault Server"
)]
struct Args {
    #[arg(short, long, default_value = "127.0.0.1:8080")]
    bind: String,

    #[arg(short, long, default_value = "./roviers_data")]
    data_dir: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    tracing::info!("Initializing Roviers Broker engine at {}...", args.data_dir);
    let engine = Arc::new(RoviersEngine::new(&args.data_dir)?);

    // Spawn the background worker pool to demonstrate async task execution
    spawn_worker_pool(Arc::clone(&engine));

    let app = create_app(engine);

    let addr: SocketAddr = args.bind.parse()?;
    tracing::info!("🏛️ Roviers Broker listening on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
