mod config;
use std::time::Duration;

use crate::config::Config;
use ingest::yellowstone_client::YellowstoneClient;

use tracing::{error, info, warn};

fn setup_logging() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
}

fn setup_rustls() {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install crypto provider");
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    setup_rustls();
    setup_logging();

    dotenv::dotenv().ok();

    let config = Config::load_config()?;

    let endpoint = config.yellowstone_grpc_endpoint;
    let token = config.yellowstone_grpc_token;

    tokio::spawn(run_yellowstone_with_reconnect(endpoint, token));

    futures::future::pending::<()>().await; // keeps main alive without using cpu
    Ok(())
}

async fn run_yellowstone_with_reconnect(endpoint: String, token: Option<String>) {
    const MAX_BACKOFF: u64 = 60;
    let mut backoff = 1u64;

    loop {
        match YellowstoneClient::connect_and_run(&endpoint, &token).await {
            Ok(_) => {
                warn!("Stream ended normally, reconnecting…");
            }
            Err(e) => {
                error!("Stream error: {:?}", e);
            }
        }

        info!("Reconnecting in {}s", backoff);
        tokio::time::sleep(Duration::from_secs(backoff)).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}
