mod config;
use crate::config::Config;
use futures::SinkExt;
use ingest::{subscriptions::Subscriptions, yellowstone_client::YellowstoneClient};

use tracing::info;

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

    let mut yellowstone_client = YellowstoneClient::new(endpoint, token).await?;

    let (mut yellowstone_tx, yellowstone_rx) =
        YellowstoneClient::subscribe(&mut yellowstone_client).await?;

    let subscription_request = Subscriptions::create_subscriptions();

    yellowstone_tx.send(subscription_request).await?;

    info!("Connected!...");
    info!("Subscribed to Dexs. Starting data stream...");

    YellowstoneClient::handle_grpc_stream(yellowstone_rx).await?;

    Ok(())
}
