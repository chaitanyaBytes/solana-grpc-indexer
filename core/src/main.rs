mod config;
use std::time::Duration;

use crate::config::Config;
use anyhow::Result;
use ingest::{types::IndexEvent, yellowstone_client::YellowstoneClient};

use processor::worker::Processor;
use tokio::sync::mpsc::{Receiver, Sender};
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
    let clickhouse_url = config.clickhouse_url;
    let clickhouse_db = config.clickhouse_db;
    let clickhouse_user = config.clickhouse_user;
    let clickhouse_password = config.clickhouse_password;

    let (event_tx, event_rx) = tokio::sync::mpsc::channel::<IndexEvent>(10_000);

    tokio::spawn(async move {
        if let Err(e) = run_yellowstone_with_reconnect(endpoint, token, event_tx).await {
            error!("stream error: {}", e);
        }
    });

    tokio::spawn(async move {
        if let Err(e) = run_processor(
            event_rx,
            clickhouse_url,
            clickhouse_user,
            clickhouse_password,
            clickhouse_db,
        )
        .await
        {
            error!("Processor error: {}", e);
        }
    });

    futures::future::pending::<()>().await; // keeps main alive without using cpu
    Ok(())
}

async fn run_yellowstone_with_reconnect(
    endpoint: String,
    token: Option<String>,
    event_tx: Sender<IndexEvent>,
) -> Result<()> {
    const MAX_BACKOFF: u64 = 60;
    let mut backoff = 1u64;

    loop {
        match YellowstoneClient::connect_and_run(&endpoint, &token, &event_tx).await {
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

pub async fn run_processor(
    mut event_rx: Receiver<IndexEvent>,
    clickhouse_url: String,
    clickhouse_user: String,
    clickhouse_password: String,
    clickhouse_db: String,
) -> anyhow::Result<()> {
    let mut processor = Processor::new(
        &clickhouse_url,
        &clickhouse_user,
        &clickhouse_password,
        &clickhouse_db,
    )
    .await
    .expect("Clickhouse init failed");

    // Create periodic flush interval
    let flush_interval = processor.flush_interval;
    let mut flush_timer = tokio::time::interval(flush_interval);
    flush_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        /*
            Wait until either:
            a new event arrives
            OR the flush timer fires
            then run the matching block.
        */
        tokio::select! {
            // Process events
            event = event_rx.recv() => {
                match event {
                    Some(event) => {
                        if let Err(e) = processor.process_event(event).await {
                            error!("Processing error: {}", e);
                        }
                    }
                    None => {
                        warn!("Event channel closed, flushing and exiting");
                        break;
                    }
                }
            }

            // Periodic flush
            _ = flush_timer.tick() => {
                if let Err(e) = processor.flush_all().await {
                    error!("Periodic flush error: {}", e);
                }
            }
        }
    }

    processor.flush_all().await?;

    Ok(())
}
