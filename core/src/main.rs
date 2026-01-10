mod config;
use bs58;
use futures::{SinkExt, StreamExt};
use std::collections::HashMap;
use tokio::time::{Duration, sleep};
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient};
use yellowstone_grpc_proto::prelude::*;

use crate::config::Config;

fn setup_rustls() {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install crypto provider");
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    setup_rustls();

    dotenv::dotenv().ok();

    println!("Starting stream of solana data");

    let config = Config::load_config()?;

    let endpoint = config.yellowstone_grpc_endpoint;
    let token = config.yellowstone_grpc_token;

    let builder = GeyserGrpcClient::build_from_shared(endpoint.to_string())?
        .tls_config(ClientTlsConfig::new().with_native_roots())?
        .x_token(Some(token))?;

    let mut client = builder.connect().await?;

    // USDC mint account
    let usdc_mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";

    let mut accounts = HashMap::new();
    accounts.insert(
        "usdc_mint".to_string(),
        SubscribeRequestFilterAccounts {
            nonempty_txn_signature: None,
            account: vec![usdc_mint.to_string()],
            owner: vec![],
            filters: vec![],
        },
    );

    let (mut tx, mut rx) = client.subscribe().await?;
    let subscribe_request = SubscribeRequest {
        accounts,
        transactions: HashMap::new(),
        blocks: HashMap::new(),
        blocks_meta: HashMap::new(),
        entry: HashMap::new(),
        commitment: Some(CommitmentLevel::Confirmed as i32),
        accounts_data_slice: vec![],
        ping: None,
        from_slot: None,
        slots: HashMap::new(),
        transactions_status: HashMap::new(),
    };

    tx.send(subscribe_request).await?;

    println!("Connected! Monitoring USDC mint account...");

    while let Some(message) = rx.next().await {
        match message {
            Ok(msg) => {
                if let Some(account) = msg.update_oneof {
                    match account {
                        subscribe_update::UpdateOneof::Account(account_update) => {
                            println!("\n📊 Account Update:");
                            println!(
                                "  Account: {}",
                                account_update
                                    .account
                                    .as_ref()
                                    .map(|a| bs58::encode(&a.pubkey).into_string())
                                    .unwrap_or("N/A".to_string())
                            );
                            println!(
                                "  Owner: {}",
                                account_update
                                    .account
                                    .as_ref()
                                    .map(|a| bs58::encode(&a.owner).into_string())
                                    .unwrap_or("N/A".to_string())
                            );
                            println!(
                                "  Lamports: {}",
                                account_update
                                    .account
                                    .as_ref()
                                    .map(|a| a.lamports)
                                    .unwrap_or(0)
                            );
                            println!("  Slot: {}", account_update.slot);
                            println!(
                                "  Data Length: {}",
                                account_update
                                    .account
                                    .as_ref()
                                    .map(|a| a.data.len())
                                    .unwrap_or(0)
                            );
                        }
                        _ => {} // Handle other update types as needed
                    }
                }
            }
            Err(error) => {
                eprintln!(" Stream error: {}", error);
                sleep(Duration::from_secs(1)).await;
            }
        }
    }

    Ok(())
}
