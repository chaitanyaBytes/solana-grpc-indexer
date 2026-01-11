use anyhow::Result;
use base64::{Engine as _, engine::general_purpose};
use chrono::Utc;
use futures::{Sink, SinkExt, Stream, StreamExt, channel::mpsc};
use tonic::Status;
use tracing::{error, info, warn};
use yellowstone_grpc_client::{
    ClientTlsConfig, GeyserGrpcBuilderError, GeyserGrpcClient, GeyserGrpcClientResult, Interceptor,
};
use yellowstone_grpc_proto::geyser::{
    SubscribeRequest, SubscribeUpdate, SubscribeUpdateAccount, SubscribeUpdateSlot,
    SubscribeUpdateTransaction, subscribe_update,
};

use crate::{
    subscriptions::Subscriptions,
    types::{SolanaAccount, SolanaTransaction, TransactionInstruction},
};

pub struct YellowstoneClient {}

impl YellowstoneClient {
    pub async fn new(
        endpoint: &str,
        token: &Option<String>,
    ) -> Result<GeyserGrpcClient<impl Interceptor + Clone>, GeyserGrpcBuilderError> {
        let builder = GeyserGrpcClient::build_from_shared(endpoint.to_string())?
            .tls_config(ClientTlsConfig::new().with_native_roots())?
            .x_token(token.clone())?;

        let client = builder.connect().await?;

        Ok(client)
    }

    pub async fn subscribe(
        client: &mut GeyserGrpcClient<impl Interceptor + Clone>,
    ) -> GeyserGrpcClientResult<(
        impl Sink<SubscribeRequest, Error = mpsc::SendError>,
        impl Stream<Item = Result<SubscribeUpdate, Status>>,
    )> {
        client.subscribe().await
    }

    pub async fn handle_grpc_stream(
        mut stream: impl Stream<Item = Result<SubscribeUpdate, Status>> + Unpin,
    ) -> Result<()> {
        while let Some(message) = stream.next().await {
            match message {
                Ok(update) => {
                    Self::process_update(update).await?;
                }
                Err(error) => {
                    error!("Stream Error: {}", error);
                    return Err(error.into());
                }
            }
        }

        warn!("gRPC stream closed by server");
        Ok(())
    }

    pub async fn connect_and_run(endpoint: &str, token: &Option<String>) -> anyhow::Result<()> {
        let mut yellowstone_client = Self::new(endpoint, token).await?;

        let (mut yellowstone_tx, yellowstone_rx) = Self::subscribe(&mut yellowstone_client).await?;

        let subscriptions = Subscriptions::create_subscriptions();

        yellowstone_tx.send(subscriptions).await?;

        info!("Connected!...");
        info!("Subscribed to Dexs. Starting data stream...");

        Self::handle_grpc_stream(yellowstone_rx).await?;

        Ok(())
    }

    pub async fn process_update(update: SubscribeUpdate) -> Result<()> {
        match update.update_oneof {
            Some(subscribe_update::UpdateOneof::Account(account_update)) => {
                Self::handle_account_update(account_update).await?;
            }
            Some(subscribe_update::UpdateOneof::Transaction(transaction_update)) => {
                Self::handle_transaction_update(transaction_update).await?;
            }
            Some(subscribe_update::UpdateOneof::Slot(slot_update)) => {
                Self::handle_slot_update(slot_update).await?;
            }
            _ => {}
        }
        Ok(())
    }

    pub async fn handle_account_update(account_update: SubscribeUpdateAccount) -> Result<()> {
        if let Some(account) = Self::into_solana_account(account_update) {
            info!(
                "Account: pubkey={}, lamports={}, owner={}, executable={}",
                account.pubkey, account.lamports, account.owner, account.executable
            );
        }
        Ok(())
    }

    pub async fn handle_transaction_update(
        transaction_update: SubscribeUpdateTransaction,
    ) -> Result<()> {
        if let Some(transaction) = Self::into_solana_transaction(transaction_update) {
            info!(
                "Transaction: signature={}, slot={}, success={}",
                transaction.signature, transaction.slot, transaction.success
            );
        }
        Ok(())
    }

    pub async fn handle_slot_update(slot_update: SubscribeUpdateSlot) -> Result<()> {
        info!("Slot: {:?}", slot_update.slot);

        Ok(())
    }

    fn into_solana_account(account_data: SubscribeUpdateAccount) -> Option<SolanaAccount> {
        if let Some(account_info) = account_data.account {
            let pubkey = bs58::encode(account_info.pubkey).into_string();
            let owner = bs58::encode(account_info.owner).into_string();
            let data = general_purpose::STANDARD.encode(&account_info.data);
            let tx_sig = account_info
                .txn_signature
                .map(|sig| bs58::encode(sig).into_string());

            Some(SolanaAccount {
                pubkey: pubkey,
                lamports: account_info.lamports,
                owner: owner,
                executable: account_info.executable,
                rent_epoch: account_info.rent_epoch,
                data: data,
                write_version: account_info.write_version,
                txn_signature: tx_sig,
                timestamp: Utc::now(),
            })
        } else {
            None
        }
    }

    fn into_solana_transaction(
        transaction_update: SubscribeUpdateTransaction,
    ) -> Option<SolanaTransaction> {
        if let Some(transaction_info) = transaction_update.transaction {
            let signature = bs58::encode(transaction_info.signature).into_string();
            let slot = transaction_update.slot;
            let is_vote = transaction_info.is_vote;
            let index = transaction_info.index;

            let (
                success,
                fee,
                pre_balances,
                post_balances,
                compute_units_consumed,
                log_messages,
                instructions,
                account_keys,
            ) = if let (Some(transaction), Some(meta)) =
                (transaction_info.transaction, transaction_info.meta)
            {
                let success = meta.err.is_none();
                let fee = Some(meta.fee);
                let pre_balances = meta.pre_balances;
                let post_balances = meta.post_balances;
                let compute_units_consumed = meta.compute_units_consumed;
                let log_messages = meta.log_messages;

                let mut instructions = Vec::new();

                if let Some(message) = transaction.message.as_ref() {
                    for instruction in &message.instructions {
                        let program_id_index = instruction.program_id_index as usize;
                        let program_id = if program_id_index < message.account_keys.len() {
                            bs58::encode(&message.account_keys[program_id_index]).into_string()
                        } else {
                            String::new()
                        };

                        let accounts: Vec<String> = instruction
                            .accounts
                            .iter()
                            .filter_map(|&id| {
                                message
                                    .account_keys
                                    .get(id as usize)
                                    .map(|key| bs58::encode(key).into_string())
                            })
                            .collect();

                        instructions.push(TransactionInstruction {
                            program_id,
                            accounts,
                            data: general_purpose::STANDARD.encode(&instruction.data),
                        });
                    }
                }

                let account_keys: Vec<String> = if let Some(message) = transaction.message.as_ref()
                {
                    message
                        .account_keys
                        .iter()
                        .map(|key| bs58::encode(key).into_string())
                        .collect()
                } else {
                    Vec::new()
                };

                (
                    success,
                    fee,
                    pre_balances,
                    post_balances,
                    compute_units_consumed,
                    log_messages,
                    instructions,
                    account_keys,
                )
            } else {
                (
                    false,
                    None,
                    Vec::new(),
                    Vec::new(),
                    None,
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                )
            };

            Some(SolanaTransaction {
                signature,
                slot,
                is_vote,
                index,
                success,
                fee,
                pre_balances,
                post_balances,
                compute_units_consumed,
                instructions,
                log_messages,
                account_keys,
            })
        } else {
            None
        }
    }
}
