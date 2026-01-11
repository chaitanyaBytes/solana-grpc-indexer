use anyhow::{Ok, Result};
use chrono::Utc;
use ingest::types::{SolanaAccount, SolanaTransaction};

use crate::clickhouse_types::{ClickHouseAccount, ClickHouseSlot, ClickHouseTransaction};

pub struct Transformer;

impl Transformer {
    pub fn transform_account(account: &SolanaAccount) -> Result<ClickHouseAccount> {
        Ok(ClickHouseAccount {
            pubkey: account.pubkey.clone(),
            lamports: account.lamports,
            owner: account.owner.clone(),
            executable: account.executable,
            rent_epoch: account.rent_epoch,
            data: account.data.clone(),
            write_version: account.write_version,
            txn_signature: account.txn_signature.clone(),
            timestamp: account.timestamp,
        })
    }

    pub fn transform_transaction(tx: &SolanaTransaction) -> Result<ClickHouseTransaction> {
        Ok(ClickHouseTransaction {
            signature: tx.signature.clone(),
            slot: tx.slot,
            is_vote: tx.is_vote,
            index: tx.index,
            success: tx.success,
            fee: tx.fee,
            compute_units_consumed: tx.compute_units_consumed,
            timestamp: Utc::now(),
            pre_balances: serde_json::to_string(&tx.pre_balances)?,
            post_balances: serde_json::to_string(&tx.post_balances)?,
            log_messages: serde_json::to_string(&tx.log_messages)?,
            account_keys: serde_json::to_string(&tx.account_keys)?,
            instructions: serde_json::to_string(&tx.instructions)?,
        })
    }

    pub fn transform_slot(slot: u64) -> ClickHouseSlot {
        ClickHouseSlot {
            slot,
            timestamp: Utc::now(),
        }
    }
}
