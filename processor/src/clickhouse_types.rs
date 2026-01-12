use clickhouse::Row;
use serde::{Deserialize, Serialize};

#[derive(Row, Debug, Clone, Serialize, Deserialize)]
pub struct ClickHouseTransaction {
    pub signature: String,
    pub slot: u64,
    pub is_vote: bool,
    pub tx_index: u64,
    pub success: bool,
    pub fee: Option<u64>,
    pub compute_units_consumed: Option<u64>,
    pub timestamp: i64,
    // JSON fields for complex data
    pub pre_balances: String,  // JSON array
    pub post_balances: String, // JSON array
    pub log_messages: String,  // JSON array
    pub account_keys: String,  // JSON array
    pub instructions: String,  // JSON array
}

#[derive(Row, Debug, Clone, Serialize, Deserialize)]
pub struct ClickHouseAccount {
    pub pubkey: String,
    pub lamports: u64,
    pub owner: String,
    pub executable: bool,
    pub rent_epoch: u64,
    pub data: String, // Base64 encoded
    pub write_version: u64,
    pub txn_signature: Option<String>,
    pub timestamp: i64,
}

#[derive(Row, Debug, Clone, Serialize, Deserialize)]
pub struct ClickHouseSlot {
    pub slot: u64,
    pub timestamp: i64,
}
