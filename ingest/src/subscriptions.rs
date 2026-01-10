use std::collections::HashMap;

use yellowstone_grpc_proto::geyser::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterAccounts,
    SubscribeRequestFilterTransactions,
};

pub struct Subscriptions;

impl Subscriptions {
    pub fn create_subscriptions() -> SubscribeRequest {
        let mut accounts = HashMap::new();
        accounts.insert(
            "dexs_accounts".to_string(),
            SubscribeRequestFilterAccounts {
                account: vec![
                    "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4".to_string(), // Jupiter v6
                    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8".to_string(), // Raydium v5
                    "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG".to_string(), // Meteora DAMM v2
                    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc".to_string(), // Orca
                ],
                nonempty_txn_signature: None,
                owner: vec![],
                filters: vec![],
            },
        );

        let mut transactions = HashMap::new();
        transactions.insert(
            "dexs_transactions".to_string(),
            SubscribeRequestFilterTransactions {
                account_include: vec![
                    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc".to_string(), // Orca
                    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8".to_string(), // Raydium v5
                    "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG".to_string(), // Meteora DAMM v2
                    "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4".to_string(), // Jupiter v6
                ],
                account_exclude: vec![],
                account_required: vec![],
                vote: Some(false),
                failed: Some(false),
                signature: None,
            },
        );

        SubscribeRequest {
            accounts,
            transactions,
            slots: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            transactions_status: HashMap::new(),
            entry: HashMap::new(),
            commitment: Some(CommitmentLevel::Confirmed as i32),
            accounts_data_slice: vec![],
            ping: None,
            from_slot: None,
        }
    }
}
