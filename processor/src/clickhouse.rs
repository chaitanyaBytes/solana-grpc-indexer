use anyhow::{Ok, Result};
use clickhouse::Client;
use tracing::info;

use crate::clickhouse_types::{ClickHouseAccount, ClickHouseSlot, ClickHouseTransaction};

pub struct ClickhouseClient {
    pub client: Client,
}

impl ClickhouseClient {
    pub async fn new(url: &str, database: &str) -> Result<Self> {
        let client = Client::default().with_url(url).with_database(database);

        let clichouse_client = Self { client };

        clichouse_client.init_tables().await?;

        Ok(clichouse_client)
    }

    async fn init_tables(&self) -> Result<()> {
        // Transactions table
        self.client
            .query(
                r#"
                CREATE TABLE IF NOT EXISTS transactions (
                    signature String,
                    slot UInt64,
                    is_vote UInt8,
                    index UInt64,
                    success UInt8,
                    fee Nullable(UInt64),
                    compute_units_consumed Nullable(UInt64),
                    timestamp DateTime64(3),
                    pre_balances String,
                    post_balances String,
                    log_messages String,
                    account_keys String,
                    instructions String
                ) ENGINE = MergeTree()
                ORDER BY (slot, index)
                PARTITION BY toYYYYMM(timestamp)
            "#,
            )
            .execute()
            .await?;

        // Accounts table
        self.client
            .query(
                r#"
                CREATE TABLE IF NOT EXISTS accounts (
                    pubkey String,
                    lamports UInt64,
                    owner String,
                    executable UInt8,
                    rent_epoch UInt64,
                    data String,
                    write_version UInt64,
                    txn_signature Nullable(String),
                    timestamp DateTime64(3)
                ) ENGINE = MergeTree()
                ORDER BY (pubkey, write_version)
                PARTITION BY toYYYYMM(timestamp)
            "#,
            )
            .execute()
            .await?;

        // Slots table
        self.client
            .query(
                r#"
                CREATE TABLE IF NOT EXISTS slots (
                    slot UInt64,
                    timestamp DateTime64(3)
                ) ENGINE = MergeTree()
                ORDER BY slot
            "#,
            )
            .execute()
            .await?;

        info!("ClickHouse tables initialized");
        Ok(())
    }

    pub async fn insert_transaction(&self, tx: &ClickHouseTransaction) -> Result<()> {
        let mut inserter = self
            .client
            .insert::<ClickHouseTransaction>("transactions")
            .await?;

        inserter.write(&tx).await?;
        inserter.end().await?;

        Ok(())
    }

    pub async fn batch_insert_transactions(&self, txs: &[ClickHouseTransaction]) -> Result<()> {
        if txs.is_empty() {
            return Ok(());
        }

        let mut inserter = self
            .client
            .insert::<ClickHouseTransaction>("transactions")
            .await?;

        // Write all items to the inserter
        for tx in txs {
            inserter.write(tx).await?;
        }

        // Finalize the batch
        inserter.end().await?;

        Ok(())
    }

    pub async fn insert_account(&self, account: &ClickHouseAccount) -> Result<()> {
        let mut inserter = self.client.insert::<ClickHouseAccount>("accounts").await?;

        inserter.write(account).await?;
        inserter.end().await?;

        Ok(())
    }

    pub async fn batch_insert_accounts(&self, accounts: &[ClickHouseAccount]) -> Result<()> {
        if accounts.is_empty() {
            return Ok(());
        }

        let mut inserter = self.client.insert::<ClickHouseAccount>("accounts").await?;

        for account in accounts {
            inserter.write(account).await?;
        }

        inserter.end().await?;

        Ok(())
    }

    pub async fn insert_slot(&self, slot: &ClickHouseSlot) -> Result<()> {
        let mut inserter = self.client.insert::<ClickHouseSlot>("slots").await?;

        inserter.write(slot).await?;
        inserter.end().await?;

        Ok(())
    }

    pub async fn batch_insert_slots(&self, slots: &[ClickHouseSlot]) -> Result<()> {
        if slots.is_empty() {
            return Ok(());
        }

        let mut inserter = self.client.insert::<ClickHouseSlot>("slots").await?;

        for slot in slots {
            inserter.write(slot).await?;
        }

        inserter.end().await?;

        Ok(())
    }
}
