use anyhow::{Ok, Result};
use clickhouse::{Client, RowOwned, RowRead};
use serde::Deserialize;
use tracing::info;

use crate::clickhouse_types::{ClickHouseAccount, ClickHouseSlot, ClickHouseTransaction};

pub struct ClickhouseClient {
    pub client: Client,
}

impl ClickhouseClient {
    pub async fn new(
        clickhouse_url: &str,
        clickhouse_user: &str,
        clickhouse_password: &str,
        clickhouse_db: &str,
    ) -> Result<Self> {
        let client = Client::default()
            .with_url(clickhouse_url)
            .with_database(clickhouse_db)
            .with_user(clickhouse_user)
            .with_password(clickhouse_password);

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
                tx_index UInt64,
                success UInt8,
                fee Nullable(UInt64),
                compute_units_consumed Nullable(UInt64),
                timestamp DateTime64(3),
                pre_balances String,
                post_balances String,
                log_messages String,
                account_keys String,
                instructions String
                )
                ENGINE = MergeTree()
                PARTITION BY toYYYYMM(toDateTime(timestamp))
                ORDER BY (slot, tx_index);
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
                PARTITION BY toYYYYMM(toDateTime(timestamp))
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

    /// Execute a SELECT query and return results as typed JSON
    pub async fn query_all_typed<T>(&self, query: &str) -> Result<serde_json::Value>
    where
        T: RowOwned + RowRead + serde::Serialize,
    {
        let rows: Vec<T> = self.client.query(query).fetch_all().await?;
        Ok(serde_json::json!(rows))
    }

    /// Execute a SELECT query and return results as raw JSON
    pub async fn query_json_raw<T>(&self, query: &str) -> Result<String> {
        let chunks = self.client.query(query).fetch_all::<String>().await?;
        Ok(chunks.join(""))
    }

    /// Execute a query that returns a single value
    pub async fn query_single<T>(&self, query: &str) -> Result<Option<T>>
    where
        T: RowOwned + for<'a> Deserialize<'a>,
    {
        let mut cursor = self.client.query(query).fetch::<T>()?;
        Ok(cursor.next().await?)
    }
}
