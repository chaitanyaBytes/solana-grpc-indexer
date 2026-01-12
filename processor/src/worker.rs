use anyhow::Result;
use ingest::types::IndexEvent;
use std::time::{self, Duration};
use tracing::{error, info};

use crate::{
    clickhouse::ClickhouseClient,
    clickhouse_types::{ClickHouseAccount, ClickHouseSlot, ClickHouseTransaction},
    transformer::Transformer,
};

pub struct Processor {
    clickhouse: ClickhouseClient,
    tx_buffer: Vec<ClickHouseTransaction>,
    account_buffer: Vec<ClickHouseAccount>,
    slot_buffer: Vec<ClickHouseSlot>,
    batch_size: usize,
    pub flush_interval: Duration,
}

impl Processor {
    pub async fn new(
        clickhouse_url: &str,
        clickhouse_user: &str,
        clickhouse_password: &str,
        clickhouse_db: &str,
    ) -> Result<Self> {
        let clickhouse = ClickhouseClient::new(
            clickhouse_url,
            clickhouse_user,
            clickhouse_password,
            clickhouse_db,
        )
        .await?;

        Ok(Self {
            clickhouse,
            tx_buffer: Vec::with_capacity(1000),
            account_buffer: Vec::with_capacity(100),
            slot_buffer: Vec::with_capacity(100),
            batch_size: 1000,
            flush_interval: Duration::from_secs(5),
        })
    }

    pub async fn process_event(&mut self, event: IndexEvent) -> Result<()> {
        match event {
            IndexEvent::Account(account) => {
                let ch_account = Transformer::transform_account(&account)?;
                self.account_buffer.push(ch_account);

                if self.account_buffer.len() >= self.batch_size {
                    self.flush_accounts().await?;
                }
            }
            IndexEvent::Transaction(transaction) => {
                let ch_tx = Transformer::transform_transaction(&transaction)?;
                self.tx_buffer.push(ch_tx);

                if self.tx_buffer.len() >= self.batch_size {
                    self.flush_transactions().await?;
                }
            }
            IndexEvent::Slot(slot) => {
                let ch_slot = Transformer::transform_slot(slot);
                self.slot_buffer.push(ch_slot);

                if self.slot_buffer.len() >= self.batch_size {
                    self.flush_slots().await?;
                }
            }
            IndexEvent::Block(_block) => {
                // handle blocks if needed
            }
        };

        Ok(())
    }

    async fn flush_accounts(&mut self) -> Result<()> {
        if self.account_buffer.is_empty() {
            return Ok(());
        }

        let count = self.account_buffer.len();
        let start_time = time::Instant::now();

        match self
            .clickhouse
            .batch_insert_accounts(&self.account_buffer)
            .await
        {
            Ok(_) => {
                let duration = start_time.elapsed().as_millis();
                info!(
                    "Inserted {} accounts to clickhouse db in {} millis",
                    count, duration
                );
                self.account_buffer.clear();
            }
            Err(e) => {
                error!("Failed to insert accounts: {}", e);
                return Err(e);
            }
        }

        Ok(())
    }

    async fn flush_transactions(&mut self) -> Result<()> {
        if self.tx_buffer.is_empty() {
            return Ok(());
        }

        let count = self.tx_buffer.len();
        let start_time = time::Instant::now();

        match self
            .clickhouse
            .batch_insert_transactions(&self.tx_buffer)
            .await
        {
            Ok(_) => {
                let duration = start_time.elapsed().as_millis();
                info!(
                    "Inserted {} transactions to clickhouse db in {} millis",
                    count, duration
                );
                self.tx_buffer.clear();
            }
            Err(e) => {
                error!("Failed to insert transactions: {}", e);
                return Err(e);
            }
        }

        Ok(())
    }

    async fn flush_slots(&mut self) -> Result<()> {
        if self.slot_buffer.is_empty() {
            return Ok(());
        }

        let count = self.slot_buffer.len();
        match self.clickhouse.batch_insert_slots(&self.slot_buffer).await {
            Ok(_) => {
                info!("Inserted {} slots to ClickHouse", count);
                self.slot_buffer.clear();
            }
            Err(e) => {
                error!("Failed to insert slots: {}", e);
                return Err(e);
            }
        }

        Ok(())
    }

    pub async fn flush_all(&mut self) -> Result<()> {
        self.flush_transactions().await?;
        self.flush_accounts().await?;
        self.flush_slots().await?;
        Ok(())
    }
}
