use std::collections::HashMap;

use anyhow::Result;
use chrono::{DateTime, Utc};
use clickhouse::Row;
use serde::{Deserialize, Serialize};

use crate::ClickhouseClient;

pub struct QueryService {
    client: ClickhouseClient,
}

impl QueryService {
    pub fn new(client: ClickhouseClient) -> Self {
        Self { client }
    }

    fn build_where_clause(&self, filters: &TransactionFilters) -> String {
        let mut conditions = Vec::new();

        if let Some(period) = &filters.period {
            conditions.push(self.period_to_sql(period));
        }

        if let Some(success) = filters.success {
            conditions.push(format!("success = {}", if success { 1 } else { 0 }));
        }

        if let Some(min_fee) = filters.min_fee {
            conditions.push(format!("fee >= {}", min_fee));
        }

        if let Some(max_fee) = filters.max_fee {
            conditions.push(format!("fee <= {}", max_fee));
        }

        if let Some((start_slot, end_slot)) = filters.slot_range {
            conditions.push(format!("slot >= {} AND slot <= {}", start_slot, end_slot));
        }

        if conditions.is_empty() {
            "1=1".to_string()
        } else {
            conditions.join(" AND ")
        }
    }

    fn period_to_sql(&self, period: &TimePeriod) -> String {
        match period {
            TimePeriod::LastHour => "timestamp >= now() - INTERVAL 1 HOUR".to_string(),
            TimePeriod::Last24Hours => "timestamp >= now() - INTERVAL 24 HOUR".to_string(),
            TimePeriod::Last7Days => "timestamp >= now() - INTERVAL 7 DAY".to_string(),
            TimePeriod::Last30Days => "timestamp >= now() - INTERVAL 30 DAY".to_string(),
            TimePeriod::Custom { start, end } => {
                format!(
                    "timestamp >= {} AND timestamp <= {}",
                    start.timestamp_millis(),
                    end.timestamp_millis()
                )
            }
        }
    }

    // ========== Transaction Queries ==========

    /// Get transaction count with optional filters
    pub async fn count_transactions(&self, filters: TransactionFilters) -> Result<u64> {
        let where_clause = self.build_where_clause(&filters);

        let query = format!(
            "SELECT count(*) as total FROM transactions WHERE {}",
            where_clause
        );

        #[derive(Row, Deserialize)]
        struct CountResult {
            total: u64,
        }

        let result = self.client.query_single::<CountResult>(&query).await?;
        Ok(result.map(|r| r.total).unwrap_or(0))
    }

    /// Get success/failure rate
    pub async fn get_success_rate(&self, period: TimePeriod) -> Result<f64> {
        let period_clause = self.period_to_sql(&period);

        let query = format!(
            r#"
        SELECT
            count(*) as total,
            sum(success) as successful
        FROM transactions
        WHERE {}
        "#,
            period_clause
        );

        #[derive(Row, Deserialize)]
        struct SuccessRateResult {
            total: u64,
            successful: u64,
        }

        let result = self
            .client
            .query_single::<SuccessRateResult>(&query)
            .await?;

        match result {
            Some(r) if r.total > 0 => Ok((r.successful as f64) / (r.total as f64) * 100.0),
            Some(_) => Ok(0.0),
            None => Ok(0.0),
        }
    }

    /// Get fee statistics (min, max, avg, median)
    pub async fn get_fee_stats(&self, period: TimePeriod) -> Result<FeeStats> {
        let period_clause = self.period_to_sql(&period);

        let query = format!(
            r#"
        SELECT 
            min(fee) as min_fee,
            max(fee) as max_fee,
            avg(fee) as avg_fee,
            quantile(0.5)(fee) as median_fee,
            sum(fee) as total_fees,
            count(*) as tx_count
        FROM transactions
        WHERE {} AND fee IS NOT NULL
        "#,
            period_clause
        );

        #[derive(Row, Deserialize)]
        struct FeeStatsResult {
            min_fee: Option<u64>,
            max_fee: Option<u64>,
            avg_fee: Option<f64>,
            median_fee: Option<f64>,
            total_fees: Option<u64>,
            tx_count: u64,
        }

        let result = self.client.query_single::<FeeStatsResult>(&query).await?;

        match result {
            Some(r) => Ok(FeeStats {
                min: r.min_fee,
                max: r.max_fee,
                average: r.avg_fee,
                median: r.median_fee.map(|v| v as u64),
                total: r.total_fees,
                transaction_count: r.tx_count,
            }),
            None => Ok(FeeStats::default()),
        }
    }

    /// Get total fees collected
    pub async fn get_total_fees(&self, period: TimePeriod) -> Result<u64> {
        let period_clause = self.period_to_sql(&period);

        let query = format!(
            "SELECT sum(fee) as total FROM transactions WHERE {} AND fee IS NOT NULL",
            period_clause
        );

        #[derive(Row, Deserialize)]
        struct TotalResult {
            total: u64,
        }

        let result = self.client.query_single::<TotalResult>(&query).await?;
        Ok(result.map(|r| r.total).unwrap_or(0))
    }

    /// Get transactions per second
    pub async fn get_tps(&self, period: TimePeriod) -> Result<f64> {
        let period_clause = self.period_to_sql(&period);

        let query = format!(
            r#"
        SELECT 
            count(*) as tx_count,
            (max(timestamp) - min(timestamp)) / 1000.0 as duration_seconds
        FROM transactions
        WHERE {}
        "#,
            period_clause
        );

        #[derive(Row, Deserialize)]
        struct TpsResult {
            tx_count: u64,
            duration_seconds: f64,
        }

        let result = self.client.query_single::<TpsResult>(&query).await?;

        match result {
            Some(r) if r.duration_seconds > 0.0 => Ok(r.tx_count as f64 / r.duration_seconds),
            _ => Ok(0.0),
        }
    }

    /// Get transaction rate over time
    pub async fn get_tps_timeseries(
        &self,
        period: TimePeriod,
        bucket: TimeBucket,
    ) -> Result<Vec<TpsDataPoint>> {
        let period_clause = self.period_to_sql(&period);
        let bucket_format = match bucket {
            TimeBucket::Minute => "toStartOfMinute(toDateTime(timestamp))",
            TimeBucket::Hour => "toStartOfHour(toDateTime(timestamp))",
            TimeBucket::Day => "toStartOfDay(toDateTime(timestamp))",
            TimeBucket::Week => "toStartOfWeek(toDateTime(timestamp))",
        };

        let query = format!(
            r#"
        SELECT 
            {} as time_bucket,
            count(*) as tx_count
        FROM transactions
        WHERE {}
        GROUP BY time_bucket
        ORDER BY time_bucket
        "#,
            bucket_format, period_clause
        );

        #[derive(Row, Deserialize, Serialize)]
        struct TpsSeriesResult {
            time_bucket: i64,
            tx_count: u64,
        }

        let mut cursor = self
            .client
            .client
            .query(&query)
            .fetch::<TpsSeriesResult>()?;
        let mut results = Vec::new();

        while let Some(row) = cursor.next().await? {
            results.push(TpsDataPoint {
                timestamp: row.time_bucket,
                tps: row.tx_count as f64,
                transaction_count: row.tx_count,
            });
        }

        Ok(results)
    }

    /// Get transactions in slot range
    pub async fn get_transactions_by_slot_range(
        &self,
        start: u64,
        end: u64,
    ) -> Result<Vec<TransactionResult>> {
        let query = format!(
            r#"
            SELECT 
                signature,
                slot,
                timestamp,
                success,
                fee
            FROM transactions
            WHERE slot >= {} AND slot <= {}
            ORDER BY slot, tx_index
            LIMIT 10000
            "#,
            start, end
        );

        #[derive(Serialize, Deserialize, Row)]
        struct TransactionResultRow {
            signature: String,
            slot: u64,
            timestamp: i64,
            success: u8,
            fee: Option<u64>,
        }

        let mut cursor = self
            .client
            .client
            .query(&query)
            .fetch::<TransactionResultRow>()?;
        let mut results = Vec::new();

        while let Some(row) = cursor.next().await? {
            results.push(TransactionResult {
                signature: row.signature,
                slot: row.slot,
                timestamp: DateTime::from_timestamp_millis(row.timestamp).unwrap_or_else(Utc::now),
                success: row.success == 1,
                fee: row.fee,
            });
        }

        Ok(results)
    }

    /// Get slot statistics
    pub async fn get_slot_stats(&self, period: TimePeriod) -> Result<SlotStats> {
        let period_clause = self.period_to_sql(&period);

        let query = format!(
            r#"
            SELECT 
                min(slot) as min_slot,
                max(slot) as max_slot,
                count(DISTINCT slot) as unique_slots,
                count(*) as tx_count
            FROM transactions
            WHERE {}
            "#,
            period_clause
        );

        #[derive(clickhouse::Row, serde::Deserialize)]
        struct SlotStatsResult {
            min_slot: u64,
            max_slot: u64,
            unique_slots: u64,
            tx_count: u64,
        }

        let result = self.client.query_single::<SlotStatsResult>(&query).await?;

        match result {
            Some(r) => Ok(SlotStats {
                min_slot: r.min_slot,
                max_slot: r.max_slot,
                unique_slots: r.unique_slots,
                total_transactions: r.tx_count,
                avg_tx_per_slot: if r.unique_slots > 0 {
                    r.tx_count as f64 / r.unique_slots as f64
                } else {
                    0.0
                },
            }),
            None => Ok(SlotStats::default()),
        }
    }

    /// Get failed transactions
    pub async fn get_failed_transactions(
        &self,
        period: TimePeriod,
        limit: Option<usize>,
    ) -> Result<Vec<TransactionResult>> {
        let period_clause = self.period_to_sql(&period);
        let limit_clause = limit.map(|l| format!("LIMIT {}", l)).unwrap_or_default();

        let query = format!(
            r#"
            SELECT 
                signature,
                slot,
                timestamp,
                success,
                fee
            FROM transactions
            WHERE {} AND success = 0
            ORDER BY timestamp DESC
            {}
            "#,
            period_clause, limit_clause
        );

        #[derive(Serialize, Deserialize, Row)]
        struct TransactionResultRow {
            signature: String,
            slot: u64,
            timestamp: i64,
            success: u8,
            fee: Option<u64>,
        }

        let mut cursor = self
            .client
            .client
            .query(&query)
            .fetch::<TransactionResultRow>()?;
        let mut results = Vec::new();

        while let Some(row) = cursor.next().await? {
            results.push(TransactionResult {
                signature: row.signature,
                slot: row.slot,
                timestamp: DateTime::from_timestamp_millis(row.timestamp).unwrap_or_else(Utc::now),
                success: false,
                fee: row.fee,
            });
        }

        Ok(results)
    }

    /// Get recent transactions
    pub async fn get_recent_transactions(
        &self,
        limit: usize,
        filters: Option<TransactionFilters>,
    ) -> Result<Vec<TransactionResult>> {
        let where_clause = filters
            .as_ref()
            .map(|f| self.build_where_clause(f))
            .unwrap_or_else(|| "1=1".to_string());

        let query = format!(
            r#"
                SELECT 
                    signature,
                    slot,
                    timestamp,
                    success,
                    fee
                FROM transactions
                WHERE {}
                ORDER BY timestamp DESC
                LIMIT {}
                "#,
            where_clause, limit
        );

        #[derive(Serialize, Deserialize, Row)]
        struct TransactionResultRow {
            signature: String,
            slot: u64,
            timestamp: i64,
            success: u8,
            fee: Option<u64>,
        }

        let mut cursor = self
            .client
            .client
            .query(&query)
            .fetch::<TransactionResultRow>()?;
        let mut results = Vec::new();

        while let Some(row) = cursor.next().await? {
            results.push(TransactionResult {
                signature: row.signature,
                slot: row.slot,
                timestamp: DateTime::from_timestamp_millis(row.timestamp).unwrap_or_else(Utc::now),
                success: row.success == 1,
                fee: row.fee,
            });
        }

        Ok(results)
    }

    /// Get transaction by signature
    pub async fn get_transaction(&self, signature: &str) -> Result<Option<TransactionResult>> {
        let query = format!(
            r#"
            SELECT 
                signature,
                slot,
                timestamp,
                success,
                fee
            FROM transactions
            WHERE signature = '{}'
            LIMIT 1
            "#,
            signature
        );

        #[derive(Serialize, Deserialize, Row)]
        struct TransactionResultRow {
            signature: String,
            slot: u64,
            timestamp: i64,
            success: u8,
            fee: Option<u64>,
        }

        let result = self
            .client
            .query_single::<TransactionResultRow>(&query)
            .await?;

        Ok(result.map(|row| TransactionResult {
            signature: row.signature,
            slot: row.slot,
            timestamp: DateTime::from_timestamp_millis(row.timestamp).unwrap_or_else(Utc::now),
            success: row.success == 1,
            fee: row.fee,
        }))
    }

    // ========== Volume Queries ==========

    /// Get volume statistics
    pub async fn get_volume(&self, _filters: VolumeFilters) -> Result<VolumeStats> {
        // Implementation
        todo!()
    }

    /// Get volume by DEX
    pub async fn get_volume_by_dex(&self, _period: TimePeriod) -> Result<HashMap<String, u64>> {
        // Implementation
        todo!()
    }

    /// Get volume by time bucket (hourly/daily)
    pub async fn get_volume_timeseries(
        &self,
        _period: TimePeriod,
        _bucket: TimeBucket,
    ) -> Result<Vec<VolumeDataPoint>> {
        // Implementation
        todo!()
    }

    // ========== Token Pair Queries ==========

    /// Get top token pairs by volume
    pub async fn get_top_pairs(
        &self,
        _limit: usize,
        _period: TimePeriod,
    ) -> Result<Vec<TokenPairStats>> {
        // Implementation
        todo!()
    }

    /// Get pair statistics
    pub async fn get_pair_stats(
        &self,
        _token_a: &str,
        _token_b: &str,
        _period: TimePeriod,
    ) -> Result<PairStats> {
        // Implementation
        todo!()
    }

    // ========== DEX Queries ==========

    /// Compare DEX performance
    pub async fn compare_dexes(&self, _period: TimePeriod) -> Result<Vec<DexStats>> {
        // Implementation
        todo!()
    }

    /// Get DEX market share
    pub async fn get_dex_market_share(&self, _period: TimePeriod) -> Result<HashMap<String, f64>> {
        // Implementation
        todo!()
    }

    // ========== User/Trader Queries ==========

    /// Get top traders
    pub async fn get_top_traders(
        &self,
        _period: TimePeriod,
        _limit: usize,
    ) -> Result<Vec<TraderStats>> {
        // Implementation
        todo!()
    }

    /// Get trader activity
    pub async fn get_trader_activity(
        &self,
        _address: &str,
        _period: TimePeriod,
    ) -> Result<TraderActivity> {
        // Implementation
        todo!()
    }

    /// Get error patterns
    pub async fn analyze_errors(&self, _period: TimePeriod) -> Result<ErrorAnalysis> {
        todo!()
    }
}

// Filter types
#[derive(Debug, Clone, Default)]
pub struct TransactionFilters {
    pub dex: Option<String>,
    pub success: Option<bool>,
    pub min_fee: Option<u64>,
    pub max_fee: Option<u64>,
    pub period: Option<TimePeriod>,
    pub slot_range: Option<(u64, u64)>,
}

#[derive(Debug, Clone)]
pub struct VolumeFilters {
    pub period: TimePeriod,
    pub tx_filters: TransactionFilters,
    pub min_volume: Option<u64>,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub enum TimePeriod {
    LastHour,
    Last24Hours,
    Last7Days,
    Last30Days,
    Custom {
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    },
}

#[derive(Debug, Clone, Copy)]
pub enum TimeBucket {
    Minute,
    Hour,
    Day,
    Week,
}

// result types

#[derive(Debug, Serialize)]
pub struct TransactionResult {
    pub signature: String,
    pub slot: u64,
    pub timestamp: DateTime<Utc>,
    pub success: bool,
    pub fee: Option<u64>,
}

#[derive(Debug, Serialize, Default)]
pub struct FeeStats {
    pub min: Option<u64>,
    pub max: Option<u64>,
    pub average: Option<f64>,
    pub median: Option<u64>,
    pub total: Option<u64>,
    pub transaction_count: u64,
}

#[derive(Debug, Serialize)]
pub struct TpsDataPoint {
    pub timestamp: i64,
    pub tps: f64,
    pub transaction_count: u64,
}

#[derive(Debug, Serialize, Default)]
pub struct SlotStats {
    pub min_slot: u64,
    pub max_slot: u64,
    pub unique_slots: u64,
    pub total_transactions: u64,
    pub avg_tx_per_slot: f64,
}

#[derive(Debug, Serialize)]
pub struct VolumeStats {
    pub total_volume: u64,
    pub transaction_count: u64,
    pub average_volume: f64,
    pub period: TimePeriod,
}

#[derive(Debug, Serialize)]
pub struct VolumeDataPoint {
    pub timestamp: DateTime<Utc>,
    pub volume: u64,
    pub transaction_count: u64,
}

#[derive(Debug, Serialize)]
pub struct TokenPairStats {
    pub token_a: String,
    pub token_b: String,
    pub swap_count: u64,
    pub total_volume: u64,
}

#[derive(Debug, Serialize)]
pub struct PairStats {
    pub token_a: String,
    pub token_b: String,
    pub total_volume: u64,
    pub swap_count: u64,
    pub unique_traders: u64,
}

#[derive(Debug, Serialize)]
pub struct DexStats {
    pub dex: String,
    pub transaction_count: u64,
    pub total_volume: u64,
    pub success_rate: f64,
    pub average_fee: f64,
}

#[derive(Debug, Serialize)]
pub struct TraderStats {
    pub address: String,
    pub tx_count: u64,
    pub total_volume: u64,
}

#[derive(Debug, Serialize)]
pub struct TraderActivity {
    pub address: String,
    pub tx_count: u64,
    pub first_seen: Option<DateTime<Utc>>,
    pub last_seen: Option<DateTime<Utc>>,
    pub total_volume: u64,
}

#[derive(Debug, Serialize)]
pub struct ErrorAnalysis {
    pub total_failed: u64,
    pub common_error_patterns: Vec<(String, u64)>,
}
