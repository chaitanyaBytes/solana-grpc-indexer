use anyhow::Result;
use clap::{Parser, Subcommand};
use processor::ClickhouseClient;
use processor::query::{QueryService, TimeBucket, TimePeriod, TransactionFilters};

#[derive(Parser)]
#[command(name = "dex-query")]
#[command(about = "Query DEX transaction data")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Get transaction count
    Count {
        /// Time period (e.g., "1h", "24h", "7d", "30d")
        period: Option<String>,
    },
    /// Get recent transactions
    Recent {
        /// Number of transactions
        limit: Option<usize>,
        /// Optional period filter
        period: Option<String>,
    },
    /// Get success rate (percentage)
    SuccessRate {
        period: Option<String>,
    },
    /// Get fee statistics
    FeeStats {
        period: Option<String>,
    },
    /// Get total Fees
    TotalFees {
        period: Option<String>,
    },
    /// Get transactions per second (TPS)
    Tps {
        period: Option<String>,
    },
    /// Get transactions per second (TPS) in time series
    TpsTimeseries {
        period: Option<String>,
        bucket: Option<String>,
    },
    // Get slot stats
    SlotStats {
        period: Option<String>,
    },
    /// Get failed transactions
    FailedTransactions {
        period: Option<String>,
        limit: Option<usize>,
    },
    /// Get transaction by signature
    Transaction {
        signature: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    dotenv::dotenv().ok();

    let client = ClickhouseClient::new(
        &std::env::var("CLICKHOUSE_URL")?,
        &std::env::var("CLICKHOUSE_USER")?,
        &std::env::var("CLICKHOUSE_PASSWORD")?,
        &std::env::var("CLICKHOUSE_DB")?,
    )
    .await?;

    let qs = QueryService::new(client);

    match cli.command {
        Commands::Count { period } => {
            let filters = TransactionFilters {
                period: parse_period(period),
                ..Default::default()
            };

            let count = qs.count_transactions(filters).await?;
            println!("Total transactions: {}", count);
        }
        Commands::SuccessRate { period } => {
            let p = parse_period(period).unwrap_or(TimePeriod::Last24Hours);
            let rate = qs.get_success_rate(p).await?;
            println!("Success rate: {:.2}%", rate);
        }
        Commands::FeeStats { period } => {
            let p = parse_period(period).unwrap_or(TimePeriod::Last24Hours);
            let stats = qs.get_fee_stats(p).await?;
            println!(
                "Fees -> min: {:?}, max: {:?}, avg: {:?}, median: {:?}, total: {:?}, tx_count: {}",
                stats.min,
                stats.max,
                stats.average,
                stats.median,
                stats.total,
                stats.transaction_count
            );
        }
        Commands::TotalFees { period } => {
            let p = parse_period(period).unwrap_or(TimePeriod::Last24Hours);
            let total_fees = qs.get_total_fees(p).await?;
            println!("total fees {}", total_fees)
        }
        Commands::Tps { period } => {
            let p = parse_period(period).unwrap_or(TimePeriod::Last24Hours);
            let tps = qs.get_tps(p).await?;
            println!("Tps: {} ", tps)
        }
        Commands::TpsTimeseries { period, bucket } => {
            let p = parse_period(period).unwrap_or(TimePeriod::Last24Hours);
            let bucket = parse_bucket(bucket).unwrap_or(TimeBucket::Hour);
            let tps_timeseries = qs.get_tps_timeseries(p, bucket).await?;
            println!("Tps in timeseries: {:?}", tps_timeseries);
        }
        Commands::SlotStats { period } => {
            let p = parse_period(period).unwrap_or(TimePeriod::Last24Hours);
            let slot_stats = qs.get_slot_stats(p).await?;
            println!("slot stats: {:?}", slot_stats);
        }
        Commands::Recent { limit, period } => {
            let filters = TransactionFilters {
                period: parse_period(period),
                ..Default::default()
            };

            let txs = qs
                .get_recent_transactions(limit.unwrap_or(10), Some(filters))
                .await?;

            for tx in txs {
                println!(
                    "{} | slot={} | success={} | fee={:?}",
                    tx.signature, tx.slot, tx.success, tx.fee
                );
            }
        }
        Commands::Transaction { signature } => {
            if let Some(sig) = signature {
                let tx = qs.get_transaction(&sig).await?;
                match tx {
                    Some(t) => println!("Transaction details: {:?}", t),
                    None => println!("invalid signature"),
                }
            } else {
                println!("signature is required")
            }
        }
        Commands::FailedTransactions { period, limit } => {
            let p = parse_period(period).unwrap_or(TimePeriod::Last24Hours);
            let failed_tx = qs.get_failed_transactions(p, limit).await?;
            println!("failed transaction: {:?}", failed_tx);
        }
    }

    Ok(())
}

fn parse_period(p: Option<String>) -> Option<TimePeriod> {
    match p.as_deref() {
        Some("1h") => Some(TimePeriod::LastHour),
        Some("24h") => Some(TimePeriod::Last24Hours),
        Some("7d") => Some(TimePeriod::Last7Days),
        Some("30d") => Some(TimePeriod::Last30Days),
        _ => None,
    }
}

fn parse_bucket(b: Option<String>) -> Option<TimeBucket> {
    match b.as_deref() {
        Some("M") => Some(TimeBucket::Minute),
        Some("H") => Some(TimeBucket::Hour),
        Some("D") => Some(TimeBucket::Day),
        Some("W") => Some(TimeBucket::Week),
        _ => None,
    }
}
