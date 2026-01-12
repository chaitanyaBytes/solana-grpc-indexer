use anyhow::Result;
use std::env;

pub struct Config {
    pub yellowstone_grpc_endpoint: String,
    pub yellowstone_grpc_token: Option<String>,
    pub clickhouse_url: String,
    pub clickhouse_user: String,
    pub clickhouse_password: String,
    pub clickhouse_db: String,
}

impl Config {
    pub fn load_config() -> Result<Self> {
        Ok(Self {
            yellowstone_grpc_endpoint: env::var("YELLOWSTONE_GRPC_ENDPOINT")
                .unwrap_or("".to_string()),
            yellowstone_grpc_token: env::var("YELLOWSTONE_GRPC_TOKEN").ok(),
            clickhouse_url: env::var("CLICKHOUSE_URL")
                .unwrap_or("http://localhost:8123".to_string()),
            clickhouse_db: env::var("DATABASE_NAME").unwrap_or("indexer".to_string()),
            clickhouse_user: env::var("CLICKHOUSE_USER").unwrap_or("default".to_string()),
            clickhouse_password: env::var("CLICKHOUSE_PASSWORD").unwrap_or("pass123".to_string()),
        })
    }
}
