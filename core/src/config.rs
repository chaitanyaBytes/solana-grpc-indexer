use anyhow::{Ok, Result};
use std::env;

pub struct Config {
    pub yellowstone_grpc_endpoint: String,
    pub yellowstone_grpc_token: String,
}

impl Config {
    pub fn load_config() -> Result<Self> {
        Ok(Self {
            yellowstone_grpc_endpoint: env::var("YELLOWSTONE_GRPC_ENDPOINT")
                .unwrap_or("".to_string()),
            yellowstone_grpc_token: env::var("YELLOWSTONE_GRPC_TOKEN").unwrap_or("".to_string()),
        })
    }
}
