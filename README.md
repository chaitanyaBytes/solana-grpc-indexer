# Solana gRPC Indexer

A high-performance Solana blockchain indexer that streams transaction data from the Solana network via Yellowstone gRPC, processes it, and stores it in ClickHouse for efficient querying and analysis.

## Overview

This project provides a complete solution for indexing and querying Solana transaction data. It consists of multiple components working together to ingest, process, and query blockchain data at scale.

## Architecture

The project is organized as a Rust workspace with the following components:

- **ingest**: Handles streaming data from Solana via Yellowstone gRPC client
- **processor**: Processes raw transaction data and stores it in ClickHouse
- **query**: CLI tool for querying indexed transaction data
- **core**: Shared configuration and utilities

## Features

- Real-time transaction streaming from Solana network
- High-performance data processing and storage
- ClickHouse integration for efficient analytics
- Comprehensive query CLI with multiple analysis commands
- Support for transaction filtering and time-based queries
- Transaction success rate analysis
- Fee statistics and TPS (transactions per second) metrics
- Time series analysis capabilities

## Prerequisites

- Rust (latest stable version)
- Docker and Docker Compose
- ClickHouse (can be run via Docker Compose)
- Access to Solana gRPC endpoint (Yellowstone gRPC)

## Setup

### 1. Clone the repository

```bash
git clone <repository-url>
cd solana-grpc-indexer
```

### 2. Start ClickHouse

The project includes a Docker Compose configuration for ClickHouse:

```bash
docker-compose up -d
```

This will start ClickHouse on:

- HTTP interface: `localhost:8123`
- Native TCP interface: `localhost:9000`

### 3. Configure environment variables

Create a `.env` file in the project root with the following variables:

```
CLICKHOUSE_URL=http://localhost:8123
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=your_password
CLICKHOUSE_DB=your_database_name
```

Additional configuration may be required for:

- Solana gRPC endpoint URL
- Redis connection (if used)
- Other service-specific settings

### 4. Build the project

```bash
cargo build --release
```

## Usage

### Running the Indexer

Start the ingestion and processing services to begin indexing Solana transactions:

```bash
# Run the processor (handles ingestion and processing)
cargo run --bin processor
```

### Query CLI

The query CLI provides various commands to analyze indexed transaction data:

#### Get Transaction Count

```bash
cargo run --bin query -- count [period]
```

Period options: `1h`, `24h`, `7d`, `30d`

Example:

```bash
cargo run --bin query -- count 24h
```

#### Get Recent Transactions

```bash
cargo run --bin query -- recent [--limit N] [--period PERIOD]
```

Example:

```bash
cargo run --bin query -- recent --limit 20 --period 1h
```

#### Get Success Rate

```bash
cargo run --bin query -- success-rate [period]
```

Example:

```bash
cargo run --bin query -- success-rate 24h
```

#### Get Fee Statistics

```bash
cargo run --bin query -- fee-stats [period]
```

Returns min, max, average, median, total fees, and transaction count.

#### Get Total Fees

```bash
cargo run --bin query -- total-fees [period]
```

#### Get Transactions Per Second (TPS)

```bash
cargo run --bin query -- tps [period]
```

#### Get TPS Time Series

```bash
cargo run --bin query -- tps-timeseries [period] [bucket]
```

Bucket options: `M` (minute), `H` (hour), `D` (day), `W` (week)

Example:

```bash
cargo run --bin query -- tps-timeseries 24h H
```

#### Get Slot Statistics

```bash
cargo run --bin query -- slot-stats [period]
```

#### Get Failed Transactions

```bash
cargo run --bin query -- failed-transactions [period] [--limit N]
```

#### Get Transaction by Signature

```bash
cargo run --bin query -- transaction <signature>
```

## Project Structure

```
solana-grpc-indexer/
├── core/           # Shared configuration and utilities
├── ingest/         # Yellowstone gRPC client and data ingestion
├── processor/      # Data processing and ClickHouse integration
├── query/          # CLI query tool
├── docker-compose.yaml
└── Cargo.toml      # Workspace configuration
```

## Dependencies

Key dependencies:

- `yellowstone-grpc-client`: Solana gRPC client
- `clickhouse`: ClickHouse database client
- `tokio`: Async runtime
- `clap`: CLI argument parsing
- `solana-sdk`: Solana blockchain SDK

## Development

### Running Tests

```bash
cargo test
```

### Building Individual Components

```bash
cargo build --package ingest
cargo build --package processor
cargo build --package query
```

## License

[Add your license information here]

## Contributing

[Add contribution guidelines here]
