use anyhow::Result;
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub protocol: Protocol,
    pub http_endpoint: String,
    pub grpc_endpoint: String,
    pub mode: SendMode,
    pub target_tps: u64,
    pub burst_size: usize,
    pub burst_interval_ms: u64,
    pub jitter_percent: f64,
    pub concurrency: usize,
    pub account_count: usize,
    pub initial_nonce: u64,
    pub payload_size: usize,
    pub tx_types: Vec<TxType>,
    pub output: OutputConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputConfig {
    pub json_interval_ms: u64,
    pub prometheus_addr: Option<String>,
    pub csv_path: Option<String>,
    pub metrics_interval_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Protocol {
    Http,
    Grpc,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SendMode {
    Fixed,
    Burst,
    Sustained,
    Jitter,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TxType {
    Transfer,
    Stake,
    ContractCall,
}

#[derive(Parser, Debug)]
#[command(name = "hcp-loadgen", version)]
struct Cli {
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(long)]
    protocol: Option<String>,
    #[arg(long)]
    http_endpoint: Option<String>,
    #[arg(long)]
    grpc_endpoint: Option<String>,
    #[arg(long)]
    mode: Option<String>,
    #[arg(long)]
    target_tps: Option<u64>,
    #[arg(long)]
    burst_size: Option<usize>,
    #[arg(long)]
    burst_interval_ms: Option<u64>,
    #[arg(long)]
    jitter_percent: Option<f64>,
    #[arg(long)]
    concurrency: Option<usize>,
    #[arg(long)]
    account_count: Option<usize>,
    #[arg(long)]
    initial_nonce: Option<u64>,
    #[arg(long)]
    payload_size: Option<usize>,
    #[arg(long)]
    tx_types: Option<String>,
    #[arg(long)]
    json_interval_ms: Option<u64>,
    #[arg(long)]
    prometheus_addr: Option<String>,
    #[arg(long)]
    csv_path: Option<String>,
    #[arg(long)]
    metrics_interval_ms: Option<u64>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            protocol: Protocol::Http,
            http_endpoint: "http://127.0.0.1:8080/tx".to_string(),
            grpc_endpoint: "http://127.0.0.1:9090".to_string(),
            mode: SendMode::Fixed,
            target_tps: 1000,
            burst_size: 100,
            burst_interval_ms: 1000,
            jitter_percent: 10.0,
            concurrency: 512,
            account_count: 10000,
            initial_nonce: 1,
            payload_size: 256,
            tx_types: vec![TxType::Transfer, TxType::Stake, TxType::ContractCall],
            output: OutputConfig {
                json_interval_ms: 1000,
                prometheus_addr: Some("0.0.0.0:9100".to_string()),
                csv_path: None,
                metrics_interval_ms: 1000,
            },
        }
    }
}

pub fn load_config() -> Result<Config> {
    let cli = Cli::parse();
    let mut config = if let Some(path) = cli.config {
        let contents = fs::read_to_string(path)?;
        toml::from_str::<Config>(&contents)?
    } else {
        Config::default()
    };

    if let Some(protocol) = cli.protocol {
        config.protocol = parse_protocol(&protocol);
    }
    if let Some(http_endpoint) = cli.http_endpoint {
        config.http_endpoint = http_endpoint;
    }
    if let Some(grpc_endpoint) = cli.grpc_endpoint {
        config.grpc_endpoint = grpc_endpoint;
    }
    if let Some(mode) = cli.mode {
        config.mode = parse_mode(&mode);
    }
    if let Some(target_tps) = cli.target_tps {
        config.target_tps = target_tps;
    }
    if let Some(burst_size) = cli.burst_size {
        config.burst_size = burst_size;
    }
    if let Some(burst_interval_ms) = cli.burst_interval_ms {
        config.burst_interval_ms = burst_interval_ms;
    }
    if let Some(jitter_percent) = cli.jitter_percent {
        config.jitter_percent = jitter_percent;
    }
    if let Some(concurrency) = cli.concurrency {
        config.concurrency = concurrency;
    }
    if let Some(account_count) = cli.account_count {
        config.account_count = account_count;
    }
    if let Some(initial_nonce) = cli.initial_nonce {
        config.initial_nonce = initial_nonce;
    }
    if let Some(payload_size) = cli.payload_size {
        config.payload_size = payload_size;
    }
    if let Some(tx_types) = cli.tx_types {
        config.tx_types = parse_tx_types(&tx_types);
    }
    if let Some(json_interval_ms) = cli.json_interval_ms {
        config.output.json_interval_ms = json_interval_ms;
    }
    if let Some(prometheus_addr) = cli.prometheus_addr {
        config.output.prometheus_addr = Some(prometheus_addr);
    }
    if let Some(csv_path) = cli.csv_path {
        config.output.csv_path = Some(csv_path);
    }
    if let Some(metrics_interval_ms) = cli.metrics_interval_ms {
        config.output.metrics_interval_ms = metrics_interval_ms;
    }

    Ok(config)
}

fn parse_protocol(value: &str) -> Protocol {
    match value.to_ascii_lowercase().as_str() {
        "grpc" => Protocol::Grpc,
        _ => Protocol::Http,
    }
}

fn parse_mode(value: &str) -> SendMode {
    match value.to_ascii_lowercase().as_str() {
        "burst" => SendMode::Burst,
        "sustained" => SendMode::Sustained,
        "jitter" => SendMode::Jitter,
        _ => SendMode::Fixed,
    }
}

fn parse_tx_types(value: &str) -> Vec<TxType> {
    value
        .split(',')
        .filter_map(|item| match item.trim().to_ascii_lowercase().as_str() {
            "transfer" => Some(TxType::Transfer),
            "stake" => Some(TxType::Stake),
            "contract_call" => Some(TxType::ContractCall),
            _ => None,
        })
        .collect()
}
