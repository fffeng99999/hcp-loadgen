use anyhow::Result;
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub protocol: Protocol,
    pub http_endpoint: String,
    pub grpc_endpoint: String,
    pub mode: SendMode,
    pub target_tps: u64,
    pub duration: u64,
    pub total_txs: u64,
    pub concurrency: usize,
    pub worker_threads: usize,
    pub async_runtime_threads: usize,
    pub batch_size: usize,
    pub burst_size: usize,
    pub burst_interval_ms: u64,
    pub warmup_duration: u64,
    pub cooldown_duration: u64,
    pub account_count: usize,
    pub accounts_per_worker: usize,
    pub initial_balance: u64,
    pub initial_nonce: u64,
    pub nonce_strategy: NonceStrategy,
    pub account_rotation: bool,
    pub max_inflight_per_account: usize,
    pub multisig: bool,
    pub signers_per_tx: usize,
    pub tx_type: TxType,
    pub payload_size: usize,
    pub message_count_per_tx: usize,
    pub gas_limit: u64,
    pub fee_amount: u64,
    pub denom: String,
    pub memo_size: usize,
    pub timeout_height: u64,
    pub extension_options: Vec<String>,
    pub tx_encoding: TxEncoding,
    pub compression: Compression,
    pub sign_algo: SignAlgo,
    pub sign_mode: SignMode,
    pub signature_cache: bool,
    pub parallel_signing: bool,
    pub signer_threads: usize,
    pub rpc_endpoint_list: Vec<String>,
    pub connection_pool_size: usize,
    pub max_idle_connections: usize,
    pub request_timeout_ms: u64,
    pub retry_count: usize,
    pub retry_backoff_ms: u64,
    pub broadcast_mode: BroadcastMode,
    pub grpc_keepalive_ms: u64,
    pub http2_enabled: bool,
    pub max_inflight_requests: usize,
    pub backpressure_threshold: usize,
    pub rate_limit_strategy: RateLimitStrategy,
    pub jitter_range: f64,
    pub send_interval_ns: u64,
    pub adaptive_tps: bool,
    pub error_threshold_stop: u64,
    pub drop_on_overflow: bool,
    pub metrics_interval_ms: u64,
    pub latency_histogram_buckets: Vec<u64>,
    pub export_format: ExportFormat,
    pub log_level: String,
    pub record_raw_latency: bool,
    pub record_error_details: bool,
    pub cpu_affinity: Option<String>,
    pub numa_node: Option<usize>,
    pub memory_limit: Option<u64>,
    pub socket_buffer_size: Option<u64>,
    pub tcp_nodelay: bool,
    pub reuse_port: bool,
    pub fault_injection_rate: f64,
    pub invalid_signature_rate: f64,
    pub nonce_conflict_rate: f64,
    pub network_delay_simulation_ms: u64,
    pub packet_loss_rate: f64,
    pub node_count: usize,
    pub group_size: usize,
    pub subblock_parallelism: usize,
    pub storage_sharing_factor: usize,
    pub database_url: String,
    pub storage_flush_interval_ms: u64,
    pub storage_channel_size: usize,
    pub storage_max_connections: u32,
    pub output: OutputConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct OutputConfig {
    pub json_interval_ms: u64,
    pub prometheus_addr: Option<String>,
    pub csv_path: Option<String>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NonceStrategy {
    Local,
    Query,
    Optimistic,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TxEncoding {
    Proto,
    Json,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Compression {
    None,
    Gzip,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignAlgo {
    Ed25519,
    Secp256k1,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignMode {
    Direct,
    Legacy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BroadcastMode {
    Async,
    Sync,
    Block,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RateLimitStrategy {
    TokenBucket,
    LeakyBucket,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExportFormat {
    Json,
    Csv,
    Prometheus,
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
    duration: Option<u64>,
    #[arg(long)]
    total_txs: Option<u64>,
    #[arg(long)]
    concurrency: Option<usize>,
    #[arg(long)]
    worker_threads: Option<usize>,
    #[arg(long)]
    async_runtime_threads: Option<usize>,
    #[arg(long)]
    batch_size: Option<usize>,
    #[arg(long)]
    burst_size: Option<usize>,
    #[arg(long)]
    burst_interval: Option<u64>,
    #[arg(long)]
    warmup_duration: Option<u64>,
    #[arg(long)]
    cooldown_duration: Option<u64>,
    #[arg(long)]
    account_count: Option<usize>,
    #[arg(long)]
    accounts_per_worker: Option<usize>,
    #[arg(long)]
    initial_balance: Option<u64>,
    #[arg(long)]
    initial_nonce: Option<u64>,
    #[arg(long)]
    nonce_strategy: Option<String>,
    #[arg(long)]
    account_rotation: Option<bool>,
    #[arg(long)]
    max_inflight_per_account: Option<usize>,
    #[arg(long)]
    multisig: Option<bool>,
    #[arg(long)]
    signers_per_tx: Option<usize>,
    #[arg(long)]
    tx_type: Option<String>,
    #[arg(long)]
    payload_size: Option<usize>,
    #[arg(long)]
    message_count_per_tx: Option<usize>,
    #[arg(long)]
    gas_limit: Option<u64>,
    #[arg(long)]
    fee_amount: Option<u64>,
    #[arg(long)]
    denom: Option<String>,
    #[arg(long)]
    memo_size: Option<usize>,
    #[arg(long)]
    timeout_height: Option<u64>,
    #[arg(long)]
    extension_options: Option<String>,
    #[arg(long)]
    tx_encoding: Option<String>,
    #[arg(long)]
    compression: Option<String>,
    #[arg(long)]
    sign_algo: Option<String>,
    #[arg(long)]
    sign_mode: Option<String>,
    #[arg(long)]
    signature_cache: Option<bool>,
    #[arg(long)]
    parallel_signing: Option<bool>,
    #[arg(long)]
    signer_threads: Option<usize>,
    #[arg(long)]
    rpc_endpoint_list: Option<String>,
    #[arg(long)]
    connection_pool_size: Option<usize>,
    #[arg(long)]
    max_idle_connections: Option<usize>,
    #[arg(long)]
    request_timeout: Option<u64>,
    #[arg(long)]
    retry_count: Option<usize>,
    #[arg(long)]
    retry_backoff: Option<u64>,
    #[arg(long)]
    broadcast_mode: Option<String>,
    #[arg(long)]
    grpc_keepalive: Option<u64>,
    #[arg(long)]
    http2_enabled: Option<bool>,
    #[arg(long)]
    max_inflight_requests: Option<usize>,
    #[arg(long)]
    backpressure_threshold: Option<usize>,
    #[arg(long)]
    rate_limit_strategy: Option<String>,
    #[arg(long)]
    jitter_range: Option<f64>,
    #[arg(long)]
    send_interval_ns: Option<u64>,
    #[arg(long)]
    adaptive_tps: Option<bool>,
    #[arg(long)]
    error_threshold_stop: Option<u64>,
    #[arg(long)]
    drop_on_overflow: Option<bool>,
    #[arg(long)]
    metrics_interval: Option<u64>,
    #[arg(long)]
    latency_histogram_buckets: Option<String>,
    #[arg(long)]
    export_format: Option<String>,
    #[arg(long)]
    log_level: Option<String>,
    #[arg(long)]
    record_raw_latency: Option<bool>,
    #[arg(long)]
    record_error_details: Option<bool>,
    #[arg(long)]
    cpu_affinity: Option<String>,
    #[arg(long)]
    numa_node: Option<usize>,
    #[arg(long)]
    memory_limit: Option<u64>,
    #[arg(long)]
    socket_buffer_size: Option<u64>,
    #[arg(long)]
    tcp_nodelay: Option<bool>,
    #[arg(long)]
    reuse_port: Option<bool>,
    #[arg(long)]
    fault_injection_rate: Option<f64>,
    #[arg(long)]
    invalid_signature_rate: Option<f64>,
    #[arg(long)]
    nonce_conflict_rate: Option<f64>,
    #[arg(long)]
    network_delay_simulation: Option<u64>,
    #[arg(long)]
    packet_loss_rate: Option<f64>,
    #[arg(long)]
    node_count: Option<usize>,
    #[arg(long)]
    group_size: Option<usize>,
    #[arg(long)]
    subblock_parallelism: Option<usize>,
    #[arg(long)]
    storage_sharing_factor: Option<usize>,
    #[arg(long)]
    database_url: Option<String>,
    #[arg(long)]
    storage_flush_interval: Option<u64>,
    #[arg(long)]
    storage_channel_size: Option<usize>,
    #[arg(long)]
    storage_max_connections: Option<u32>,
    #[arg(long)]
    json_interval_ms: Option<u64>,
    #[arg(long)]
    prometheus_addr: Option<String>,
    #[arg(long)]
    csv_path: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            protocol: Protocol::Http,
            http_endpoint: "http://127.0.0.1:8080/tx".to_string(),
            grpc_endpoint: "http://127.0.0.1:9090".to_string(),
            mode: SendMode::Fixed,
            target_tps: 1000,
            duration: 0,
            total_txs: 0,
            concurrency: 512,
            worker_threads: 4,
            async_runtime_threads: 4,
            batch_size: 100,
            burst_size: 100,
            burst_interval_ms: 1000,
            warmup_duration: 0,
            cooldown_duration: 0,
            account_count: 10000,
            accounts_per_worker: 0,
            initial_balance: 1_000_000,
            initial_nonce: 1,
            nonce_strategy: NonceStrategy::Local,
            account_rotation: true,
            max_inflight_per_account: 128,
            multisig: false,
            signers_per_tx: 1,
            tx_type: TxType::Transfer,
            payload_size: 256,
            message_count_per_tx: 1,
            gas_limit: 200_000,
            fee_amount: 1,
            denom: "uhcp".to_string(),
            memo_size: 0,
            timeout_height: 0,
            extension_options: Vec::new(),
            tx_encoding: TxEncoding::Json,
            compression: Compression::None,
            sign_algo: SignAlgo::Ed25519,
            sign_mode: SignMode::Direct,
            signature_cache: false,
            parallel_signing: false,
            signer_threads: 1,
            rpc_endpoint_list: Vec::new(),
            connection_pool_size: 512,
            max_idle_connections: 256,
            request_timeout_ms: 5000,
            retry_count: 0,
            retry_backoff_ms: 100,
            broadcast_mode: BroadcastMode::Sync,
            grpc_keepalive_ms: 0,
            http2_enabled: true,
            max_inflight_requests: 0,
            backpressure_threshold: 0,
            rate_limit_strategy: RateLimitStrategy::TokenBucket,
            jitter_range: 10.0,
            send_interval_ns: 0,
            adaptive_tps: false,
            error_threshold_stop: 0,
            drop_on_overflow: true,
            metrics_interval_ms: 1000,
            latency_histogram_buckets: Vec::new(),
            export_format: ExportFormat::Json,
            log_level: "info".to_string(),
            record_raw_latency: false,
            record_error_details: false,
            cpu_affinity: None,
            numa_node: None,
            memory_limit: None,
            socket_buffer_size: None,
            tcp_nodelay: true,
            reuse_port: false,
            fault_injection_rate: 0.0,
            invalid_signature_rate: 0.0,
            nonce_conflict_rate: 0.0,
            network_delay_simulation_ms: 0,
            packet_loss_rate: 0.0,
            node_count: 0,
            group_size: 0,
            subblock_parallelism: 0,
            storage_sharing_factor: 0,
            database_url: "postgres://user_rbc3B8:password_DfA4Pw@192.168.58.102:5432/hcp_server?sslmode=disable".to_string(),
            storage_flush_interval_ms: 2000,
            storage_channel_size: 10000,
            storage_max_connections: 4,
            output: OutputConfig {
                json_interval_ms: 1000,
                prometheus_addr: Some("0.0.0.0:9100".to_string()),
                csv_path: None,
            },
        }
    }
}

impl Default for OutputConfig {
    fn default() -> Self {
        Self {
            json_interval_ms: 1000,
            prometheus_addr: Some("0.0.0.0:9100".to_string()),
            csv_path: None,
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
    if let Some(duration) = cli.duration {
        config.duration = duration;
    }
    if let Some(total_txs) = cli.total_txs {
        config.total_txs = total_txs;
    }
    if let Some(concurrency) = cli.concurrency {
        config.concurrency = concurrency;
    }
    if let Some(worker_threads) = cli.worker_threads {
        config.worker_threads = worker_threads;
    }
    if let Some(async_runtime_threads) = cli.async_runtime_threads {
        config.async_runtime_threads = async_runtime_threads;
    }
    if let Some(batch_size) = cli.batch_size {
        config.batch_size = batch_size;
    }
    if let Some(burst_size) = cli.burst_size {
        config.burst_size = burst_size;
    }
    if let Some(burst_interval) = cli.burst_interval {
        config.burst_interval_ms = burst_interval;
    }
    if let Some(warmup_duration) = cli.warmup_duration {
        config.warmup_duration = warmup_duration;
    }
    if let Some(cooldown_duration) = cli.cooldown_duration {
        config.cooldown_duration = cooldown_duration;
    }
    if let Some(account_count) = cli.account_count {
        config.account_count = account_count;
    }
    if let Some(accounts_per_worker) = cli.accounts_per_worker {
        config.accounts_per_worker = accounts_per_worker;
    }
    if let Some(initial_balance) = cli.initial_balance {
        config.initial_balance = initial_balance;
    }
    if let Some(initial_nonce) = cli.initial_nonce {
        config.initial_nonce = initial_nonce;
    }
    if let Some(nonce_strategy) = cli.nonce_strategy {
        config.nonce_strategy = parse_nonce_strategy(&nonce_strategy);
    }
    if let Some(account_rotation) = cli.account_rotation {
        config.account_rotation = account_rotation;
    }
    if let Some(max_inflight_per_account) = cli.max_inflight_per_account {
        config.max_inflight_per_account = max_inflight_per_account;
    }
    if let Some(multisig) = cli.multisig {
        config.multisig = multisig;
    }
    if let Some(signers_per_tx) = cli.signers_per_tx {
        config.signers_per_tx = signers_per_tx;
    }
    if let Some(tx_type) = cli.tx_type {
        config.tx_type = parse_tx_type(&tx_type);
    }
    if let Some(payload_size) = cli.payload_size {
        config.payload_size = payload_size;
    }
    if let Some(message_count_per_tx) = cli.message_count_per_tx {
        config.message_count_per_tx = message_count_per_tx;
    }
    if let Some(gas_limit) = cli.gas_limit {
        config.gas_limit = gas_limit;
    }
    if let Some(fee_amount) = cli.fee_amount {
        config.fee_amount = fee_amount;
    }
    if let Some(denom) = cli.denom {
        config.denom = denom;
    }
    if let Some(memo_size) = cli.memo_size {
        config.memo_size = memo_size;
    }
    if let Some(timeout_height) = cli.timeout_height {
        config.timeout_height = timeout_height;
    }
    if let Some(extension_options) = cli.extension_options {
        config.extension_options = parse_list(extension_options);
    }
    if let Some(tx_encoding) = cli.tx_encoding {
        config.tx_encoding = parse_tx_encoding(&tx_encoding);
    }
    if let Some(compression) = cli.compression {
        config.compression = parse_compression(&compression);
    }
    if let Some(sign_algo) = cli.sign_algo {
        config.sign_algo = parse_sign_algo(&sign_algo);
    }
    if let Some(sign_mode) = cli.sign_mode {
        config.sign_mode = parse_sign_mode(&sign_mode);
    }
    if let Some(signature_cache) = cli.signature_cache {
        config.signature_cache = signature_cache;
    }
    if let Some(parallel_signing) = cli.parallel_signing {
        config.parallel_signing = parallel_signing;
    }
    if let Some(signer_threads) = cli.signer_threads {
        config.signer_threads = signer_threads;
    }
    if let Some(rpc_endpoint_list) = cli.rpc_endpoint_list {
        config.rpc_endpoint_list = parse_list(rpc_endpoint_list);
    }
    if let Some(connection_pool_size) = cli.connection_pool_size {
        config.connection_pool_size = connection_pool_size;
    }
    if let Some(max_idle_connections) = cli.max_idle_connections {
        config.max_idle_connections = max_idle_connections;
    }
    if let Some(request_timeout) = cli.request_timeout {
        config.request_timeout_ms = request_timeout;
    }
    if let Some(retry_count) = cli.retry_count {
        config.retry_count = retry_count;
    }
    if let Some(retry_backoff) = cli.retry_backoff {
        config.retry_backoff_ms = retry_backoff;
    }
    if let Some(broadcast_mode) = cli.broadcast_mode {
        config.broadcast_mode = parse_broadcast_mode(&broadcast_mode);
    }
    if let Some(grpc_keepalive) = cli.grpc_keepalive {
        config.grpc_keepalive_ms = grpc_keepalive;
    }
    if let Some(http2_enabled) = cli.http2_enabled {
        config.http2_enabled = http2_enabled;
    }
    if let Some(max_inflight_requests) = cli.max_inflight_requests {
        config.max_inflight_requests = max_inflight_requests;
    }
    if let Some(backpressure_threshold) = cli.backpressure_threshold {
        config.backpressure_threshold = backpressure_threshold;
    }
    if let Some(rate_limit_strategy) = cli.rate_limit_strategy {
        config.rate_limit_strategy = parse_rate_limit_strategy(&rate_limit_strategy);
    }
    if let Some(jitter_range) = cli.jitter_range {
        config.jitter_range = jitter_range;
    }
    if let Some(send_interval_ns) = cli.send_interval_ns {
        config.send_interval_ns = send_interval_ns;
    }
    if let Some(adaptive_tps) = cli.adaptive_tps {
        config.adaptive_tps = adaptive_tps;
    }
    if let Some(error_threshold_stop) = cli.error_threshold_stop {
        config.error_threshold_stop = error_threshold_stop;
    }
    if let Some(drop_on_overflow) = cli.drop_on_overflow {
        config.drop_on_overflow = drop_on_overflow;
    }
    if let Some(metrics_interval) = cli.metrics_interval {
        config.metrics_interval_ms = metrics_interval;
    }
    if let Some(latency_histogram_buckets) = cli.latency_histogram_buckets {
        config.latency_histogram_buckets = parse_list(latency_histogram_buckets)
            .into_iter()
            .filter_map(|value| value.parse::<u64>().ok())
            .collect();
    }
    if let Some(export_format) = cli.export_format {
        config.export_format = parse_export_format(&export_format);
    }
    if let Some(log_level) = cli.log_level {
        config.log_level = log_level;
    }
    if let Some(record_raw_latency) = cli.record_raw_latency {
        config.record_raw_latency = record_raw_latency;
    }
    if let Some(record_error_details) = cli.record_error_details {
        config.record_error_details = record_error_details;
    }
    if let Some(cpu_affinity) = cli.cpu_affinity {
        config.cpu_affinity = Some(cpu_affinity);
    }
    if let Some(numa_node) = cli.numa_node {
        config.numa_node = Some(numa_node);
    }
    if let Some(memory_limit) = cli.memory_limit {
        config.memory_limit = Some(memory_limit);
    }
    if let Some(socket_buffer_size) = cli.socket_buffer_size {
        config.socket_buffer_size = Some(socket_buffer_size);
    }
    if let Some(tcp_nodelay) = cli.tcp_nodelay {
        config.tcp_nodelay = tcp_nodelay;
    }
    if let Some(reuse_port) = cli.reuse_port {
        config.reuse_port = reuse_port;
    }
    if let Some(fault_injection_rate) = cli.fault_injection_rate {
        config.fault_injection_rate = fault_injection_rate;
    }
    if let Some(invalid_signature_rate) = cli.invalid_signature_rate {
        config.invalid_signature_rate = invalid_signature_rate;
    }
    if let Some(nonce_conflict_rate) = cli.nonce_conflict_rate {
        config.nonce_conflict_rate = nonce_conflict_rate;
    }
    if let Some(network_delay_simulation) = cli.network_delay_simulation {
        config.network_delay_simulation_ms = network_delay_simulation;
    }
    if let Some(packet_loss_rate) = cli.packet_loss_rate {
        config.packet_loss_rate = packet_loss_rate;
    }
    if let Some(node_count) = cli.node_count {
        config.node_count = node_count;
    }
    if let Some(group_size) = cli.group_size {
        config.group_size = group_size;
    }
    if let Some(subblock_parallelism) = cli.subblock_parallelism {
        config.subblock_parallelism = subblock_parallelism;
    }
    if let Some(storage_sharing_factor) = cli.storage_sharing_factor {
        config.storage_sharing_factor = storage_sharing_factor;
    }
    if let Some(database_url) = cli.database_url {
        config.database_url = database_url;
    }
    if let Some(storage_flush_interval) = cli.storage_flush_interval {
        config.storage_flush_interval_ms = storage_flush_interval;
    }
    if let Some(storage_channel_size) = cli.storage_channel_size {
        config.storage_channel_size = storage_channel_size;
    }
    if let Some(storage_max_connections) = cli.storage_max_connections {
        config.storage_max_connections = storage_max_connections;
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

    if config.account_count == 0 && config.accounts_per_worker > 0 {
        let accounts_per_worker = config.accounts_per_worker;
        config.account_count = accounts_per_worker.saturating_mul(config.worker_threads);
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

fn parse_tx_type(value: &str) -> TxType {
    match value.to_ascii_lowercase().as_str() {
        "stake" => TxType::Stake,
        "contract_call" => TxType::ContractCall,
        _ => TxType::Transfer,
    }
}

fn parse_nonce_strategy(value: &str) -> NonceStrategy {
    match value.to_ascii_lowercase().as_str() {
        "query" => NonceStrategy::Query,
        "optimistic" => NonceStrategy::Optimistic,
        _ => NonceStrategy::Local,
    }
}

fn parse_tx_encoding(value: &str) -> TxEncoding {
    match value.to_ascii_lowercase().as_str() {
        "proto" => TxEncoding::Proto,
        _ => TxEncoding::Json,
    }
}

fn parse_compression(value: &str) -> Compression {
    match value.to_ascii_lowercase().as_str() {
        "gzip" => Compression::Gzip,
        _ => Compression::None,
    }
}

fn parse_sign_algo(value: &str) -> SignAlgo {
    match value.to_ascii_lowercase().as_str() {
        "secp256k1" => SignAlgo::Secp256k1,
        _ => SignAlgo::Ed25519,
    }
}

fn parse_sign_mode(value: &str) -> SignMode {
    match value.to_ascii_lowercase().as_str() {
        "legacy" => SignMode::Legacy,
        _ => SignMode::Direct,
    }
}

fn parse_broadcast_mode(value: &str) -> BroadcastMode {
    match value.to_ascii_lowercase().as_str() {
        "async" => BroadcastMode::Async,
        "block" => BroadcastMode::Block,
        _ => BroadcastMode::Sync,
    }
}

fn parse_rate_limit_strategy(value: &str) -> RateLimitStrategy {
    match value.to_ascii_lowercase().as_str() {
        "leaky_bucket" => RateLimitStrategy::LeakyBucket,
        _ => RateLimitStrategy::TokenBucket,
    }
}

fn parse_export_format(value: &str) -> ExportFormat {
    match value.to_ascii_lowercase().as_str() {
        "csv" => ExportFormat::Csv,
        "prometheus" => ExportFormat::Prometheus,
        _ => ExportFormat::Json,
    }
}

fn parse_list(value: String) -> Vec<String> {
    value
        .split(',')
        .map(|item| item.trim().to_string())
        .filter(|item| !item.is_empty())
        .collect()
}
