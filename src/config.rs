use anyhow::{anyhow, Result};
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

/// 负载生成器全局配置结构体，聚合了协议、账户、交易、网络、存储等所有可调参数。
/// 支持从配置文件、命令行参数和环境变量加载并合并。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    /// 与节点通信的协议类型：Http 或 Grpc
    pub protocol: Protocol,
    /// HTTP 交易广播端点地址
    pub http_endpoint: String,
    /// gRPC 交易广播端点地址
    pub grpc_endpoint: String,
    pub quic_endpoint: String,
    /// Tendermint RPC 端点地址（用于查询账户状态等）
    pub rpc_endpoint: String,
    /// 目标链的 chain_id
    pub chain_id: String,
    /// 密钥环后端类型，如 "test" 或 "file"
    pub keyring_backend: String,
    /// 密钥环主目录路径（可选）
    pub keyring_home: Option<String>,
    /// 预生成账户数据文件路径（可选）
    pub account_file: Option<String>,
    /// 链客户端二进制名称或路径，默认从环境变量 HCPD_BINARY 读取，否则为 "hcapd"
    pub cli_binary: String,
    /// 交易发送模式：Fixed（固定间隔）、Burst（突发）、Sustained（持续）、Jitter（抖动）
    pub mode: SendMode,
    /// 目标每秒交易数（TPS）
    pub target_tps: u64,
    /// 压测持续时间（秒），0 表示不限制
    pub duration: u64,
    /// 总交易发送数量上限，0 表示不限制
    pub total_txs: u64,
    /// 并发连接数或并发任务数
    pub concurrency: usize,
    /// 工作线程数，用于分片处理交易
    pub worker_threads: usize,
    /// 异步运行时线程数（Tokio worker threads）
    pub async_runtime_threads: usize,
    /// 每批处理的交易数量
    pub batch_size: usize,
    /// 突发模式下每次突发发送的交易数量
    pub burst_size: usize,
    /// 突发模式下每次突发之间的间隔（毫秒）
    pub burst_interval_ms: u64,
    /// 预热阶段持续时间（秒），此阶段数据不计入统计
    pub warmup_duration: u64,
    /// 冷却阶段持续时间（秒），此阶段数据不计入统计
    pub cooldown_duration: u64,
    /// 预生成账户总数
    pub account_count: usize,
    /// 每个工作线程分配的账户数，0 表示由程序自动计算
    pub accounts_per_worker: usize,
    /// 每个账户的初始余额
    pub initial_balance: u64,
    /// 每个账户的初始 nonce 值
    pub initial_nonce: u64,
    /// Nonce 管理策略：Local（本地递增）、Query（查询链上）、Optimistic（乐观）
    pub nonce_strategy: NonceStrategy,
    /// 是否启用账户轮换，避免同一账户被过度使用
    pub account_rotation: bool,
    /// 每个账户最大允许的在途（inflight）交易数
    pub max_inflight_per_account: usize,
    /// 是否启用多签交易
    pub multisig: bool,
    /// 每笔交易需要的签名者数量
    pub signers_per_tx: usize,
    /// 交易类型：Transfer（转账）、Stake（质押）、ContractCall（合约调用）
    pub tx_type: TxType,
    /// 交易附加负载的大小（字节），用于模拟大数据交易
    pub payload_size: usize,
    /// 每笔交易中包含的消息数量
    pub message_count_per_tx: usize,
    /// 单笔交易的 gas 上限
    pub gas_limit: u64,
    /// 交易手续费金额
    pub fee_amount: u64,
    /// 转账金额
    pub send_amount: u64,
    /// 代币最小单位名称，如 "uhcap"
    pub denom: String,
    /// 交易备注（memo）的大小（字节）
    pub memo_size: usize,
    /// 交易超时块高，0 表示不设置
    pub timeout_height: u64,
    /// 交易扩展选项列表
    pub extension_options: Vec<String>,
    /// 交易编码格式：Proto 或 Json
    pub tx_encoding: TxEncoding,
    /// 交易压缩方式：None 或 Gzip
    pub compression: Compression,
    /// 签名算法：Ed25519 或 Secp256k1
    pub sign_algo: SignAlgo,
    /// 签名模式：Direct 或 Legacy
    pub sign_mode: SignMode,
    /// 是否启用签名缓存，避免重复签名计算
    pub signature_cache: bool,
    /// 是否启用并行签名
    pub parallel_signing: bool,
    /// 签名专用线程数
    pub signer_threads: usize,
    /// RPC 端点列表，用于负载均衡或故障转移
    pub rpc_endpoint_list: Vec<String>,
    /// HTTP/gRPC 连接池大小
    pub connection_pool_size: usize,
    /// 连接池中最大空闲连接数
    pub max_idle_connections: usize,
    /// 单次请求超时时间（毫秒）
    pub request_timeout_ms: u64,
    /// 请求失败后的重试次数
    pub retry_count: usize,
    /// 重试间隔退避时间（毫秒）
    pub retry_backoff_ms: u64,
    /// 交易广播模式：Async（异步）、Sync（同步等待检查）、Block（同步等待出块）
    pub broadcast_mode: BroadcastMode,
    /// gRPC  keepalive 间隔（毫秒），0 表示禁用
    pub grpc_keepalive_ms: u64,
    /// 是否启用 HTTP/2
    pub http2_enabled: bool,
    /// 最大在途请求数，0 表示不限制
    pub max_inflight_requests: usize,
    /// 背压阈值，当在途交易超过此值时触发流量控制，0 表示使用默认值 5_000_000
    pub backpressure_threshold: usize,
    /// 限流策略：TokenBucket（令牌桶）或 LeakyBucket（漏桶）
    pub rate_limit_strategy: RateLimitStrategy,
    /// 抖动模式下的抖动范围百分比
    pub jitter_range: f64,
    /// 固定发送模式下的发送间隔（纳秒）
    pub send_interval_ns: u64,
    /// 是否启用自适应 TPS，根据系统负载动态调整发送速率
    pub adaptive_tps: bool,
    /// 错误阈值，当连续错误数超过此值时停止压测，0 表示不限制
    pub error_threshold_stop: u64,
    /// 当队列溢出时是否丢弃新交易，true 表示丢弃，false 表示阻塞等待
    pub drop_on_overflow: bool,
    /// 指标采集和输出间隔（毫秒）
    pub metrics_interval_ms: u64,
    /// 延迟直方图的分桶边界（毫秒）
    pub latency_histogram_buckets: Vec<u64>,
    /// 指标导出格式：Json、Csv 或 Prometheus
    pub export_format: ExportFormat,
    /// 日志级别，如 "info"、"debug"、"warn"、"error"
    pub log_level: String,
    /// 是否记录原始延迟数据
    pub record_raw_latency: bool,
    /// 是否记录错误详情
    pub record_error_details: bool,
    /// CPU 亲和性设置（可选），格式如 "0-3,5"
    pub cpu_affinity: Option<String>,
    /// 指定的 NUMA 节点（可选）
    pub numa_node: Option<usize>,
    /// 内存限制（字节，可选）
    pub memory_limit: Option<u64>,
    /// 套接字缓冲区大小（可选）
    pub socket_buffer_size: Option<u64>,
    /// 是否启用 TCP_NODELAY，禁用 Nagle 算法以降低延迟
    pub tcp_nodelay: bool,
    /// 是否启用 SO_REUSEPORT，允许多个套接字绑定同一端口
    pub reuse_port: bool,
    /// 故障注入率（0.0 ~ 1.0），用于模拟异常交易
    pub fault_injection_rate: f64,
    /// 无效签名注入率（0.0 ~ 1.0），用于测试签名验证
    pub invalid_signature_rate: f64,
    /// Nonce 冲突注入率（0.0 ~ 1.0），用于测试重放保护
    pub nonce_conflict_rate: f64,
    /// 模拟网络延迟（毫秒）
    pub network_delay_simulation_ms: u64,
    /// 模拟丢包率（0.0 ~ 1.0）
    pub packet_loss_rate: f64,
    /// 共识节点数量，用于实验场景配置
    pub node_count: usize,
    /// 分组大小，用于分层共识等实验
    pub group_size: usize,
    /// 子块并行度，用于并行执行实验
    pub subblock_parallelism: usize,
    /// 存储共享因子，用于存储分片实验
    pub storage_sharing_factor: usize,
    /// 账户选择模式：RoundRobin（轮询）、Random（随机）、Zipf（齐普夫分布）
    pub account_selection_mode: AccountSelectionMode,
    /// Zipf 分布的 alpha 参数，越大越集中
    pub zipf_alpha: f64,
    /// 每个工作线程内部的交易缓冲区容量
    pub worker_buffer_capacity: usize,
    /// PostgreSQL 数据库连接 URL
    pub database_url: String,
    /// 数据库 schema 名称
    pub db_schema: String,
    /// 启动时是否重置（清空）数据库 schema
    pub reset_schema_on_start: bool,
    /// 存储刷新间隔（毫秒），控制数据批量写入数据库的频率
    pub storage_flush_interval_ms: u64,
    /// 存储通道大小，用于解耦交易发送和持久化
    pub storage_channel_size: usize,
    /// 存储模块的数据库最大连接数
    pub storage_max_connections: u32,
    /// 输出配置子结构，包含 JSON 间隔、Prometheus 地址、CSV 路径等
    pub output: OutputConfig,
}

/// 输出配置子结构，控制指标和结果的输出方式。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct OutputConfig {
    /// JSON 指标输出间隔（毫秒）
    pub json_interval_ms: u64,
    /// Prometheus 暴露地址（可选），如 "0.0.0.0:9100"
    pub prometheus_addr: Option<String>,
    /// CSV 结果输出文件路径（可选）
    pub csv_path: Option<String>,
}

/// 配置文件中的 performance 段落，用于覆盖部分性能相关参数。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct PerformanceSection {
    worker_buffer_capacity: Option<usize>,
    backpressure_threshold: Option<usize>,
    storage_channel_size: Option<usize>,
}

/// 配置文件顶层结构，包含主配置和 performance 段落。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
struct FileConfig {
    #[serde(flatten)]
    config: Config,
    performance: PerformanceSection,
}

/// 通信协议枚举，支持 HTTP 和 gRPC 两种广播方式。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Protocol {
    Http,
    Grpc,
    Quic,
}

/// 交易发送模式枚举，决定交易流量的时间分布特征。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SendMode {
    /// 固定间隔发送
    Fixed,
    /// 周期性突发发送
    Burst,
    /// 持续饱和发送
    Sustained,
    /// 带随机抖动的发送
    Jitter,
}

/// 交易类型枚举，用于构造不同种类的交易负载。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TxType {
    /// 普通转账交易
    Transfer,
    /// 质押交易
    Stake,
    /// 合约调用交易
    ContractCall,
}

/// Nonce 管理策略枚举，控制如何维护交易序列号。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NonceStrategy {
    /// 仅在本地内存中递增 nonce
    Local,
    /// 每笔交易前查询链上最新 nonce
    Query,
    /// 乐观本地递增，冲突后回退
    Optimistic,
}

/// 交易编码格式枚举。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TxEncoding {
    /// Protobuf 二进制编码
    Proto,
    /// JSON 文本编码
    Json,
}

/// 交易压缩方式枚举。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Compression {
    /// 不压缩
    None,
    /// Gzip 压缩
    Gzip,
}

/// 签名算法枚举。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignAlgo {
    /// Ed25519 签名算法
    Ed25519,
    /// Secp256k1 签名算法
    Secp256k1,
}

/// 签名模式枚举。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignMode {
    /// 直接签名模式
    Direct,
    /// 遗留签名模式（兼容旧版）
    Legacy,
}

/// 交易广播模式枚举，决定节点对 BroadcastTx 的响应方式。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BroadcastMode {
    /// 异步广播，立即返回不等待
    Async,
    /// 同步广播，等待 CheckTx 结果
    Sync,
    /// 阻塞广播，等待交易被打包入块
    Block,
}

/// 限流策略枚举，用于控制交易发送速率。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RateLimitStrategy {
    /// 令牌桶算法，允许一定突发流量
    TokenBucket,
    /// 漏桶算法，输出速率更平滑
    LeakyBucket,
}

/// 账户选择模式枚举，决定如何从账户池中选择发送账户。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountSelectionMode {
    /// 轮询选择，均匀分布
    RoundRobin,
    /// 随机选择
    Random,
    /// 按 Zipf 分布选择，模拟热点账户
    Zipf,
}

/// 指标导出格式枚举。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExportFormat {
    /// JSON 格式输出
    Json,
    /// CSV 格式输出
    Csv,
    /// Prometheus 格式暴露
    Prometheus,
}

/// 命令行参数结构体，使用 clap 派生宏自动生成 CLI 解析逻辑。
/// 每个字段对应一个 Config 中的配置项，用于命令行覆盖。
#[derive(Parser, Debug)]
#[command(name = "hcap-loadgen", version)]
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
    quic_endpoint: Option<String>,
    #[arg(long)]
    rpc_endpoint: Option<String>,
    #[arg(long)]
    chain_id: Option<String>,
    #[arg(long)]
    keyring_backend: Option<String>,
    #[arg(long)]
    keyring_home: Option<String>,
    #[arg(long)]
    account_file: Option<String>,
    #[arg(long)]
    cli_binary: Option<String>,
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
    send_amount: Option<u64>,
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
    #[arg(long, visible_alias = "backpressure-limit")]
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
    account_selection_mode: Option<String>,
    #[arg(long)]
    zipf_alpha: Option<f64>,
    #[arg(long, visible_alias = "buffer-size")]
    worker_buffer_capacity: Option<usize>,
    #[arg(long)]
    database_url: Option<String>,
    #[arg(long, visible_alias = "database-schema")]
    db_schema: Option<String>,
    #[arg(long)]
    reset_schema_on_start: Option<bool>,
    #[arg(long)]
    storage_flush_interval: Option<u64>,
    #[arg(long, visible_alias = "channel-capacity")]
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
    /// 提供 Config 的默认值，确保所有字段都有合理的初始设置。
    fn default() -> Self {
        Self {
            protocol: Protocol::Http,
            http_endpoint: "http://127.0.0.1:8080/tx".to_string(),
            grpc_endpoint: "http://127.0.0.1:9090".to_string(),
            quic_endpoint: "127.0.0.1:8443".to_string(),
            rpc_endpoint: "tcp://127.0.0.1:26657".to_string(),
            chain_id: "hcap-testnet-1".to_string(),
            keyring_backend: "test".to_string(),
            keyring_home: None,
            account_file: None,
            cli_binary: std::env::var("HCPD_BINARY").unwrap_or_else(|_| "hcapd".to_string()),
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
            send_amount: 1,
            denom: "uhcap".to_string(),
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
            account_selection_mode: AccountSelectionMode::RoundRobin,
            zipf_alpha: 0.0,
            worker_buffer_capacity: 1000,
            database_url: "postgres://user_rbc3B8:password_DfA4Pw@192.168.58.102:5432/hcap_server?sslmode=disable&search_path=loadgendata,public".to_string(),
            db_schema: "loadgendata".to_string(),
            reset_schema_on_start: false,
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
    /// OutputConfig 的默认值实现。
    fn default() -> Self {
        Self {
            json_interval_ms: 1000,
            prometheus_addr: Some("0.0.0.0:9100".to_string()),
            csv_path: None,
        }
    }
}

/// 加载并合并配置：优先从配置文件读取，然后用命令行参数覆盖，最后执行校验。
pub fn load_config() -> Result<Config> {
    let cli = Cli::parse();
    let mut config = if let Some(path) = cli.config {
        // 若指定了配置文件，解析 TOML 格式的 FileConfig
        let contents = fs::read_to_string(path)?;
        let mut file_config = toml::from_str::<FileConfig>(&contents)?;
        // 将 performance 段落的可选值合并到主配置
        if let Some(worker_buffer_capacity) = file_config.performance.worker_buffer_capacity {
            file_config.config.worker_buffer_capacity = worker_buffer_capacity;
        }
        if let Some(backpressure_threshold) = file_config.performance.backpressure_threshold {
            file_config.config.backpressure_threshold = backpressure_threshold;
        }
        if let Some(storage_channel_size) = file_config.performance.storage_channel_size {
            file_config.config.storage_channel_size = storage_channel_size;
        }
        file_config.config
    } else {
        Config::default()
    };

    // 以下所有 if let 块用于将命令行参数（若存在）覆盖到 config 中
    if let Some(protocol) = cli.protocol {
        config.protocol = parse_protocol(&protocol);
    }
    if let Some(http_endpoint) = cli.http_endpoint {
        config.http_endpoint = http_endpoint;
    }
    if let Some(grpc_endpoint) = cli.grpc_endpoint {
        config.grpc_endpoint = grpc_endpoint;
    }
    if let Some(quic_endpoint) = cli.quic_endpoint {
        config.quic_endpoint = quic_endpoint;
    }
    if let Some(rpc_endpoint) = cli.rpc_endpoint {
        config.rpc_endpoint = rpc_endpoint;
    }
    if let Some(chain_id) = cli.chain_id {
        config.chain_id = chain_id;
    }
    if let Some(keyring_backend) = cli.keyring_backend {
        config.keyring_backend = keyring_backend;
    }
    if let Some(keyring_home) = cli.keyring_home {
        config.keyring_home = Some(keyring_home);
    }
    if let Some(account_file) = cli.account_file {
        config.account_file = Some(account_file);
    }
    if let Some(cli_binary) = cli.cli_binary {
        config.cli_binary = cli_binary;
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
    if let Some(send_amount) = cli.send_amount {
        config.send_amount = send_amount;
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
    if let Some(account_selection_mode) = cli.account_selection_mode {
        config.account_selection_mode = parse_account_selection_mode(&account_selection_mode);
    }
    if let Some(zipf_alpha) = cli.zipf_alpha {
        config.zipf_alpha = zipf_alpha;
    }
    if let Some(worker_buffer_capacity) = cli.worker_buffer_capacity {
        config.worker_buffer_capacity = worker_buffer_capacity;
    }
    if let Some(database_url) = cli.database_url {
        config.database_url = database_url;
    }
    if let Some(db_schema) = cli.db_schema {
        config.db_schema = db_schema;
    }
    if let Some(reset_schema_on_start) = cli.reset_schema_on_start {
        config.reset_schema_on_start = reset_schema_on_start;
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

    // 若未指定 account_count 但指定了 accounts_per_worker，则自动计算总账户数
    if config.account_count == 0 && config.accounts_per_worker > 0 {
        let accounts_per_worker = config.accounts_per_worker;
        config.account_count = accounts_per_worker.saturating_mul(config.worker_threads);
    }

    // 以下进行配置合法性校验
    if config.worker_buffer_capacity == 0 {
        return Err(anyhow!("worker_buffer_capacity must be greater than 0"));
    }
    if config.storage_channel_size == 0 {
        return Err(anyhow!("storage_channel_size must be greater than 0"));
    }
    let effective_backpressure = if config.backpressure_threshold == 0 {
        5_000_000
    } else {
        config.backpressure_threshold
    };
    let min_backpressure =
        config.worker_buffer_capacity.saturating_mul(config.worker_threads.max(1));
    if effective_backpressure <= min_backpressure {
        return Err(anyhow!(
            "backpressure_threshold must be greater than worker_buffer_capacity * worker_threads"
        ));
    }
    if !is_valid_identifier(&config.db_schema) {
        return Err(anyhow!(
            "db_schema must contain only letters, digits, and underscores, and cannot start with a digit"
        ));
    }

    Ok(config)
}

/// 检查字符串是否为合法的数据库标识符（字母、数字、下划线，且不以数字开头）。
fn is_valid_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    match chars.next() {
        Some(ch) if ch.is_ascii_alphabetic() || ch == '_' => {}
        _ => return false,
    }
    chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

/// 将字符串解析为 Protocol 枚举，不区分大小写，未知值默认返回 Http。
fn parse_protocol(value: &str) -> Protocol {
    match value.to_ascii_lowercase().as_str() {
        "grpc" => Protocol::Grpc,
        "quic" => Protocol::Quic,
        _ => Protocol::Http,
    }
}

/// 将字符串解析为 SendMode 枚举，不区分大小写，未知值默认返回 Fixed。
fn parse_mode(value: &str) -> SendMode {
    match value.to_ascii_lowercase().as_str() {
        "burst" => SendMode::Burst,
        "sustained" => SendMode::Sustained,
        "jitter" => SendMode::Jitter,
        _ => SendMode::Fixed,
    }
}

/// 将字符串解析为 TxType 枚举，不区分大小写，未知值默认返回 Transfer。
fn parse_tx_type(value: &str) -> TxType {
    match value.to_ascii_lowercase().as_str() {
        "stake" => TxType::Stake,
        "contract_call" => TxType::ContractCall,
        _ => TxType::Transfer,
    }
}

/// 将字符串解析为 NonceStrategy 枚举，不区分大小写，未知值默认返回 Local。
fn parse_nonce_strategy(value: &str) -> NonceStrategy {
    match value.to_ascii_lowercase().as_str() {
        "query" => NonceStrategy::Query,
        "optimistic" => NonceStrategy::Optimistic,
        _ => NonceStrategy::Local,
    }
}

/// 将字符串解析为 TxEncoding 枚举，不区分大小写，未知值默认返回 Json。
fn parse_tx_encoding(value: &str) -> TxEncoding {
    match value.to_ascii_lowercase().as_str() {
        "proto" => TxEncoding::Proto,
        _ => TxEncoding::Json,
    }
}

/// 将字符串解析为 Compression 枚举，不区分大小写，未知值默认返回 None。
fn parse_compression(value: &str) -> Compression {
    match value.to_ascii_lowercase().as_str() {
        "gzip" => Compression::Gzip,
        _ => Compression::None,
    }
}

/// 将字符串解析为 SignAlgo 枚举，不区分大小写，未知值默认返回 Ed25519。
fn parse_sign_algo(value: &str) -> SignAlgo {
    match value.to_ascii_lowercase().as_str() {
        "secp256k1" => SignAlgo::Secp256k1,
        _ => SignAlgo::Ed25519,
    }
}

/// 将字符串解析为 SignMode 枚举，不区分大小写，未知值默认返回 Direct。
fn parse_sign_mode(value: &str) -> SignMode {
    match value.to_ascii_lowercase().as_str() {
        "legacy" => SignMode::Legacy,
        _ => SignMode::Direct,
    }
}

/// 将字符串解析为 BroadcastMode 枚举，不区分大小写，未知值默认返回 Sync。
fn parse_broadcast_mode(value: &str) -> BroadcastMode {
    match value.to_ascii_lowercase().as_str() {
        "async" => BroadcastMode::Async,
        "block" => BroadcastMode::Block,
        _ => BroadcastMode::Sync,
    }
}

/// 将字符串解析为 RateLimitStrategy 枚举，不区分大小写，未知值默认返回 TokenBucket。
fn parse_rate_limit_strategy(value: &str) -> RateLimitStrategy {
    match value.to_ascii_lowercase().as_str() {
        "leaky_bucket" => RateLimitStrategy::LeakyBucket,
        _ => RateLimitStrategy::TokenBucket,
    }
}

/// 将字符串解析为 AccountSelectionMode 枚举，不区分大小写，未知值默认返回 RoundRobin。
fn parse_account_selection_mode(value: &str) -> AccountSelectionMode {
    match value.to_ascii_lowercase().as_str() {
        "zipf" => AccountSelectionMode::Zipf,
        "random" => AccountSelectionMode::Random,
        _ => AccountSelectionMode::RoundRobin,
    }
}

/// 将字符串解析为 ExportFormat 枚举，不区分大小写，未知值默认返回 Json。
fn parse_export_format(value: &str) -> ExportFormat {
    match value.to_ascii_lowercase().as_str() {
        "csv" => ExportFormat::Csv,
        "prometheus" => ExportFormat::Prometheus,
        _ => ExportFormat::Json,
    }
}

/// 将逗号分隔的字符串解析为字符串向量，自动去除空白并过滤空值。
fn parse_list(value: String) -> Vec<String> {
    value
        .split(',')
        .map(|item| item.trim().to_string())
        .filter(|item| !item.is_empty())
        .collect()
}
