use anyhow::Result;
use async_trait::async_trait;
use crate::config::BroadcastMode as ConfigBroadcastMode;
use cosmos_sdk_proto::cosmos::tx::v1beta1::service_client::ServiceClient;
use cosmos_sdk_proto::cosmos::tx::v1beta1::{BroadcastMode as ProtoBroadcastMode, BroadcastTxRequest};
use reqwest::Client;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use tonic::transport::Channel;

/// 广播结果结构体，包含请求延迟和是否成功。
#[derive(Debug, Clone)]
pub struct SendResult {
    /// 从发送到收到响应的延迟（毫秒）
    pub latency_ms: f64,
    /// 请求是否被节点接受（HTTP 2xx 或 gRPC code == 0）
    pub success: bool,
}

/// 广播器 trait，定义交易发送的统一接口。
/// 实现者需保证线程安全（Send + Sync）。
#[async_trait]
pub trait Broadcaster: Send + Sync {
    /// 发送已编码的交易负载，返回发送结果。
    async fn send(&self, payload: Vec<u8>) -> Result<SendResult>;
}

/// HTTP 广播器，通过 REST API 将交易提交到节点。
#[derive(Clone)]
pub struct HttpBroadcaster {
    /// HTTP 客户端，复用连接池
    client: Client,
    /// 目标端点地址，如 "http://127.0.0.1:8080/tx"
    endpoint: String,
}

impl HttpBroadcaster {
    /// 创建新的 HTTP 广播器，配置连接池大小以支持高并发。
    pub fn new(endpoint: String, concurrency: usize) -> Result<Self> {
        let client = Client::builder().pool_max_idle_per_host(concurrency).build()?;
        Ok(Self { client, endpoint })
    }
}

#[async_trait]
impl Broadcaster for HttpBroadcaster {
    /// 通过 HTTP POST 发送交易负载，返回延迟和成功状态。
    async fn send(&self, payload: Vec<u8>) -> Result<SendResult> {
        let start = Instant::now();
        let resp = self.client.post(&self.endpoint).body(payload).send().await?;
        let latency_ms = start.elapsed().as_secs_f64() * 1000.0;
        let status = resp.status();
        Ok(SendResult {
            latency_ms,
            success: status.is_success(),
        })
    }
}

/// gRPC 广播器，通过 Cosmos SDK 的 gRPC 服务广播交易。
#[derive(Clone)]
pub struct GrpcBroadcaster {
    /// gRPC 客户端，使用 Mutex 保证并发安全
    client: Arc<Mutex<ServiceClient<Channel>>>,
    /// 广播模式（Async / Sync / Block）
    mode: ProtoBroadcastMode,
}

impl GrpcBroadcaster {
    /// 创建新的 gRPC 广播器，建立到指定端点的连接并配置广播模式。
    pub async fn new(endpoint: String, mode: ConfigBroadcastMode) -> Result<Self> {
        let channel = Channel::from_shared(endpoint)?.connect().await?;
        let client = ServiceClient::new(channel);
        let mode = match mode {
            ConfigBroadcastMode::Async => ProtoBroadcastMode::Async,
            ConfigBroadcastMode::Sync => ProtoBroadcastMode::Sync,
            ConfigBroadcastMode::Block => ProtoBroadcastMode::Block,
        };
        Ok(Self {
            client: Arc::new(Mutex::new(client)),
            mode,
        })
    }
}

#[async_trait]
impl Broadcaster for GrpcBroadcaster {
    /// 通过 gRPC BroadcastTx 接口发送交易，解析响应判断成功或失败。
    async fn send(&self, payload: Vec<u8>) -> Result<SendResult> {
        let start = Instant::now();
        let request = BroadcastTxRequest {
            tx_bytes: payload,
            mode: self.mode as i32,
        };
        let mut client = self.client.lock().await;
        let response = client.broadcast_tx(request).await?;
        let latency_ms = start.elapsed().as_secs_f64() * 1000.0;
        let success = response
            .get_ref()
            .tx_response
            .as_ref()
            .map(|tx| tx.code == 0)
            .unwrap_or(false);
        if let Some(tx) = response.get_ref().tx_response.as_ref() {
            if tx.code != 0 {
                eprintln!("grpc broadcast rejected: code={} log={}", tx.code, tx.raw_log);
            }
        }
        Ok(SendResult {
            latency_ms,
            success,
        })
    }
}
