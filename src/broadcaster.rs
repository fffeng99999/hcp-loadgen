use anyhow::Result;
use async_trait::async_trait;
use cosmos_sdk_proto::cosmos::tx::v1beta1::service_client::ServiceClient;
use cosmos_sdk_proto::cosmos::tx::v1beta1::{BroadcastMode, BroadcastTxRequest};
use reqwest::Client;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use tonic::transport::Channel;

#[derive(Debug, Clone)]
pub struct SendResult {
    pub latency_ms: f64,
    pub success: bool,
}

#[async_trait]
pub trait Broadcaster: Send + Sync {
    async fn send(&self, payload: Vec<u8>) -> Result<SendResult>;
}

#[derive(Clone)]
pub struct HttpBroadcaster {
    client: Client,
    endpoint: String,
}

impl HttpBroadcaster {
    pub fn new(endpoint: String, concurrency: usize) -> Result<Self> {
        let client = Client::builder().pool_max_idle_per_host(concurrency).build()?;
        Ok(Self { client, endpoint })
    }
}

#[async_trait]
impl Broadcaster for HttpBroadcaster {
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

#[derive(Clone)]
pub struct GrpcBroadcaster {
    client: Arc<Mutex<ServiceClient<Channel>>>,
    mode: BroadcastMode,
}

impl GrpcBroadcaster {
    pub async fn new(endpoint: String) -> Result<Self> {
        let channel = Channel::from_shared(endpoint)?.connect().await?;
        let client = ServiceClient::new(channel);
        Ok(Self {
            client: Arc::new(Mutex::new(client)),
            mode: BroadcastMode::Sync,
        })
    }
}

#[async_trait]
impl Broadcaster for GrpcBroadcaster {
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
        Ok(SendResult {
            latency_ms,
            success,
        })
    }
}
