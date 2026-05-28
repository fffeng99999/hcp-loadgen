/// 核心模块，包含交易负载生成、签名、调度和广播的核心逻辑。
///
/// - `broadcaster`: 定义广播器 trait 及 HTTP/gRPC 实现，负责将交易发送到区块链节点。
/// - `scheduler`: 调度器实现，按不同发送模式（固定间隔、突发、持续、抖动）分发交易。
/// - `signer`: 提供交易签名功能。
/// - `tx_builder`: 构造和编码交易，支持 Protobuf/JSON 及 Gzip 压缩。
pub mod broadcaster;
pub mod scheduler;
pub mod signer;
pub mod tx_builder;
