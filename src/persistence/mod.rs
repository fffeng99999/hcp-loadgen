/// 持久化模块，封装数据库交互与数据存储逻辑。
///
/// - `storage`: 提供 PostgreSQL 连接池管理、账户加载、交易批量写入及 schema 初始化功能。
pub mod storage;
