# hcp-loadgen
          
`hcp-loadgen` 是一个用 Rust 编写的**高并发交易负载生成器**，用于向 HCP 区块链节点发送压力测试交易。以下是详细的代码结构和功能解析：

---

## 一、项目概览

| 属性 | 说明 |
|------|------|
| **语言** | Rust (Tokio 异步运行时) |
| **用途** | 区块链交易负载生成与性能测试 |
| **协议支持** | HTTP REST、gRPC |
| **数据持久化** | PostgreSQL |
| **指标输出** | JSON 日志、CSV、Prometheus |

---

## 二、模块结构

```
hcp-loadgen/
├── Cargo.toml              # 依赖配置
├── src/
│   ├── main.rs             # 程序入口与主流程编排
│   ├── config.rs           # 配置定义与加载
│   ├── types.rs            # 核心数据结构
│   ├── account_pool.rs     # 账户池管理
│   ├── metrics.rs          # 性能指标采集
│   ├── core/               # 核心引擎模块
│   │   ├── mod.rs
│   │   ├── scheduler.rs    # 调度器（发送模式控制）
│   │   ├── tx_builder.rs   # 交易构建器
│   │   ├── signer.rs       # 签名器
│   │   └── broadcaster.rs  # 广播器（HTTP/gRPC）
│   └── persistence/        # 持久化模块
│       ├── mod.rs
│       └── storage.rs      # PostgreSQL 存储
└── scripts/                # 辅助脚本
    ├── init_loadgen_db.py
    ├── insert_one_user.py
    └── seed_ten_users_data.py
```

---

## 三、各模块详解

### 1. [main.rs](file:///f:/hcp-project-experiment/hcp-loadgen/src/main.rs) — 主流程

主函数按以下顺序执行：

```rust
1. 加载配置 (load_config)
2. 初始化指标系统 (Metrics::new)
3. 连接数据库并加载/生成账户 (Storage::new → load_initial_state)
4. 配置账户选择模式 (Zipf/Random/RoundRobin)
5. 创建广播器 (HttpBroadcaster 或 GrpcBroadcaster)
6. 启动持久化后台任务 (tokio::spawn 接收交易记录)
7. 创建调度器并运行 (Scheduler::run)
8. 等待 Ctrl-C 信号，优雅关闭
9. 刷写最终结果到数据库
```

**核心设计**：
- 使用 `mpsc::channel` 解耦交易发送与持久化
- 通过 `AtomicU64` 实现背压控制（backpressure）
- 支持 `tokio::select!` 并发处理发送与信号

---

### 2. [config.rs](file:///f:/hcp-project-experiment/hcp-loadgen/src/config.rs) — 配置系统

配置通过 **TOML 文件 + 环境变量 + 命令行参数** 加载，主要字段：

| 配置项 | 说明 |
|--------|------|
| `protocol` | 发送协议：`Http` / `Grpc` |
| `mode` | 发送模式：`Fixed` / `Burst` / `Sustained` / `Jitter` |
| `target_tps` | 目标 TPS |
| `concurrency` | 并发数 |
| `worker_threads` | 工作线程数 |
| `tx_type` | 交易类型：`Transfer` / `Stake` / `ContractCall` |
| `tx_encoding` | 编码格式：`Proto` / `Json` |
| `compression` | 压缩：`None` / `Gzip` |
| `account_selection_mode` | 账户选择：`Zipf` / `Random` / `RoundRobin` |
| `database_url` | PostgreSQL 连接串 |
| `db_schema` | 数据库 Schema 前缀 |

---

### 3. [types.rs](file:///f:/hcp-project-experiment/hcp-loadgen/src/types.rs) — 核心数据结构

```rust
InMemoryAccount          # 内存账户（含 nonce、余额、私钥）
TransactionRecord        # 交易记录（哈希、延迟、时间戳等）
BalanceSnapshot          # 余额快照
AccountIdentity          # 账户身份
```

`InMemoryAccount` 使用 `AtomicU64` 管理 nonce，`RwLock` 管理余额，支持无锁并发读取。

---

### 4. [account_pool.rs](file:///f:/hcp-project-experiment/hcp-loadgen/src/account_pool.rs) — 账户池

管理所有测试账户，支持三种选择策略：

| 模式 | 实现 |
|------|------|
| **RoundRobin** | `AtomicUsize` 循环取模 |
| **Random** | `rand::thread_rng()` 随机选取 |
| **Zipf** | `WeightedIndex` 按幂律分布偏斜选取（模拟真实热点） |

使用 `DashMap` 存储账户，支持高并发无锁访问。

---

### 5. [core/scheduler.rs](file:///f:/hcp-project-experiment/hcp-loadgen/src/core/scheduler.rs) — 调度器

控制交易发送节奏，支持四种模式：

| 模式 | 行为 |
|------|------|
| **Fixed** | 固定间隔发送（按 target_tps 计算） |
| **Burst** | 突发批量发送 + 间隔休眠 |
| **Sustained** | 持续不间断发送（yield_now 让出 CPU） |
| **Jitter** | 固定间隔 + 随机抖动 |

**Worker 模型**：
- 启动 `worker_threads` 个异步任务
- 每个 Worker 维护本地缓冲区（`local_buffer`）
- 缓冲区满后批量发送到持久化通道
- 通过 `backlog_records` 实现背压：超过阈值时暂停分发

**交易处理流程**（`process_send`）：
```
选取账户 → 获取 nonce → 构建交易 → 签名 → 编码 → 广播
    ↓
记录指标（sent/success/reject/latency）
    ↓
更新账户余额（BUY/SELL 模拟）
    ↓
写入本地缓冲区
```

---

### 6. [core/tx_builder.rs](file:///f:/hcp-project-experiment/hcp-loadgen/src/core/tx_builder.rs) — 交易构建

- 支持 **Cosmos SDK** 标准交易格式（`MsgSend`）
- 使用 `prost` 进行 Protobuf 编码
- 支持 **JSON 编码** 和 **Gzip 压缩**
- Memo 字段携带 nonce 和随机 payload

---

### 7. [core/signer.rs](file:///f:/hcp-project-experiment/hcp-loadgen/src/core/signer.rs) — 签名器

极简实现：使用 `Sha256(private_key || data)` 生成签名。生产环境可替换为真实加密库（如 `secp256k1`、`ed25519`）。

---

### 8. [core/broadcaster.rs](file:///f:/hcp-project-experiment/hcp-loadgen/src/core/broadcaster.rs) — 广播器

定义 `Broadcaster` trait，两种实现：

| 实现 | 协议 | 特点 |
|------|------|------|
| `HttpBroadcaster` | HTTP POST | `reqwest` 客户端，连接池复用 |
| `GrpcBroadcaster` | gRPC | `tonic` 客户端，支持 Async/Sync/Block 三种广播模式 |

返回 `SendResult`（延迟 + 成功标志）。

---

### 9. [metrics.rs](file:///f:/hcp-project-experiment/hcp-loadgen/src/metrics.rs) — 指标系统

多维度指标采集：

| 指标 | 说明 |
|------|------|
| `sent` | 已发送交易数 |
| `success` | 成功交易数 |
| `reject` | 被拒绝/失败数 |
| `latency_hist` | HDR Histogram 延迟分布（P50/P90/P95/P99） |
| `cpu_percent` / `mem_bytes` | 系统资源占用 |

**输出方式**：
1. **JSON 日志**：定期打印到 stdout
2. **CSV 文件**：结构化写入
3. **Prometheus**：通过 `/metrics` HTTP 端点暴露

---

### 10. [persistence/storage.rs](file:///f:/hcp-project-experiment/hcp-loadgen/src/persistence/storage.rs) — 持久化

PostgreSQL 存储层，功能：

| 功能 | 说明 |
|------|------|
| `prepare_schema` | 自动创建 Schema 和表（accounts/balances/orders/trades） |
| `load_initial_state` | 从 DB 加载已有账户，或生成新账户 |
| `flush_trade_batch` | 批量写入交易记录（COPY BINARY 高效导入） |
| `flush_results_to_db` | 实验结束刷写余额和账户信息 |

**表结构**：
- `accounts`：账户基本信息
- `balances`：可用/冻结余额
- `orders`：订单记录
- `trades`：成交记录（含延迟）

使用 `tokio-postgres` 的 `BinaryCopyInWriter` 实现高效批量插入。

---

## 四、数据流全景

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Config    │────→│  Scheduler  │────→│   Workers   │
│  (TOML/CLI) │     │ (发送调度)   │     │ (交易构造)   │
└─────────────┘     └─────────────┘     └──────┬──────┘
                                                │
                    ┌───────────────────────────┘
                    ▼
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  PostgreSQL │←────│  Storage    │←────│  Channel    │
│  (持久化)    │     │ (批量写入)   │     │ (交易记录)   │
└─────────────┘     └─────────────┘     └──────▲──────┘
                                               │
                    ┌──────────────────────────┘
                    ▼
              ┌─────────────┐     ┌─────────────┐
              │ Broadcaster │────→│ Blockchain  │
              │(HTTP/gRPC)  │     │   Nodes     │
              └─────────────┘     └─────────────┘
                    │
                    ▼
              ┌─────────────┐
              │   Metrics   │────→ JSON / CSV / Prometheus
              │  (指标采集)  │
              └─────────────┘
```

---

## 五、关键技术亮点

1. **异步全链路**：基于 Tokio，从调度到广播到持久化全异步
2. **背压机制**：通过 `AtomicU64` 计数器防止内存无限增长
3. **批量优化**：Worker 本地缓冲 + COPY BINARY 批量写入
4. **多种负载模式**：Fixed/Burst/Sustained/Jitter 覆盖不同测试场景
5. **Zipf 分布**：模拟真实世界中交易地址热点
6. **多协议支持**：HTTP REST 和 gRPC 无缝切换
7. **资源监控**：内置 CPU/内存采集，Prometheus 原生暴露

---

## 六、辅助脚本

| 脚本 | 用途 |
|------|------|
| `init_loadgen_db.py` | 初始化数据库 |
| `insert_one_user.py` | 插入单个测试用户 |
| `seed_ten_users_data.py` | 批量插入 10 个测试用户 |
