# MultiIndexer —— Phase 2 阶段文档

> 日期：2026-05-25
> 阶段目标：连接真实区块链节点（ETH / BSC / SOL），实时解析链上事件，写入 MySQL + Redis，搭建伪 API 验证数据流通。

---

## 一、本阶段已实现的功能

| 模块 | 功能点 | 状态 |
|------|--------|------|
| **Listener Service** | 连接真实 Ethereum RPC（publicnode），实时获取新区块 | 已实现 |
| **Listener Service** | 连接真实 BSC RPC（publicnode），实时获取新区块 | 已实现 |
| **Listener Service** | 连接真实 Solana RPC（publicnode），实时获取新区块 | 已实现 |
| **Listener Service** | EVM 链事件解析：ERC-20 Transfer/Approval、ERC-721 Transfer/Mint/Burn、ERC-1155 TransferSingle/TransferBatch | 已实现 |
| **Listener Service** | Solana SPL Token Transfer 解析（通过 jsonParsed 编码） | 已实现 |
| **Listener Service** | 事件分类（token / nft）并按 routing key 发布到 RabbitMQ | 已实现 |
| **Listener Service** | 保留 Mock 模式用于无 RPC 环境测试 | 已实现 |
| **Processor: Token** | 消费 token.* 事件，写入 MySQL events 表 + Redis 缓存 | 已实现 |
| **Processor: NFT** | 消费 nft.* 事件，写入 MySQL events 表 + Redis 缓存 | 已实现 |
| **Processor: Alert** | 消费全部事件（#），检测大额转账（USD 阈值可配）+ 监控钱包地址 | 已实现 |
| **Processor: Alert** | 内置常用代币价格表（USDT/USDC/DAI/WETH/WBTC/WBNB 等），估算 USD 价值 | 已实现 |
| **MySQL** | 4 张表：blocks、events、alerts、indexer_state | 已部署（Docker） |
| **Redis** | 热数据缓存：最新事件列表、最新区块号、事件计数器、告警列表 | 已部署（Docker） |
| **Fake API** | HTTP 服务（端口 9090），查询 MySQL + Redis 数据并返回 JSON | 已实现 |
| **Fake API** | 实时控制台打印所有 RabbitMQ 事件（docker logs 可见） | 已实现 |
| **Fake API** | Web 仪表盘页面（/）含 API 导航链接 | 已实现 |
| **Docker Compose** | 一键启动 RabbitMQ + MySQL + Redis 基础设施 | 已实现 |

---

## 二、架构与数据流

```
┌──────────────────────────────────────────────────────────────────┐
│                     REAL BLOCKCHAIN NODES                         │
│  Ethereum (12s)         BSC (3s)            Solana (400ms)        │
│  publicnode.com         publicnode.com      publicnode.com        │
└────────┬───────────────────┬──────────────────────┬───────────────┘
         │                   │                      │
         │  HTTP RPC Poll    │  HTTP RPC Poll       │  HTTP RPC Poll
         ▼                   ▼                      ▼
┌──────────────────────────────────────────────────────────────────┐
│  Listener Service (Go)                                           │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐           │
│  │ EVMWatcher   │  │ EVMWatcher   │  │ SolWatcher   │           │
│  │ (eth)        │  │ (bsc)        │  │ (sol)        │           │
│  │ eth_getLogs  │  │ eth_getLogs  │  │ getBlock     │           │
│  │ per-block    │  │ per-block    │  │ jsonParsed   │           │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘           │
│         │                  │                  │                   │
│         └──────────────────┼──────────────────┘                   │
│                            │ Publish JSON                         │
└────────────────────────────┼──────────────────────────────────────┘
                             │
                             ▼
              ┌──────────────────────────────┐
              │  RabbitMQ (Topic Exchange)   │
              │  Exchange: indexer.tx        │
              │  ┌────────────────────────┐  │
              │  │ token.processor.queue  │  │ ← binds: token.*
              │  │ nft.processor.queue    │  │ ← binds: nft.*
              │  │ alert.processor.queue  │  │ ← binds: #
              │  └────────────────────────┘  │
              └──────────────┬───────────────┘
                             │ Consume
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│  Processor Service (Go)                                          │
│  ┌────────────────┐ ┌────────────┐ ┌────────────────┐           │
│  │ TokenConsumer  │ │NFTConsumer │ │ AlertConsumer  │           │
│  │ (token.*)      │ │ (nft.*)    │ │ (# all events) │           │
│  └───────┬────────┘ └─────┬──────┘ └───────┬────────┘           │
│          │                │                │                      │
│          └────────────────┼────────────────┘                      │
│                           ▼                                       │
│          ┌────────────────┴────────────────┐                      │
│          ▼                                 ▼                      │
│   ┌──────────┐                      ┌──────────┐                 │
│   │  MySQL   │                      │  Redis   │                 │
│   │ (全量)   │                      │ (热缓存)  │                 │
│   └────┬─────┘                      └────┬─────┘                 │
└────────┼─────────────────────────────────┼────────────────────────┘
         │                                 │
         └──────────────┬──────────────────┘
                        │ Query
                        ▼
         ┌──────────────────────────────┐
         │  Fake API Service (Go)       │
         │  Port 9090                   │
         │  ┌────────────────────────┐  │
         │  │ GET /api/events        │  │ ← MySQL query
         │  │ GET /api/events/:chain │  │
         │  │ GET /api/alerts        │  │
         │  │ GET /api/stats         │  │
         │  │ GET /api/redis         │  │ ← Redis query
         │  │ GET /api/health        │  │
         │  │ GET /                  │  │ ← HTML dashboard
         │  └────────────────────────┘  │
         │  Console: real-time RMQ log  │
         └──────────────────────────────┘
```

### 数据流步骤

1. **Listener 轮询新区块**：ETH 每 12s、BSC 每 3s、SOL 每 1s 向 publicnode RPC 发起请求
2. **ETH/BSC**：对每个新区块调用 `eth_getLogs`，过滤 Transfer / TransferSingle / TransferBatch / Approval 事件
3. **SOL**：对每个新 Slot 调用 `getBlock`（jsonParsed 编码），解析 SPL Token transfer 指令
4. **事件分类**：
   - 3 个 Topic 的 Transfer → ERC-20 token（amount 在 data 中）
   - 4 个 Topic 的 Transfer → ERC-721 NFT（tokenId 是第 4 个 topic）
   - from = 0x0 → Mint；to = 0x0 → Burn
5. **发布到 RabbitMQ**：routing key 格式为 `{category}.{event_type}`（如 `token.transfer`、`nft.mint`）
6. **Processor 消费**：
   - Token Consumer 绑定 `token.*` → 写入 MySQL events 表 → 更新 Redis
   - NFT Consumer 绑定 `nft.*` → 写入 MySQL events 表 → 更新 Redis
   - Alert Consumer 绑定 `#` → 检测大额转账（USD 阈值） + 监控钱包地址 → 写入 alerts 表 → Redis 缓存
7. **Fake API 查询**：HTTP 端点直接查询 MySQL / Redis 返回 JSON，同时实时打印 RabbitMQ 消息到控制台

---

## 三、项目目录结构

```
MultiIndexer/
├── README.md                       # 项目整体架构文档
├── Phase1_阶段文档.md               # 阶段一总结
├── Phase2_阶段文档.md               # 本文件
├── init.sql                        # MySQL 初始化（4 张表）
├── docker-compose.yml              # 基础设施编排（RabbitMQ + MySQL + Redis）
├── listener/
│   ├── main.go                     # 入口：RabbitMQ 连接、启动 Watchers
│   ├── types.go                    # ChainEvent 结构体定义 + JSON 序列化
│   ├── evm.go                      # EVMWatcher：ETH/BSC 区块轮询 + 事件解析
│   ├── sol.go                      # SolWatcher：Solana Slot 轮询 + 交易解析
│   ├── mock.go                     # Mock 模式（Phase 1 遗留，保留用于无 RPC 测试）
│   ├── go.mod / go.sum
│   └── Dockerfile
├── processor/
│   ├── main.go                     # 入口：连接 RMQ/MySQL/Redis、启动三个 Consumer
│   ├── token.go                    # TokenConsumer：消费 token.* 事件
│   ├── nft.go                      # NFTConsumer：消费 nft.* 事件
│   ├── alert.go                    # AlertConsumer：大额检测 + 地址监控
│   ├── db.go                       # MySQL 连接池 + 写入/查询操作
│   ├── redis.go                    # Redis 连接 + 缓存操作
│   ├── go.mod / go.sum
│   └── Dockerfile
├── fake-api/
│   ├── main.go                     # HTTP 服务 + RMQ 实时订阅 + 数据查询
│   ├── go.mod / go.sum
│   └── Dockerfile
└── fake-indexer/                   # Phase 1 遗留（可删除）
```

---

## 四、如何启动

### 前置条件

- Docker Desktop 已安装并运行
- Go 1.22+ 已安装（用于编译服务）
- 端口 `5672`、`6379`、`3307`、`9090` 未被占用

### 步骤 1：启动基础设施

```bash
# 在项目根目录
docker compose up -d
```

启动 RabbitMQ（管理界面 http://localhost:15672，账号 admin/admin）、MySQL（端口 3307）、Redis（端口 6379）。

### 步骤 2：编译 Go 服务

```bash
# 编译 Listener
cd listener && go build -o listener.exe . && cd ..

# 编译 Processor
cd processor && go build -o processor.exe . && cd ..

# 编译 Fake API
cd fake-api && go build -o fake-api.exe . && cd ..
```

### 步骤 3：启动服务（Mock 模式，无 RPC 依赖）

```bash
# 终端 1：Listener（Mock 模式）
cd listener
set RABBITMQ_URL=amqp://admin:admin@localhost:5672/
set MODE=mock
listener.exe

# 终端 2：Processor
cd processor
set RABBITMQ_URL=amqp://admin:admin@localhost:5672/
set REDIS_URL=localhost:6379
set MYSQL_DSN=indexer:indexerpass@tcp(localhost:3307)/indexer
processor.exe

# 终端 3：Fake API
cd fake-api
set RABBITMQ_URL=amqp://admin:admin@localhost:5672/
set REDIS_URL=localhost:6379
set MYSQL_DSN=indexer:indexerpass@tcp(localhost:3307)/indexer
set LISTEN_PORT=9090
fake-api.exe
```

### 步骤 4：启动服务（Real 模式，连接真实区块链）

```bash
# 终端 1：Listener（Real 模式）
cd listener
set RABBITMQ_URL=amqp://admin:admin@localhost:5672/
set MODE=real
set ETH_RPC_URL=https://ethereum-rpc.publicnode.com
set BSC_RPC_URL=https://bsc-rpc.publicnode.com
set SOL_RPC_URL=https://solana-rpc.publicnode.com
listener.exe

# 终端 2 和 3 同上
```

### 步骤 5：验证数据流通

打开浏览器访问：
- **数据仪表盘**：http://localhost:9090/
- **事件列表**：http://localhost:9090/api/events
- **ETH 事件**：http://localhost:9090/api/events/eth
- **告警列表**：http://localhost:9090/api/alerts
- **统计信息**：http://localhost:9090/api/stats
- **Redis 缓存**：http://localhost:9090/api/redis
- **健康检查**：http://localhost:9090/api/health

查看实时事件日志：
```bash
docker logs -f listener-service    # 替代：listener 终端输出
docker logs -f processor-service   # 替代：processor 终端输出
docker logs -f fake-api-service    # 替代：fake-api 终端输出
```

---

## 五、核心设计决策

### 1. publicnode RPC 的 eth_getLogs 限制及应对

publicnode.com 对 `eth_getLogs` 的限制：**单次查询只能覆盖 1 个区块**（多区块范围会返回 `-32701` 错误）。

**解决方案**：EVM Watcher 对每个新区块单独调用 `eth_getLogs`，而非批量查询。虽然增加了 RPC 调用次数，但在轮询模式下（ETH 每 12s 仅 1 个新区块，BSC 每 3s 约 1 个新区块），额外的请求开销可忽略不计。

### 2. Solana Block 延迟问题

Solana 的 400ms 出块速度意味着每秒钟产生约 2.5 个 Slot。publicnode RPC 的区块数据存在延迟（最新几个 Slot 的数据尚未可用）。

**解决方案**：
- 使用 `finalized` 确认级别（而非 `confirmed`）
- 落后当前最新 Slot 约 50 个 Slot（~20 秒）以确保数据可用

### 3. ERC-20 vs ERC-721 的区分

两者使用相同的 Transfer 事件签名（`0xddf252ad...`），但：
- **ERC-20**：3 个 indexed 参数 → 3 个 Topic（signature, from, to），amount 在 data 中
- **ERC-721**：4 个 indexed 参数 → 4 个 Topic（signature, from, to, tokenId），无 amount

通过 Topic 数量即可区分。

### 4. Solana NFT 检测（启发式）

Solana 的 jsonParsed 编码无法直接区分 SPL Token 和 NFT。采用启发式方法：
- `transferChecked` 类型 + amount = 1 → 判定为 NFT
- 其余 → 判定为 Token

这个判断在 `transferChecked` 场景下较准确，但对普通 `transfer` 指令无法区分。有待后续改进。

### 5. USD 价值估算

Alert 处理器内置了常用代币的参考价格表（USDT/USDC/DAI/WETH/WBTC/WBNB 等）。对于已知合约地址，通过 `amount / 10^decimals * price` 估算 USD 价值。对于未知代币，使用链原生代币价格（ETH $2500, BNB $600, SOL $150）作为默认。

**限制**：这是静态价格表，不实时更新。生产环境应接入 Chainlink 或其他价格预言机。

### 6. 为什么 Go 服务不在 Docker 中运行？

本机 Docker 无法访问 Docker Hub（网络限制），无法拉取 `golang:1.22-alpine` 和 `alpine:latest` 镜像。因此采用混合部署：
- **Docker**：运行 RabbitMQ、MySQL、Redis（镜像已预缓存）
- **本机 Go**：运行 Listener、Processor、Fake API（编译为 Windows EXE）

如果未来网络环境改善，可以恢复 Docker 多阶段构建方式（Dockerfile 已提供）。

---

## 六、环境变量参考

### Listener

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/` | RabbitMQ 连接地址 |
| `MODE` | `mock` | 运行模式：`mock` 或 `real` |
| `ETH_RPC_URL` | `https://ethereum-rpc.publicnode.com` | ETH RPC 端点 |
| `BSC_RPC_URL` | `https://bsc-rpc.publicnode.com` | BSC RPC 端点 |
| `SOL_RPC_URL` | `https://solana-rpc.publicnode.com` | SOL RPC 端点 |
| `ETH_WALLET` | （空） | 监控的 ETH/BSC 钱包地址 |
| `SOL_WALLET` | （空） | 监控的 SOL 钱包地址 |

### Processor

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/` | RabbitMQ 连接地址 |
| `REDIS_URL` | `localhost:6379` | Redis 连接地址 |
| `MYSQL_DSN` | `indexer:indexerpass@tcp(localhost:3306)/indexer` | MySQL 连接字符串 |
| `ALERT_THRESHOLD_USD` | `100000` | 大额转账 USD 阈值 |
| `ETH_WALLET` | （空） | 监控的 ETH/BSC 钱包地址 |
| `SOL_WALLET` | （空） | 监控的 SOL 钱包地址 |

### Fake API

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/` | RabbitMQ 连接地址 |
| `REDIS_URL` | `localhost:6379` | Redis 连接地址 |
| `MYSQL_DSN` | `indexer:indexerpass@tcp(localhost:3306)/indexer` | MySQL 连接字符串 |
| `LISTEN_PORT` | `9090` | HTTP 服务端口 |

---

## 七、MySQL 表结构

### blocks
| 列 | 类型 | 说明 |
|----|------|------|
| id | BIGINT PK | 自增主键 |
| chain | VARCHAR(20) | eth / bsc / sol |
| block_number | BIGINT | 区块号 / Slot |
| block_hash | VARCHAR(100) | 区块哈希 |
| block_time | TIMESTAMP | 出块时间 |
| tx_count | INT | 交易数 |

### events
| 列 | 类型 | 说明 |
|----|------|------|
| id | BIGINT PK | 自增主键 |
| chain | VARCHAR(20) | 链标识 |
| block_number | BIGINT | 区块号 |
| tx_hash | VARCHAR(100) | 交易哈希 |
| log_index | INT | 日志索引 |
| category | VARCHAR(20) | token / nft |
| event_type | VARCHAR(50) | Transfer / Mint / Burn / Approval |
| contract_address | VARCHAR(100) | 合约地址 |
| from_address | VARCHAR(100) | 发送方 |
| to_address | VARCHAR(100) | 接收方 |
| token_id | VARCHAR(100) | NFT Token ID |
| amount | DECIMAL(65,0) | 金额（原始单位） |
| symbol | VARCHAR(20) | 代币符号 |
| decimals | INT | 代币精度 |
| raw_data | JSON | 原始事件 JSON |
| created_at | TIMESTAMP | 插入时间 |

唯一键：`(tx_hash, log_index)`

### alerts
| 列 | 类型 | 说明 |
|----|------|------|
| id | BIGINT PK | 自增主键 |
| chain | VARCHAR(20) | 链标识 |
| alert_type | VARCHAR(50) | large_transfer / watched_address |
| tx_hash | VARCHAR(100) | 交易哈希 |
| severity | VARCHAR(20) | low / medium / high / critical |
| detail | JSON | 告警详情 |
| notified | BOOLEAN | 是否已通知 |
| created_at | TIMESTAMP | 创建时间 |

### indexer_state
| 列 | 类型 | 说明 |
|----|------|------|
| chain | VARCHAR(20) PK | 链标识 |
| last_block | BIGINT | 最后处理的区块号 |
| updated_at | TIMESTAMP | 更新时间 |

---

## 八、Fake API 端点

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/` | HTML 仪表盘（含导航链接） |
| GET | `/api/health` | 健康检查（MySQL + Redis 状态） |
| GET | `/api/events` | 最新 50 条事件（所有链，MySQL 优先 → Redis 回退） |
| GET | `/api/events/eth` | ETH 最新事件 |
| GET | `/api/events/bsc` | BSC 最新事件 |
| GET | `/api/events/sol` | SOL 最新事件 |
| GET | `/api/alerts` | 最新 50 条告警 |
| GET | `/api/stats` | 统计摘要（MySQL + Redis 数据合并） |
| GET | `/api/redis` | Redis 缓存数据快照 |

---

## 九、测试验证结果（2026-05-25）

### Mock 模式测试

```
Listener (mock) → RabbitMQ → Processor → MySQL + Redis
```

- 116 条模拟事件成功写入 MySQL ✓
- Token / NFT / Approval / Mint / Burn 全部覆盖 ✓
- 三条链独立运行，互不阻塞 ✓

### Real 模式测试

```
Listener (real) → ETH/BSC/SOL RPC → RabbitMQ → Processor → MySQL + Redis → Fake API
```

- ETH（publicnode）：64 条真实事件，区块高度 ~25,169,791 ✓
- BSC（publicnode）：428 条真实事件，区块高度 ~100,280,144 ✓
- SOL（publicnode）：1,726 条真实事件，Slot ~421,991,502 ✓
- 总计：**2,218 条真实链上事件**在约 25 秒内被成功索引 ✓
- 892 条告警生成（阈值 $1,000 测试用；生产建议 $100,000） ✓
- Fake API 所有端点正常返回数据 ✓
- Redis 缓存（事件列表、区块号、统计数据）正常 ✓

---

## 十、已知限制与 TODO

1. **Solana NFT 检测精度**：当前使用启发式方法（amount=1 的 transferChecked 判定为 NFT），可能误判。精确检测需要查询 Token Metadata Program。
2. **代币价格静态**：Alert 处理器使用硬编码价格表，不实时更新。应接入价格预言机（Chainlink / Pyth）。
3. **区块确认机制**：当前获取即处理，未实现 N 区块确认等待。生产环境应等待确认数达标再入队（ETH 12、BSC 20、SOL 32）。
4. **链重组处理**：未实现 reorg 检测与回滚逻辑。Phase 3 补充。
5. **ETH/BSC 每区块独立 RPC 调用**：虽然绕过了 publicnode 的限制，但在高负载场景下 RPC 调用数较多。有专用节点时可恢复批量查询。
6. **Solana Block 延迟**：50 个 Slot 的 lag 意味着约 20 秒延迟。可根据 RPC 性能调整。
7. **Processor 的 auto-ack**：当前手动 ACK（正确处理失败重试），但未实现死信队列（DLQ）。
8. **Go 服务未容器化**：受网络限制，Go 服务在本机运行。网络恢复后可切回 Docker 多阶段构建。
9. **无 Prometheus 指标**：Phase 5 补充。
10. **RPC 容错与重试**：已实现指数退避重连，但未实现多 RPC 故障切换。

---

## 十一、阶段结论

Phase 2 已成功实现 **"真实区块链监听 → 事件解析 → 消息队列 → 存储（MySQL + Redis）→ 数据展示"** 的完整闭环：

- Listener 可同时监听 ETH / BSC / SOL 三条真实区块链，实时解析 Token Transfer、NFT Mint/Burn/Transfer、Approval 等事件；
- Processor 按业务类型（Token / NFT / Alert）分别消费、入库、缓存；
- 大额转账告警（USD 阈值可配置）和钱包地址监控已生效；
- Fake API 通过 HTTP 端点和控制台实时打印，完整展示了数据流通状态；
- 2,218 条真实链上事件在 25 秒测试窗口内成功索引，验证了系统的正确性。

本阶段为 Phase 3（链重组处理、区块确认机制、Processor 水平扩展）和 Phase 4（生产级 RESTful API、Nginx 负载均衡）奠定了坚实的数据基础。

---

*Phase 2 结束。下一步：Phase 3 —— 链重组检测与回滚、确认机制、死信队列、Processor 扩展。*
