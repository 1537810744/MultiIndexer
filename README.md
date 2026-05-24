# MultiIndexer — 高并发多链监听索引器

> 监听 Ethereum / Solana / BSC 链上交易，实时解析 NFT 铸造、Token Transfer、大额告警等事件，对外提供高并发查询 API。

---

## 一、架构全景

```
                                    ┌─────────────────┐
                                    │   外部用户/DApp  │
                                    └────────┬────────┘
                                             │ HTTPS
                                    ┌────────▼────────┐
                                    │  Nginx          │
                                    │  (负载均衡)      │
                                    └────────┬────────┘
                                             │
                    ┌────────────────────────┼────────────────────────┐
                    │                        │                        │
           ┌────────▼────────┐      ┌────────▼────────┐      ┌────────▼────────┐
           │  API Instance 1 │      │  API Instance 2 │      │  API Instance N │
           │  (Query Service)│      │  (Query Service)│      │  (Query Service)│
           └────────┬────────┘      └────────┬────────┘      └────────┬────────┘
                    │                        │                        │
                    └────────────────────────┼────────────────────────┘
                                             │ 先查 Redis → 再查 MySQL
                                    ┌────────┴────────┐
                                    │                 │
                           ┌────────▼────────┐ ┌──────▼──────┐
                           │     Redis       │ │    MySQL    │
                           │   (热数据缓存)   │ │  (全量档案)  │
                           └─────────────────┘ └─────────────┘
                                    ▲
                                    │ 写入热数据 / 更新缓存
                    ┌───────────────┼───────────────┐
                    │               │               │
           ┌────────▼────────┐ ┌────▼─────┐ ┌───────▼────────┐
           │ NFT Processor   │ │Token Proc│ │ Alert Processor│
           │ (铸造/转移解析)  │ │(转账解析)│ │ (大额/黑名单)  │
           └────────┬────────┘ └────┬─────┘ └────────┬───────┘
                    │               │                │
                    └───────────────┼────────────────┘
                                    │ 消费 RabbitMQ
                           ┌────────▼────────┐
                           │    RabbitMQ     │
                           │   (消息队列)     │
                           │  ┌───────────┐  │
                           │  │ eth.tx    │  │
                           │  │ bsc.tx    │  │
                           │  │ sol.tx    │  │
                           │  └───────────┘  │
                           └────────┬────────┘
                                    │ 发布原始交易
                           ┌────────▼────────┐
                           │ Listener Service│
                           │  (监听微服务)    │
                           │  ┌───────────┐  │
                           │  │ ETH Watcher│  │
                           │  │ BSC Watcher│  │
                           │  │ SOL Watcher│  │
                           │  └───────────┘  │
                           └─────────────────┘
                                    │
                           ┌────────┴────────┐
                           │  WebSocket/RPC   │
                           │  多链节点连接    │
                           └─────────────────┘
```

---

## 二、分层设计决策

### 2.1 监听层（Listener Service）

**决策：一个微服务，内部多 Goroutine。**

为什么不是多个微服务？
- Ethereum / BSC 同为 EVM 链，监听逻辑 90% 复用，拆成多个服务是重复造轮子。
- Go 协程成本极低（~2KB 栈），单进程内跑 3 个 `ChainWatcher` 绰绰有余。
- 统一配置、统一监控、统一日志，运维负担最小。

为什么不是 Kafka？
- 区块链出块稳定（ETH 12s、BSC 3s、SOL 400ms），日常吞吐量很低，Kafka 的运维成本（KRaft、分区、消费组）得不偿失。
- RabbitMQ 足够支撑这个项目，单机部署、管理界面友好、支持多消费者路由。

**内部结构：**

```go
// 每个链一个 Watcher 协程
func (w *ETHWatcher) Run() {
    for {
        head := w.pollLatestBlock()      // 轮询/WS 获取最新区块头
        block := w.fetchBlock(head)      // 拉取完整区块数据
        txs := w.extractTxs(block)       // 提取关注的交易
        w.mq.Publish("eth.tx", txs)      // 推送到 RabbitMQ
        w.handleReorg(head)              // 检测链重组
    }
}
```

- **ETH Watcher**：通过 `eth_subscribe(newHeads)` WebSocket 监听，12 区块确认后推送。
- **BSC Watcher**：逻辑复用 ETH Watcher，换 RPC Endpoint 和确认数（20 区块）。
- **SOL Watcher**：使用 `blockSubscribe` 或轮询 `getBlock`，32 Slot 确认后推送。

**关键设计：**
- **连接池**：每个 Watcher 维护一个可复用的 HTTP/RPC 连接池，避免频繁建连。
- **确认机制**：交易不立即推送，达到安全确认数后才入队，防止链重组导致数据回滚。
- **重组处理**：内存缓存最近 20 个区块，检测到分叉时发布 `reorg_event` 到 RabbitMQ，Processor 消费后执行数据修正。

---

### 2.2 消息队列层（RabbitMQ）

**决策：RabbitMQ，按链分 Exchange，按业务类型分 Queue。**

```
┌─────────────────────────────────────────┐
│            eth.tx (Topic Exchange)       │
│  ┌─────────────┐  ┌─────────────┐       │
│  │ nft_queue   │  │ token_queue │       │
│  │ (binding:   │  │ (binding:   │       │
│  │  nft.*)     │  │  token.*)   │       │
│  └─────────────┘  └─────────────┘       │
│  ┌─────────────┐                        │
│  │ alert_queue │                        │
│  │ (binding:   │                        │
│  │  alert.*)   │                        │
│  └─────────────┘                        │
└─────────────────────────────────────────┘
```

- **Exchange**：`eth.tx`、`bsc.tx`、`sol.tx` 三个 Topic Exchange。
- **Queue**：每个 Processor 声明自己的 Queue，通过 routing key 绑定（如 `nft.*`、`token.*`、`alert.*`）。
- **优势**：同一笔交易可以被多个 Processor 并行消费，互不阻塞。
- **持久化**：Queue 和 Message 都设置 `durable=true`，RabbitMQ 重启不丢数据。
- **ACK**：Processor 处理成功后手动 ACK，失败则 NACK + 重新入队（限重试 3 次，死信队列收纳）。

---

### 2.3 处理层（Processor Services）

**决策：按业务域拆成 3 个独立微服务。**

| 服务 | 职责 | 消费 Queue | 输出 |
|------|------|-----------|------|
| `nft-processor` | 解析 ERC-721 / ERC-1155 / Metaplex 铸造、转移、销毁 | `eth.nft_queue`, `bsc.nft_queue`, `sol.nft_queue` | MySQL + Redis |
| `token-processor` | 解析 ERC-20 / SPL Transfer、Approval、Burn | `eth.token_queue`, `bsc.token_queue`, `sol.token_queue` | MySQL + Redis |
| `alert-processor` | 大额阈值检测、地址黑名单命中、异常频率检测 | `eth.alert_queue`, `bsc.alert_queue`, `sol.alert_queue` | Redis + Webhook |

**为什么拆分？**
- **独立扩展**：NFT 解析可能需要调用 IPFS/Arweave，速度慢，需要更多实例；Alert 逻辑轻量，实例少。
- **独立发布**：Token 合约 ABI 更新时，只需要重启 `token-processor`，不影响 NFT 和 Alert。
- **故障隔离**：IPFS 超时导致 `nft-processor` 卡住，`token-processor` 照样正常运行。

**内部流水线：**

```
RabbitMQ Consumer (1 个协程)
    → 投递到解析协程池 (N 个协程)
        → 解析完成后投递到写入协程 (M 个协程)
            → MySQL 写入 + Redis 更新
```

- **解析协程池**：用 Go channel 实现简易协程池，防止并发过高打爆 RPC。
- **幂等性**：所有写入以 `tx_hash + log_index` 为唯一键，`INSERT IGNORE` 或 `ON DUPLICATE KEY UPDATE`。
- **顺序性**：同一合约的事件按区块高度顺序处理，RabbitMQ 按 `contract_address` 分区（Single Active Consumer 或自定义分片）。

---

### 2.4 查询层（Query Service）

**决策：无状态多实例 + Nginx 负载均衡。**

- **无状态**：不持有任何业务状态，所有数据来自 Redis / MySQL，水平扩展只需加实例。
- **负载均衡**：Nginx 轮询（Round-Robin）分发，支持加权、健康检查。

**查询路径：**

```
Request → Nginx → API Instance
    → [1] Redis Get (P99 < 1ms)
        → Hit: 直接返回 JSON
        → Miss: [2] MySQL Query → 回写 Redis (TTL 7 天) → 返回 JSON
```

**缓存策略：**
- **热数据**：最新 100 条交易、用户最近活动、聚合统计（如 24h 交易量），长期驻留 Redis + TTL。
- **温数据**：历史交易查询结果，缓存 1 小时。
- **冷数据**：极早期数据（半年前），直接走 MySQL，不缓存。
- **缓存更新**：由 Processor 主动写入/失效，API 服务只读 Redis，不写 Redis（职责分离）。

---

### 2.5 存储层

| 存储 | 角色 | 数据内容 | 配置 |
|------|------|---------|------|
| **Redis** | 热缓存 + 临时状态 | 最新区块高度、热点交易列表、用户余额快照、告警计数器、API 响应缓存 | 持久化 RDB（每日快照），可重建 |
| **MySQL** | 全量档案 + 关系查询 | 区块表、交易表、事件日志表、合约元数据表、地址标签表 | 主从复制，每日冷备 |

**核心表设计（简化）：**

```sql
-- 区块表
CREATE TABLE blocks (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    chain VARCHAR(20) NOT NULL,          -- eth / bsc / sol
    block_number BIGINT NOT NULL,
    block_hash VARCHAR(100) NOT NULL,
    block_time TIMESTAMP NOT NULL,
    tx_count INT DEFAULT 0,
    UNIQUE KEY uk_chain_number (chain, block_number)
);

-- 交易事件表（NFT + Token 统一存储，通过 event_type 区分）
CREATE TABLE events (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    chain VARCHAR(20) NOT NULL,
    block_number BIGINT NOT NULL,
    tx_hash VARCHAR(100) NOT NULL,
    log_index INT NOT NULL,
    event_type VARCHAR(50) NOT NULL,      -- Transfer / Mint / Burn / Approval
    contract_address VARCHAR(100) NOT NULL,
    from_address VARCHAR(100),
    to_address VARCHAR(100),
    token_id VARCHAR(100),                -- NFT 专用
    amount DECIMAL(78, 0),                -- Token 专用
    raw_data JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_tx_log (tx_hash, log_index),
    KEY idx_chain_contract (chain, contract_address),
    KEY idx_chain_from (chain, from_address),
    KEY idx_chain_to (chain, to_address),
    KEY idx_block_time (block_time)
);

-- 告警记录表
CREATE TABLE alerts (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    chain VARCHAR(20) NOT NULL,
    alert_type VARCHAR(50) NOT NULL,      -- large_transfer / blacklisted / frequency
    tx_hash VARCHAR(100) NOT NULL,
    severity VARCHAR(20),                 -- low / medium / high / critical
    detail JSON,
    notified BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    KEY idx_chain_type (chain, alert_type),
    KEY idx_created (created_at)
);
```

---

## 三、数据流详解

### 3.1 正常流程（以一笔 ERC-20 Transfer 为例）

```
1. ETH Watcher 通过 WebSocket 收到新区块 #18,000,000
        ↓
2. 拉取区块内所有交易，筛选出事件日志（Event Logs）
        ↓
3. 达到 12 区块确认后，将事件序列化为 JSON：
   {
     "chain": "eth",
     "block_number": 18000000,
     "tx_hash": "0xabc...",
     "log_index": 5,
     "event_type": "Transfer",
     "contract_address": "0xusdt...",
     "from": "0xA...",
     "to": "0xB...",
     "amount": "1000000000"
   }
        ↓
4. 发布到 RabbitMQ Exchange "eth.tx"，routing key = "token.transfer"
        ↓
5. Token Processor 从 "eth.token_queue" 消费到该消息
        ↓
6. 解析金额、更新地址余额、写入 MySQL events 表
        ↓
7. 同步更新 Redis 缓存（用户最新交易列表、合约统计）
        ↓
8. Alert Processor 同时消费到该消息，检测金额 > 阈值
        ↓
9. 写入 MySQL alerts 表，推送 Webhook 到外部系统
        ↓
10. 用户调用 API: GET /transactions?chain=eth&address=0xA
        ↓
11. Query Service 先查 Redis → 命中缓存 → 直接返回结果
```

### 3.2 链重组流程

```
1. ETH Watcher 检测到区块 #18,000,005 的 hash 与缓存不一致
        ↓
2. 确认发生重组，回滚深度 = 3 个区块
        ↓
3. 发布 reorg_event 到 RabbitMQ：
   {
     "type": "reorg",
     "chain": "eth",
     "from_block": 18000003,
     "to_block": 18000005,
     "new_blocks": [...]
   }
        ↓
4. Token / NFT Processor 消费 reorg_event
        ↓
5. 删除 MySQL 中 from_block ~ to_block 的 events 记录
        ↓
6. 重新处理 new_blocks 中的交易
        ↓
7. 更新 Redis 缓存（删除旧缓存，写入新数据）
```

---

## 四、部署拓扑（Docker Compose）

```yaml
version: "3.8"

services:
  # ─── 网关 ───
  nginx:
    image: nginx:alpine
    ports:
      - "8080:80"
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf
    depends_on:
      - query-service
    networks:
      - indexer-net

  # ─── 中间件 ───
  rabbitmq:
    image: rabbitmq:3-management-alpine
    ports:
      - "5672:5672"     # AMQP
      - "15672:15672"   # Management UI
    environment:
      RABBITMQ_DEFAULT_USER: admin
      RABBITMQ_DEFAULT_PASS: admin
    volumes:
      - rabbitmq_data:/var/lib/rabbitmq
    networks:
      - indexer-net

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data
    networks:
      - indexer-net

  mysql:
    image: mysql:8.0
    ports:
      - "3306:3306"
    environment:
      MYSQL_ROOT_PASSWORD: rootpass
      MYSQL_DATABASE: indexer
      MYSQL_USER: indexer
      MYSQL_PASSWORD: indexerpass
    volumes:
      - mysql_data:/var/lib/mysql
      - ./init.sql:/docker-entrypoint-initdb.d/init.sql
    networks:
      - indexer-net

  # ─── 业务服务 ───
  listener:
    build: ./listener
    environment:
      - RABBITMQ_URL=amqp://admin:admin@rabbitmq:5672/
      - ETH_RPC=wss://eth-mainnet.g.alchemy.com/v2/xxx
      - BSC_RPC=wss://bsc-mainnet.nodereal.io/ws/v1/xxx
      - SOL_RPC=wss://api.mainnet-beta.solana.com
    depends_on:
      - rabbitmq
    networks:
      - indexer-net

  nft-processor:
    build: ./processor/nft
    environment:
      - RABBITMQ_URL=amqp://admin:admin@rabbitmq:5672/
      - REDIS_URL=redis:6379
      - MYSQL_DSN=indexer:indexerpass@tcp(mysql:3306)/indexer
    depends_on:
      - rabbitmq
      - redis
      - mysql
    deploy:
      replicas: 2
    networks:
      - indexer-net

  token-processor:
    build: ./processor/token
    environment:
      - RABBITMQ_URL=amqp://admin:admin@rabbitmq:5672/
      - REDIS_URL=redis:6379
      - MYSQL_DSN=indexer:indexerpass@tcp(mysql:3306)/indexer
    depends_on:
      - rabbitmq
      - redis
      - mysql
    deploy:
      replicas: 2
    networks:
      - indexer-net

  alert-processor:
    build: ./processor/alert
    environment:
      - RABBITMQ_URL=amqp://admin:admin@rabbitmq:5672/
      - REDIS_URL=redis:6379
      - MYSQL_DSN=indexer:indexerpass@tcp(mysql:3306)/indexer
      - WEBHOOK_URL=https://your-alert-endpoint.com/notify
    depends_on:
      - rabbitmq
      - redis
      - mysql
    networks:
      - indexer-net

  query-service:
    build: ./api
    environment:
      - REDIS_URL=redis:6379
      - MYSQL_DSN=indexer:indexerpass@tcp(mysql:3306)/indexer
    depends_on:
      - redis
      - mysql
    deploy:
      replicas: 3
    networks:
      - indexer-net

volumes:
  rabbitmq_data:
  redis_data:
  mysql_data:

networks:
  indexer-net:
    driver: bridge
```

---

## 五、技术栈

| 层级 | 选型 | 理由 |
|------|------|------|
| 监听服务 | Go 1.22+ | `go-ethereum`、`solana-go`、`gorilla/websocket` |
| 处理服务 | Go 1.22+ | 协程模型天然适合并发流水线 |
| API 服务 | Go + `gin` / `echo` | 高性能、生态成熟 |
| 消息队列 | RabbitMQ | 足够支撑当前规模，运维轻量，管理界面友好 |
| 缓存 | Redis | 已有，不引入新组件 |
| 数据库 | MySQL 8.0 | 结构化数据、B+ 树范围查询优秀 |
| 负载均衡 | Nginx | 成熟稳定，支持健康检查 |
| 容器化 | Docker + Docker Compose | 本地一键启动，K8s 就绪 |
| 监控（可选）| Prometheus + Grafana | 指标采集 + 可视化 |

---

## 六、API 设计（Query Service）

```
GET /health
    → 健康检查

GET /v1/blocks?chain={eth|bsc|sol}&limit={n}
    → 查询最新区块列表

GET /v1/transactions?chain={}&address={}&contract={}&page={}&page_size={}
    → 查询地址相关交易（先 Redis 后 MySQL）

GET /v1/events?chain={}&contract={}&event_type={}&from_block={}&to_block={}
    → 查询合约事件日志

GET /v1/nfts/contract/{contract_address}/tokens?owner={}&page={}
    → 查询 NFT 持有列表

GET /v1/alerts?chain={}&severity={}&page={}
    → 查询告警记录（内部管理用）

GET /v1/stats/volume?chain={}&contract={}&days={7}
    → 查询交易量统计（Redis 聚合缓存）
```

---

## 七、开发路线图

### Phase 1 — 骨架搭建
1. `docker-compose.yml` 跑通 Nginx + RabbitMQ + Redis + MySQL
2. 初始化 MySQL 表结构（`init.sql`）
3. 搭建 `listener`、`processor/*`、`api` 的 Go 模块骨架

### Phase 2 — 监听与消息
4. 实现 Ethereum Watcher（WebSocket 订阅 + 区块拉取 + RabbitMQ 发布）
5. BSC Watcher（复用 ETH 逻辑，换配置）
6. Solana Watcher（独立实现，基于 `blockSubscribe`）
7. RabbitMQ Exchange / Queue / Binding 初始化

### Phase 3 — 处理与存储
8. Token Processor：解析 ERC-20 Transfer，写入 MySQL + Redis
9. NFT Processor：解析 ERC-721 / ERC-1155 Mint / Transfer
10. Alert Processor：大额检测 + 黑名单匹配 + Webhook 推送
11. 实现链重组的检测与回滚逻辑

### Phase 4 — API 与优化
12. Query Service RESTful API 实现
13. Redis 缓存层接入（查询 + 写入）
14. Nginx 负载均衡配置
15. 压力测试与性能调优

### Phase 5 — 生产化
16. 日志结构化（JSON）+ 统一 Trace ID
17. Prometheus 指标埋点
18. 服务健康检查与优雅退出
19. K8s deployment YAML 编写

---

## 八、FAQ

**Q: 为什么 RabbitMQ 而不是 Kafka？**  
A: 区块链出块稳定（ETH 12s、BSC 3s），日常吞吐量不高。RabbitMQ 单机部署简单、管理界面直观、足够支撑当前规模。如果未来吞吐量暴涨（如监听 50+ 条链），再迁移到 Kafka 成本也很低（因为 Listener 和 Processor 之间是标准消息接口）。

**Q: Listener 一个服务挂了就全挂了，风险是不是太大？**  
A: 单个服务内 3 个 Watcher 协程是隔离的（一个 panic 不会拖垮整个进程，因为有 recover）。如果追求更强隔离，可以**同代码多实例部署**：Instance-1 配 ETH+BSC，Instance-2 配 SOL。这样既有代码复用，又有故障隔离。

**Q: 加一条新链（如 Base / Arbitrum）要改多少代码？**  
A: 如果是 EVM 链：① Listener 加一行配置（复用 ETH Watcher）；② RabbitMQ 加 Exchange（或复用现有）；③ Processor 无需改动。零业务代码修改，纯配置扩展。

**Q: Processor 消费不过来怎么办？**  
A: RabbitMQ 会堆积消息。通过 `docker compose scale token-processor=5` 水平扩容 Processor 实例。因为 Processor 是无状态的，扩容只需加实例数。

**Q: Redis 挂了查询怎么办？**  
A: API Service 检测到 Redis 连接失败时，自动降级直接查 MySQL，同时记录日志告警。Processor 检测到 Redis 故障时，跳过缓存写（允许短暂不一致），优先保证数据入库。
