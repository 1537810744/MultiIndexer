# MultiIndexer —— Phase 1 阶段文档

> 日期：2026-05-24
> 阶段目标：搭建监听微服务 + 消息队列 + 伪索引测试服务，打通"监听 → 消息队列 → 消费"的全链路。

---

## 一、本阶段已实现的功能

| 模块 | 功能点 | 状态 |
|------|--------|------|
| **RabbitMQ** | 消息队列中间件，提供消息路由、缓冲、持久化 | 已部署（Docker） |
| **Listener Service** | 同时监听 3 条链（ETH / BSC / SOL） | 已实现（Mock 模式） |
| **Listener Service** | 按链的出块节奏独立并发运行（3 个 goroutine） | 已实现 |
| **Listener Service** | 将链上事件序列化为 JSON 并发布到 RabbitMQ | 已实现 |
| **Listener Service** | 单链 panic 自动恢复（不影响其他链） | 已实现 |
| **Listener Service** | 带指数退避的 RabbitMQ 重连机制 | 已实现 |
| **Fake-Indexer** | 消费 RabbitMQ 消息并原样打印到控制台 | 已实现 |
| **Fake-Indexer** | 按 routing key（eth.tx / bsc.tx / sol.tx）绑定队列 | 已实现 |
| **Docker Compose** | 一键启动全部服务（RabbitMQ + Listener + Fake-Indexer） | 已实现 |
| **Docker Compose** | 服务健康检查与启动顺序控制（depends_on + condition） | 已实现 |

---

## 二、架构与数据流

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  ETH Watcher    │     │  BSC Watcher    │     │  SOL Watcher    │
│  (12s 间隔)      │     │  (3s 间隔)       │     │  (400ms 间隔)   │
│  goroutine      │     │  goroutine      │     │  goroutine      │
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │  Publish JSON
                                 ▼
                    ┌──────────────────────────┐
                    │   RabbitMQ               │
                    │   Exchange: indexer.tx   │
                    │   Type: topic            │
                    │   Routing: *.tx          │
                    └────────────┬─────────────┘
                                 │ Consume
                                 ▼
                    ┌──────────────────────────┐
                    │   Fake-Indexer Service   │
                    │   Queue: fake-indexer-queue│
                    │   Bindings: eth.tx       │
                    │             bsc.tx       │
                    │             sol.tx       │
                    └──────────────────────────┘
```

### 数据流步骤

1. **生成事件**：每个 Watcher 按自己的定时器触发，调用 `generateMockEvents()` 生成 1~3 条模拟链上事件。
2. **序列化**：事件结构体通过 `json.Marshal()` 转为 JSON 字节流。
3. **发布**：`ch.Publish()` 将消息发送到 `indexer.tx` Exchange，routing key 为 `eth.tx` / `bsc.tx` / `sol.tx`。
4. **路由**：RabbitMQ Topic Exchange 根据 routing key 将消息投递到绑定了对应 key 的 `fake-indexer-queue`。
5. **消费**：Fake-Indexer 从队列中取出消息，打印 routing key、时间戳和完整 JSON 正文。

---

## 三、项目目录结构

```
MultiIndexer/
├── README.md                      # 项目整体架构文档
├── Step1.txt                      # 阶段任务要求
├── docker-compose.yml             # 容器编排配置
├── docs/
│   └── Phase1_阶段文档.md          # 本文件
├── listener/
│   ├── Dockerfile                 # 多阶段构建镜像
│   ├── go.mod                     # Go 模块定义
│   ├── go.sum                     # 依赖校验和（自动生成）
│   └── main.go                    # 监听服务源码（已加详细注释）
└── fake-indexer/
    ├── Dockerfile                 # 多阶段构建镜像
    ├── go.mod                     # Go 模块定义
    ├── go.sum                     # 依赖校验和（自动生成）
    └── main.go                    # 伪索引服务源码（已加详细注释）
```

---

## 四、如何启动

### 前置条件

- Docker Desktop 已安装并运行
- 本地端口 `5672`、`15672` 未被占用

### 启动命令

在项目根目录执行：

```bash
docker compose up --build -d
```

- `--build`：强制重新构建镜像（代码更新后需要）
- `-d`：后台运行（detached 模式）

### 查看服务状态

```bash
docker compose ps
```

预期看到三个容器均为 `running` 或 `healthy` 状态：
- `mq-indexer`（RabbitMQ）
- `listener-service`（监听服务）
- `fake-indexer-service`（伪索引服务）

### 查看日志（验证消息是否流通）

```bash
# 查看监听服务日志（确认消息已发出）
docker logs -f listener-service

# 查看伪索引服务日志（确认消息已收到并打印）
docker logs -f fake-indexer-service
```

### 停止全部服务

```bash
docker compose down
```

如需同时删除 RabbitMQ 数据卷（清空消息和队列）：

```bash
docker compose down -v
```

---

## 五、验证清单

| 检查项 | 验证方式 | 预期结果 |
|--------|----------|----------|
| RabbitMQ Web UI 可访问 | 浏览器打开 http://localhost:15672 | 登录页面，账号 `admin` / `admin` |
| Exchange 已创建 | RabbitMQ UI → Exchanges 标签 | 看到 `indexer.tx`，Type 为 `topic` |
| Queue 已创建 | RabbitMQ UI → Queues 标签 | 看到 `fake-indexer-queue` |
| Binding 正确 | 点击 `fake-indexer-queue` → Bindings | 绑定了 `eth.tx`、`bsc.tx`、`sol.tx` |
| 消息正在产生 | `docker logs -f listener-service` | 持续输出 `[Watcher-eth] Published tx=...` |
| 消息正在消费 | `docker logs -f fake-indexer-service` | 持续输出 `[FakeIndexer] --- Message #...` 和 JSON 内容 |
| 三条链同时运行 | 观察两条日志的时间戳 | ETH（12s）、BSC（3s）、SOL（400ms）各自独立触发 |
| 故障自愈 | 手动重启 RabbitMQ 容器 | listener 和 fake-indexer 自动重连，不崩溃 |

---

## 六、Mock 模式 vs 真实模式

| 维度 | 当前（Mock） | 未来（Real） |
|------|-------------|-------------|
| 区块来源 | 程序内部定时器生成 | 连接真实节点 WebSocket / RPC |
| 交易数据 | 随机生成的假哈希、假地址 | 链上真实交易、真实事件日志 |
| 事件类型 | Transfer / Mint / Burn / Approval 随机挑选 | 按合约 ABI 解析真实事件 |
| 金额 | 随机数字 | 链上实际转账金额 |
| 时间戳 | 当前系统时间 | 区块实际出块时间 |
| 确认机制 | 无（立即发布） | N 个区块确认后才发布，防链重组 |
| 链重组处理 | 未实现 | 检测分叉并发布 reorg_event |

---

## 七、关键技术决策说明

### 1. 为什么用 RabbitMQ 而不是 Kafka？
- 区块链出块稳定，日常吞吐量低（ETH 12s、BSC 3s、SOL 400ms）。
- RabbitMQ 单机部署简单、带管理界面、足够支撑当前规模。
- 如果未来扩展到 50+ 条链，迁移到 Kafka 的改造成本很低（Listener 和 Processor 之间是标准消息接口）。

### 2. 为什么三条链放在一个微服务里？
- ETH 和 BSC 同为 EVM 链，监听逻辑 90% 复用。
- Go 协程成本极低（~2KB 栈），单进程内跑 3 个 Watcher 绰绰有余。
- 统一配置、统一监控、统一日志，运维负担最小。
- 如果追求更强隔离，可以同代码多实例部署（Instance-1 跑 ETH+BSC，Instance-2 跑 SOL）。

### 3. 为什么用 Topic Exchange？
- 同一笔交易可以被多个 Processor 并行消费（互不阻塞）。
- routing key 支持模式匹配，扩展新链时只需新增 binding，无需改消费者代码。

---

## 八、已知限制与 TODO

1. **未连接真实区块链**：当前为 Mock 数据，Phase 2 将接入真实 RPC/WebSocket。
2. **auto-ack = true**：Fake-Indexer 自动确认消息，生产环境应改为手动 ack + 死信队列。
3. **无持久化消息验证**：RabbitMQ 重启后消息是否保留，需额外测试。
4. **无监控指标**：未接入 Prometheus / Grafana，Phase 5 补充。
5. **Processor 未实现**：真正的 NFT / Token / Alert Processor 在后续阶段开发。

---

## 九、阶段结论

Phase 1 已成功打通 **"监听 → 消息队列 → 消费 → 打印验证"** 的完整骨架链路：

- Listener 服务按三条链的各自节奏并发产生模拟事件；
- RabbitMQ 稳定路由消息；
- Fake-Indexer 正确消费并输出；
- Docker Compose 一键启动，服务具备自愈重连能力。

本阶段为后续接入真实链节点、开发业务 Processor、搭建 Query API 奠定了可靠的消息基础设施。

---
*Phase 1 结束。下一步：Phase 2 —— 接入真实区块链节点（ETH / BSC / SOL WebSocket/RPC）。*
