// ============================================================================
// main.go — 处理器（Processor）的入口文件
// ============================================================================
// 处理器是整个 MultiIndexer 系统的"大脑"，负责：
//   1. 从 RabbitMQ 消费链上事件（由 Listener 发布）
//   2. 将事件写入 MySQL（持久化存储）和 Redis（热数据缓存）
//   3. 检测大额转账和监控地址活动，触发告警
//
// 【架构设计：三个独立消费者】
// Token 消费者 — 订阅 "token.*" routing key，处理代币转账
// NFT 消费者   — 订阅 "nft.*" routing key，处理 NFT 转移
// Alert 消费者 — 订阅 "#"（所有事件），扫描大额转账和监控地址
//
// 每个消费者使用独立的 RabbitMQ Channel（不是 Connection），
// 这样做的好处：一个消费者的 ACK 延迟不会阻塞另一个消费者。
//
// 【RabbitMQ 概念：Channel 隔离】
// Channel 是 Connection 上的虚拟连接，一个 Connection 可以有多个 Channel。
// 本程序的三个消费者各用独立的 Channel，实现消息处理的隔离。
// ============================================================================

package main

import (
	"encoding/json" // JSON 序列化/反序列化
	"fmt"           // 格式化输出日志
	"os"            // 操作系统接口，读取环境变量
	"strconv"       // 字符串转数字
	"time"          // 时间处理，用于重试等待

	amqp "github.com/rabbitmq/amqp091-go" // RabbitMQ Go 客户端
)

// ============================================================================
// ChainEvent — 从 RabbitMQ 消息中反序列化的事件结构体
// ============================================================================
// 这个结构体必须和 listener/types.go 中的 ChainEvent 保持字段一致，
// 否则 JSON 反序列化会失败（字段对不上）。
// 注意：这里没有方法（toJSON、routingKey 等），因为消费者只需要读取数据。
type ChainEvent struct {
	Chain        string `json:"chain"`
	BlockNumber  uint64 `json:"block_number"`
	BlockHash    string `json:"block_hash"`
	TxHash       string `json:"tx_hash"`
	LogIndex     int    `json:"log_index"`
	Category     string `json:"category"`
	EventType    string `json:"event_type"`
	ContractAddr string `json:"contract_address"`
	FromAddr     string `json:"from_address"`
	ToAddr       string `json:"to_address"`
	TokenID      string `json:"token_id"`
	Amount       string `json:"amount"`
	Symbol       string `json:"symbol"`
	Decimals     uint8  `json:"decimals"`
	RawData      string `json:"raw_data"`
	Timestamp    int64  `json:"timestamp"`
}

// ============================================================================
// main() — 处理器入口
// ============================================================================
func main() {
	// ============================================================
	// 第一步：读取配置（环境变量）
	// ============================================================
	amqpURL := getEnv("RABBITMQ_URL", "amqp://admin:admin@localhost:5672/")
	mysqlDSN := getEnv("MYSQL_DSN", "indexer:indexerpass@tcp(localhost:3307)/indexer")
	redisURL := getEnv("REDIS_URL", "localhost:6379")
	thresholdStr := getEnv("ALERT_THRESHOLD_USD", "100000") // 大额告警阈值（美元）
	ethWallet := getEnv("ETH_WALLET", "")                     // 要监控的 ETH/BSC 钱包地址
	solWallet := getEnv("SOL_WALLET", "")                     // 要监控的 Solana 钱包地址

	// strconv.ParseFloat 将字符串转为浮点数
	threshold, err := strconv.ParseFloat(thresholdStr, 64)
	if err != nil {
		threshold = 100000 // 默认 10 万美元触发告警
	}

	fmt.Println("==========================================")
	fmt.Println("[Processor] MultiIndexer Event Processor")
	fmt.Println("[Processor] Consumers: Token, NFT, Alert")
	fmt.Println("==========================================")

	// ============================================================
	// 第二步：连接 MySQL 数据库
	// ============================================================
	// panic 在不可恢复的错误时使用。MySQL 连不上，处理器没意义，直接退出。
	db, err := NewDBPool(mysqlDSN)
	if err != nil {
		panic(fmt.Sprintf("MySQL connection failed: %v", err))
	}
	defer db.Close()

	// ============================================================
	// 第三步：连接 Redis 缓存
	// ============================================================
	redis, err := NewRedisStore(redisURL)
	if err != nil {
		panic(fmt.Sprintf("Redis connection failed: %v", err))
	}
	defer redis.Close()

	// ============================================================
	// 第四步：连接 RabbitMQ 并启动三个消费者
	// ============================================================
	// 创建第一个 Channel（Token 消费者用）
	conn, ch := connectRabbitMQ(amqpURL)
	defer conn.Close()
	defer ch.Close()

	// 声明 Exchange（幂等操作，如果已存在则不做任何事）
	if err := ch.ExchangeDeclare(
		"indexer.tx", "topic", true, false, false, false, nil,
	); err != nil {
		panic(fmt.Sprintf("Failed to declare exchange: %v", err))
	}

	// ============================================================
	// 【RabbitMQ 概念：QoS（Quality of Service）— 消费限流】
	// ch.Qos(1, 0, false) 的含义：
	//   prefetchCount=1：每次只从 RabbitMQ 取 1 条消息，处理完（ACK）后再取下一条
	//   prefetchSize=0：不限制消息大小
	//   global=false：只对当前 Channel 有效
	//
	// 为什么要设 prefetchCount=1？
	//   1. 公平分发：处理快的消费者多拿消息，慢的少拿
	//   2. 内存控制：不会在消费者本地堆积大量未处理消息
	//   3. 故障恢复：如果消费者崩溃，未 ACK 的消息会回到队列，被其他消费者处理
	// ============================================================
	ch.Qos(1, 0, false)

	// --- Token 消费者 ---
	tokenConsumer := NewTokenConsumer(ch, db, redis)
	if err := tokenConsumer.Start(); err != nil {
		panic(fmt.Sprintf("Failed to start token consumer: %v", err))
	}

	// --- NFT 消费者（独立 Channel）---
	// 【设计思路：为什么 NFT 要另开 Channel？】
	// 如果 Token 和 NFT 共享一个 Channel，设了 QoS prefetchCount=1 后，
	// 两个消费者加起来也只能同时处理 1 条消息，性能很差。
	// 独立 Channel 意味着两个消费者各可以独立预取 1 条消息。
	nftConn, nftCh := connectRabbitMQ(amqpURL)
	defer nftConn.Close()
	defer nftCh.Close()
	nftCh.Qos(1, 0, false)

	nftConsumer := NewNFTConsumer(nftCh, db, redis)
	if err := nftConsumer.Start(); err != nil {
		panic(fmt.Sprintf("Failed to start NFT consumer: %v", err))
	}

	// --- Alert 消费者（又一个独立 Channel）---
	alertConn, alertCh := connectRabbitMQ(amqpURL)
	defer alertConn.Close()
	defer alertCh.Close()
	alertCh.Qos(1, 0, false)

	alertConsumer := NewAlertConsumer(alertCh, db, redis, threshold, ethWallet, solWallet)
	if err := alertConsumer.Start(); err != nil {
		panic(fmt.Sprintf("Failed to start alert consumer: %v", err))
	}

	fmt.Println("[Processor] All consumers started. Processing events...")
	fmt.Println("[Processor] Token Queue: token.processor.queue")
	fmt.Println("[Processor] NFT Queue:   nft.processor.queue")
	fmt.Println("[Processor] Alert Queue: alert.processor.queue")

	// 【Go 语法：select {} 永久阻塞】
	// main goroutine 阻塞在这里，让后台的消费者 goroutine 持续运行。
	// select {} 不消耗 CPU（和 for {} 不同），它会让出 CPU 时间片。
	select {}
}

// ============================================================================
// 工具函数
// ============================================================================

// getEnv 读取环境变量，不存在时返回默认值。
func getEnv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

// connectRabbitMQ 连接到 RabbitMQ，带指数退避重试。
// 返回 (Connection, Channel)，和 listener 中的同名函数逻辑一致。
func connectRabbitMQ(amqpURL string) (*amqp.Connection, *amqp.Channel) {
	var conn *amqp.Connection
	var err error

	// 指数退避重试：最多 10 次
	for i := 0; i < 10; i++ {
		conn, err = amqp.Dial(amqpURL) // TCP 连接
		if err == nil {
			break
		}
		fmt.Printf("[RabbitMQ] Connect attempt %d failed: %v, retrying...\n", i+1, err)
		time.Sleep(time.Duration(i+1) * time.Second) // 第1次等1秒，第2次等2秒...
	}
	if err != nil {
		panic(fmt.Sprintf("Failed to connect to RabbitMQ: %v", err))
	}

	// 在连接上创建 Channel
	ch, err := conn.Channel()
	if err != nil {
		panic(fmt.Sprintf("Failed to open channel: %v", err))
	}

	fmt.Println("[RabbitMQ] Connected successfully")
	return conn, ch
}

// shortHash 截取哈希的前6位和后4位，中间用 ... 省略。
// 例如：0xa1b2c3d4e5f6...abcd → "0xa1b2...abcd"
func shortHash(hash string) string {
	if len(hash) >= 12 {
		return hash[:6] + "..." + hash[len(hash)-4:]
	}
	return hash
}

// 【Go 语法：空白导入（blank import）的变体】
// var _ = json.Marshal 确保 encoding/json 包被导入（防止 go fmt 自动删除 import）。
// 实际上 json.Marshal 并没用到，这只是保留导入的一种技巧。
var _ = json.Marshal
