// ============================================================================
// alert.go — 告警消费者
// ============================================================================
// AlertConsumer 订阅所有事件（routing key "#"），扫描异常并触发告警。
// 两种告警规则：
//   1. 大额转账检测：如果转账的美元估值超过阈值（默认 $100,000），触发告警
//   2. 监控地址活动：如果交易涉及用户指定的钱包地址，触发告警
//
// 【设计思路：为什么告警消费者订阅所有事件？】
// Alert 消费者需要扫描每一个事件（不管是什么类型和链），因为：
//   - 大额转账可能来自任何 Token
//   - 监控地址的活动可能涉及任何类型的事件（转账、NFT、授权等）
// 所以用 routing key "#" （匹配所有），相当于全量订阅。
//
// 【RabbitMQ 概念：routing key 通配符】
//   * （星号）— 匹配恰好一个词，如 "token.*" 匹配 "token.transfer" 但不匹配 "token.a.b"
//   # （井号）— 匹配零个或多个词，如 "#" 匹配所有 routing key
// ============================================================================

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ============================================================================
// AlertConsumer — 告警消费者结构体
// ============================================================================
type AlertConsumer struct {
	ch             *amqp.Channel // RabbitMQ 通道
	db             *DBPool       // MySQL 连接池
	redis          *RedisStore   // Redis 客户端
	thresholdUSD   float64       // 大额告警阈值（美元）
	watchAddrETH   string        // EVM 链要监控的钱包地址
	watchAddrSOL   string        // Solana 链要监控的钱包地址
}

// ============================================================================
// 代币价格表（硬编码，仅用于演示）
// ============================================================================
// 【设计说明：为什么用硬编码价格而不是实时查询？】
// 阶段 2 的目标是验证数据流是否通畅，不是实现生产级的价格服务。
// 生产环境应该接入 Chainlink、CoinGecko 等价格预言机获取实时价格。
// 这里的价格只是近似值，用于触发告警的演示。
var tokenPricesUSD = map[string]float64{
	// --- Ethereum 主网 ---
	"0xdac17f958d2ee523a2206206994597c13d831ec7": 1.0,      // USDT (Tether)
	"0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": 1.0,      // USDC (Circle)
	"0x6b175474e89094c44da98b954eedeac495271d0f": 1.0,      // DAI (Maker)
	"0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": 2500.0,   // WETH (Wrapped Ether)
	"0x2260fac5e5542a773aa44fbcfedf7c193bc2c599": 65000.0,  // WBTC (Wrapped Bitcoin)
	// --- BSC ---
	"0x55d398326f99059ff775485246999027b3197955": 1.0,      // USDT (BSC)
	"0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d": 1.0,      // USDC (BSC)
	"0xe9e7cea3dedca5984780bafc599bd69add087d56": 1.0,      // BUSD
	"0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c": 600.0,    // WBNB (Wrapped BNB)
	// --- 原生代币 ---
	"eth_native": 2500.0, // ETH 原生币
	"bsc_native": 600.0,  // BNB 原生币
	"sol_native": 150.0,  // SOL 原生币
}

// NewAlertConsumer 创建告警消费者。
func NewAlertConsumer(ch *amqp.Channel, db *DBPool, redis *RedisStore, threshold float64, watchETH, watchSOL string) *AlertConsumer {
	return &AlertConsumer{
		ch:           ch,
		db:           db,
		redis:        redis,
		thresholdUSD: threshold,
		watchAddrETH: watchETH,
		watchAddrSOL: watchSOL,
	}
}

// ============================================================================
// Start() — 声明告警队列、绑定路由、启动消费
// ============================================================================
func (c *AlertConsumer) Start() error {
	// 声明持久化告警队列
	q, err := c.ch.QueueDeclare(
		"alert.processor.queue",
		true,  // durable，重启后保留
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,
	)
	if err != nil {
		return fmt.Errorf("declare alert queue: %v", err)
	}

	// 【关键】用 "#" 绑定所有 routing key，订阅全部事件
	// "#" 是 RabbitMQ topic exchange 的通配符，匹配所有 routing key
	if err := c.ch.QueueBind(q.Name, "#", "indexer.tx", false, nil); err != nil {
		return fmt.Errorf("bind #: %v", err)
	}

	msgs, err := c.ch.Consume(
		q.Name,
		"alert-processor",
		false, // auto-ack = false（手动确认）
		false, false, false, nil,
	)
	if err != nil {
		return fmt.Errorf("consume alert: %v", err)
	}

	fmt.Println("[AlertProcessor] Started, bound to: # (all events)")
	fmt.Printf("[AlertProcessor] Large transfer threshold: $%.0f\n", c.thresholdUSD)
	fmt.Printf("[AlertProcessor] Watching ETH/BSC: %s\n", c.watchAddrETH)
	fmt.Printf("[AlertProcessor] Watching SOL: %s\n", c.watchAddrSOL)
	go c.consumeLoop(msgs)
	return nil
}

// consumeLoop — 消息消费主循环。
func (c *AlertConsumer) consumeLoop(msgs <-chan amqp.Delivery) {
	for msg := range msgs {
		c.handle(msg)
	}
}

// handle — 处理每条消息：执行两个告警检查。
func (c *AlertConsumer) handle(msg amqp.Delivery) {
	var ev ChainEvent
	if err := json.Unmarshal(msg.Body, &ev); err != nil {
		msg.Nack(false, false) // 格式错误，丢弃
		return
	}

	ctx := context.Background()

	// 检查 1：大额转账检测
	c.checkLargeTransfer(ctx, &ev)

	// 检查 2：监控地址活动检测
	c.checkWatchedAddress(ctx, &ev)

	// 告警处理完毕，确认消息
	msg.Ack(false)
	time.Sleep(5 * time.Millisecond)
}

// ============================================================================
// checkLargeTransfer() — 大额转账检测
// ============================================================================
func (c *AlertConsumer) checkLargeTransfer(ctx context.Context, ev *ChainEvent) {
	// 只关心 Token 的 Transfer（NFT 转账金额通常是 1，不需要大额检测）
	if ev.Category != "token" || ev.EventType != "Transfer" {
		return
	}
	if ev.Amount == "" || ev.Amount == "0" {
		return
	}

	// 估算交易的美金价值
	usdValue := c.estimateUSD(ev)
	if usdValue >= c.thresholdUSD {
		// 根据超出阈值的倍数定严重级别
		severity := "medium"             // 默认：超过阈值 1x
		if usdValue >= c.thresholdUSD*10 {
			severity = "high"            // 超过阈值 10x
		}
		if usdValue >= c.thresholdUSD*100 {
			severity = "critical"        // 超过阈值 100x
		}

		// 构建告警详情（JSON 格式存在数据库里）
		detail := fmt.Sprintf(
			`{"chain":"%s","tx_hash":"%s","from":"%s","to":"%s","amount":"%s","symbol":"%s","estimated_usd":%.2f,"contract":"%s"}`,
			ev.Chain, ev.TxHash, ev.FromAddr, ev.ToAddr, ev.Amount, ev.Symbol, usdValue, ev.ContractAddr,
		)

		// 写入 MySQL 告警表
		if err := c.db.InsertAlert(ev.Chain, "large_transfer", ev.TxHash, severity, detail); err != nil {
			fmt.Printf("[AlertProcessor] MySQL alert insert error: %v\n", err)
			return
		}

		// 写入 Redis 缓存
		if err := c.redis.CacheAlert(ctx, ev.Chain, "large_transfer", ev.TxHash, severity, detail); err != nil {
			fmt.Printf("[AlertProcessor] Redis alert cache error: %v\n", err)
		}

		// 打印大额转账告警（醒目标记）
		fmt.Printf("[AlertProcessor] 🚨 LARGE TRANSFER [%s] %s | %s -> %s | %s %s (~$%.0f) | tx=%s\n",
			severity, ev.Chain, shortHash(ev.FromAddr), shortHash(ev.ToAddr),
			ev.Amount, ev.Symbol, usdValue, shortHash(ev.TxHash))
	}
}

// ============================================================================
// checkWatchedAddress() — 监控地址活动检测
// ============================================================================
func (c *AlertConsumer) checkWatchedAddress(ctx context.Context, ev *ChainEvent) {
	// 根据链选择监控地址
	watched := c.watchAddrETH
	if ev.Chain == "sol" {
		watched = c.watchAddrSOL
	}
	if watched == "" {
		return // 没配置监控地址，跳过
	}

	// 检查事件是否涉及监控地址（发送方或接收方）
	if ev.FromAddr == watched || ev.ToAddr == watched {
		severity := "low"    // 监控地址活动默认为低严重度
		direction := "OUT"
		if ev.ToAddr == watched {
			direction = "IN" // 转入监控地址
		}

		detail := fmt.Sprintf(
			`{"chain":"%s","tx_hash":"%s","direction":"%s","from":"%s","to":"%s","category":"%s","event_type":"%s","amount":"%s","symbol":"%s"}`,
			ev.Chain, ev.TxHash, direction, ev.FromAddr, ev.ToAddr, ev.Category, ev.EventType, ev.Amount, ev.Symbol,
		)

		c.db.InsertAlert(ev.Chain, "watched_address", ev.TxHash, severity, detail)
		c.redis.CacheAlert(ctx, ev.Chain, "watched_address", ev.TxHash, severity, detail)

		fmt.Printf("[AlertProcessor] 👁 WATCHED ADDRESS [%s] | %s %s | tx=%s\n",
			direction, ev.Chain, ev.Category, shortHash(ev.TxHash))
	}
}

// ============================================================================
// estimateUSD() — 估算转账的美金价值
// ============================================================================
// Amount 已在 Listener 中标准化（原始值 / 10^decimals），
// 所以这里直接解析为 float64 即可，不再需要 big.Int 除法。
func (c *AlertConsumer) estimateUSD(ev *ChainEvent) float64 {
	// 尝试从已知代币价格表中查找该合约的价格
	lowerAddr := ev.ContractAddr
	if price, ok := tokenPricesUSD[lowerAddr]; ok {
		// Amount 已标准化（如 "1.5" = 1.5 USDC），直接用 strconv 解析
		f, err := strconv.ParseFloat(ev.Amount, 64)
		if err != nil {
			return 0
		}
		return f * price
	}

	// 如果查不到合约价格，尝试按原生代币估算
	if ev.Symbol == "" && (ev.ContractAddr == "" || ev.Category == "token") {
		f, err := strconv.ParseFloat(ev.Amount, 64)
		if err != nil {
			return 0
		}
		if price, ok := tokenPricesUSD[ev.Chain+"_native"]; ok {
			return f * price
		}
	}

	return 0 // 无法估算
}
