// ============================================================================
// token.go — Token 事件消费者
// ============================================================================
// TokenConsumer 订阅 "token.*" routing key，处理所有链的代币转账事件：
//   - token.transfer（转账）
//   - token.mint（铸造）
//   - token.burn（销毁）
//   - token.approval（授权）
//
// 每条事件都会：
//   1. 写入 MySQL（持久化，用于历史查询）
//   2. 更新 indexer_state（记录每条链处理到哪个区块了）
//   3. 写入 Redis（热数据，用于实时查询）
//
// 【RabbitMQ 概念：Manual ACK（手动确认）】
// auto-ack=false 意味着消费者必须显式调用 msg.Ack() 来确认消息已被处理。
// 如果消费者崩溃或调用 Nack()，消息会重新入队，确保不丢失。
// 这就是 RabbitMQ 的 "at-least-once" 投递保证。
// ============================================================================

package main

import (
	"context"       // Go 上下文，用于超时控制和取消信号传递
	"encoding/json" // JSON 反序列化
	"fmt"           // 格式化日志
	"time"          // 模拟处理时间

	amqp "github.com/rabbitmq/amqp091-go" // RabbitMQ 客户端
)

// ============================================================================
// TokenConsumer — 代币事件消费者
// ============================================================================
type TokenConsumer struct {
	ch    *amqp.Channel  // RabbitMQ 通道
	db    *DBPool        // MySQL 连接池
	redis *RedisStore    // Redis 客户端
}

// NewTokenConsumer 创建代币事件消费者。
func NewTokenConsumer(ch *amqp.Channel, db *DBPool, redis *RedisStore) *TokenConsumer {
	return &TokenConsumer{ch: ch, db: db, redis: redis}
}

// ============================================================================
// Start() — 声明队列、绑定路由键、启动消费循环
// ============================================================================
func (c *TokenConsumer) Start() error {
	// ============================================================
	// 【RabbitMQ 概念：QueueDeclare — 声明队列】
	// 参数：
	//   "token.processor.queue" — 队列名称
	//   true (durable)    — 队列持久化，RabbitMQ 重启后队列还在
	//   false (autoDelete) — 没有消费者时自动删除队列（设 false 保留队列）
	//   false (exclusive) — 队列是否独占（设 false 允许多个消费者）
	//   false (noWait)    — 不等待服务器确认
	//   nil (args)        — 额外参数（如 TTL、死信队列等）
	//
	// 声明是幂等的：如果队列已存在且参数一致，不报错。
	// ============================================================
	q, err := c.ch.QueueDeclare(
		"token.processor.queue",
		true,  // durable — 服务器重启后保留队列
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,   // 无额外参数
	)
	if err != nil {
		return fmt.Errorf("declare token queue: %v", err)
	}

	// ============================================================
	// 【RabbitMQ 概念：QueueBind — 绑定队列到 Exchange】
	// QueueBind(队列名, routing_key, exchange名, noWait, args)
	// 这条绑定表示：Exchange "indexer.tx" 中 routing key 为 "token.transfer" 的消息
	// 会被路由到 "token.processor.queue" 队列。
	//
	// 我们绑定 4 个 routing key：transfer, mint, burn, approval
	// 这样所有代币事件都会进入同一个队列。
	// ============================================================
	bindings := []string{"token.transfer", "token.mint", "token.burn", "token.approval"}
	for _, key := range bindings {
		if err := c.ch.QueueBind(q.Name, key, "indexer.tx", false, nil); err != nil {
			return fmt.Errorf("bind %s: %v", key, err)
		}
	}

	// ============================================================
	// 【RabbitMQ 概念：Consume — 开始消费】
	// 参数：
	//   q.Name             — 队列名
	//   "token-processor"  — 消费者标签（用于管理和追踪）
	//   false (autoAck)    — 【关键】手动确认模式！必须显式 ACK
	//   false (exclusive)  — 不独占
	//   false (noLocal)    — 不接收本连接发布的消息（集群模式用）
	//   false (noWait)     — 不等待服务器确认
	//   nil                — 无额外参数
	//
	// 返回值 msgs 是一个 <-chan amqp.Delivery（只读通道），
	// 可以用 for range 遍历收到的消息。
	// ============================================================
	msgs, err := c.ch.Consume(
		q.Name,
		"token-processor",
		false, // auto-ack = false（手动确认模式）
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,
	)
	if err != nil {
		return fmt.Errorf("consume token: %v", err)
	}

	fmt.Println("[TokenProcessor] Started, bound to: token.*")

	// 【Go 并发核心：在新的 goroutine 中运行消费循环】
	// go 关键字启动新协程，消费循环不阻塞 Start() 返回。
	go c.consumeLoop(msgs)
	return nil
}

// ============================================================================
// consumeLoop — 消息消费主循环
// ============================================================================
// 【Go 语法：for range 通道】
// for msg := range msgs 会持续从通道中读取消息。
// 当通道被关闭（msgs 被 close）时，循环自动退出。
// 通道只要不关闭，这个 goroutine 会一直运行。
func (c *TokenConsumer) consumeLoop(msgs <-chan amqp.Delivery) {
	for msg := range msgs {
		c.handle(msg) // 处理每一条消息
	}
}

// ============================================================================
// handle() — 处理单条消息
// ============================================================================
func (c *TokenConsumer) handle(msg amqp.Delivery) {
	// 第一步：JSON 反序列化（[]byte → Go struct）
	var ev ChainEvent
	if err := json.Unmarshal(msg.Body, &ev); err != nil {
		fmt.Printf("[TokenProcessor] JSON parse error: %v\n", err)
		// 【RabbitMQ 概念：Nack（否定确认）】
		// Nack(false, false): 第一个 false=不批量拒绝，第二个 false=不重新入队
		// 消息格式错误，重新入队也没用，直接丢弃。
		msg.Nack(false, false) // 丢弃格式错误的消息
		return
	}

	// 第二步：只处理 token 类别的事件（以防万一收到 nft 事件）
	if ev.Category != "token" {
		msg.Ack(false) // 不是我们要的消息，确认并跳过
		return
	}

	// 第三步：写入 MySQL 数据库
	ctx := context.Background() // 创建空的 context（没有超时限制）

	if err := c.db.InsertEvent(&ev); err != nil {
		fmt.Printf("[TokenProcessor] MySQL insert error: %v (tx=%s)\n", err, ev.TxHash)
		// 【关键】MySQL 写入失败，Nack 并重新入队，等 MySQL 恢复后重试
		msg.Nack(false, true) // requeue=true：消息重新排到队列末尾
		return
	}

	// 第四步：更新索引状态（记录每条链同步到哪个区块了）
	c.db.UpdateIndexerState(ev.Chain, ev.BlockNumber)

	// 第五步：写入 Redis 缓存（非关键路径，失败不重试）
	if err := c.redis.CacheEvent(ctx, &ev); err != nil {
		fmt.Printf("[TokenProcessor] Redis cache error: %v\n", err)
		// Redis 写入失败不影响 ACK，因为 MySQL 已经写成功了
	}

	// 第六步：确认消息处理完毕
	// Ack(false): false 表示只确认当前这条消息，不批量确认
	msg.Ack(false)

	// 打印处理成功的日志
	unit := ev.Symbol
	if unit == "" {
		unit = ev.Chain
	}
	fmt.Printf("[TokenProcessor] %s %s | %s | %s -> %s | %s %s | tx=%s\n",
		ev.Chain, ev.EventType,
		shortHash(ev.ContractAddr),
		shortHash(ev.FromAddr), shortHash(ev.ToAddr),
		ev.Amount, unit, shortHash(ev.TxHash))

	// 模拟处理耗时（实际系统中这是真实的数据库 + Redis 写入时间）
	time.Sleep(10 * time.Millisecond)
}
