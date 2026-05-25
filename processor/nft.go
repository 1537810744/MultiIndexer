// ============================================================================
// nft.go — NFT 事件消费者
// ============================================================================
// NFTConsumer 订阅 "nft.*" routing key，处理所有链的 NFT 事件：
//   - nft.transfer（NFT 转移）
//   - nft.mint（NFT 铸造 — 从 0x0 地址转出）
//   - nft.burn（NFT 销毁 — 转入 0x0 地址）
//
// 和 TokenConsumer 采用相同的处理模式：
//   手动 ACK → MySQL 持久化 → Redis 缓存 → ACK 确认
//
// 【区块链概念：NFT vs Token 的区别】
// NFT（Non-Fungible Token，非同质化代币）和普通 Token 的关键区别：
//   1. 每个 NFT 是唯一的（有 token_id），不能像代币那样互换
//   2. NFT 转账金额通常是 1（转移整个 NFT）
//   3. NFT 标准：EVM 上主要是 ERC-721 和 ERC-1155，Solana 上是 Metaplex
// ============================================================================

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// NFTConsumer — NFT 事件消费者，结构和 TokenConsumer 完全对称。
type NFTConsumer struct {
	ch    *amqp.Channel
	db    *DBPool
	redis *RedisStore
}

// NewNFTConsumer 创建 NFT 事件消费者。
func NewNFTConsumer(ch *amqp.Channel, db *DBPool, redis *RedisStore) *NFTConsumer {
	return &NFTConsumer{ch: ch, db: db, redis: redis}
}

// Start 声明 NFT 队列、绑定路由键、启动消费循环。
func (c *NFTConsumer) Start() error {
	// 声明持久化队列 "nft.processor.queue"
	q, err := c.ch.QueueDeclare(
		"nft.processor.queue",
		true,  // durable = 持久化，RabbitMQ 重启后队列保留
		false, // auto-delete = false，即使没有消费者也保留队列
		false, // exclusive = false，允许多个消费者共享
		false, // no-wait = false，阻塞等待服务器确认
		nil,   // 无额外参数
	)
	if err != nil {
		return fmt.Errorf("declare nft queue: %v", err)
	}

	// 绑定 NFT 相关的 routing key
	// 注意：没有 nft.approval（NFT 通常不单独触发 Approval 事件）
	bindings := []string{"nft.transfer", "nft.mint", "nft.burn"}
	for _, key := range bindings {
		if err := c.ch.QueueBind(q.Name, key, "indexer.tx", false, nil); err != nil {
			return fmt.Errorf("bind %s: %v", key, err)
		}
	}

	// 开始消费，auto-ack=false（手动确认模式）
	msgs, err := c.ch.Consume(
		q.Name,
		"nft-processor", // 消费者标签
		false,            // auto-ack = false
		false, false, false, nil,
	)
	if err != nil {
		return fmt.Errorf("consume nft: %v", err)
	}

	fmt.Println("[NFTProcessor] Started, bound to: nft.*")
	go c.consumeLoop(msgs)
	return nil
}

// consumeLoop — 持续从通道读取并处理消息。
func (c *NFTConsumer) consumeLoop(msgs <-chan amqp.Delivery) {
	for msg := range msgs {
		c.handle(msg)
	}
}

// handle — 处理单条 NFT 消息。
func (c *NFTConsumer) handle(msg amqp.Delivery) {
	var ev ChainEvent
	if err := json.Unmarshal(msg.Body, &ev); err != nil {
		fmt.Printf("[NFTProcessor] JSON parse error: %v\n", err)
		msg.Nack(false, false) // 格式错误，丢弃不重试
		return
	}

	// 过滤：只处理 nft 类别
	if ev.Category != "nft" {
		msg.Ack(false) // 不关我们事的消息，确认跳过
		return
	}

	ctx := context.Background()

	// 写入 MySQL
	if err := c.db.InsertEvent(&ev); err != nil {
		fmt.Printf("[NFTProcessor] MySQL insert error: %v (tx=%s)\n", err, ev.TxHash)
		msg.Nack(false, true) // MySQL 故障，重新入队等待重试
		return
	}

	// 更新索引进度
	c.db.UpdateIndexerState(ev.Chain, ev.BlockNumber)

	// 写入 Redis 缓存（非关键路径）
	if err := c.redis.CacheEvent(ctx, &ev); err != nil {
		fmt.Printf("[NFTProcessor] Redis cache error: %v\n", err)
	}

	msg.Ack(false) // 处理成功，确认

	// NFT 事件的日志格式：展示合约地址、TokenID、流转方向
	unit := ev.Symbol
	if unit == "" {
		if ev.TokenID != "" {
			unit = "NFT#" + ev.TokenID
		} else {
			unit = "NFT"
		}
	}
	fmt.Printf("[NFTProcessor] %s %s | %s %s | %s -> %s | tx=%s\n",
		ev.Chain, ev.EventType,
		shortHash(ev.ContractAddr), unit,
		shortHash(ev.FromAddr), shortHash(ev.ToAddr),
		shortHash(ev.TxHash))

	time.Sleep(10 * time.Millisecond)
}
