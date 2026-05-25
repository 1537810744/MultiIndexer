// ============================================================================
// mock.go — 模拟区块链观察者（Mock Watcher）
// ============================================================================
// 在 mock 模式下，我们不连接真实的区块链 RPC 节点，而是用程序内部的
// 随机数生成器产生看起来像链上交易的"假数据"。
//
// 为什么要保留 mock 模式？
//   1. 不需要 RPC 节点（没有 API Key 也能开发测试）
//   2. 不需要等真实出块（12 秒太慢，mock 可以任意调速）
//   3. 可以控制数据量和事件类型（压力测试时非常有用）
// ============================================================================

package main

import (
	"fmt"       // 格式化输出日志
	"math/rand" // 伪随机数生成器，用于生成模拟的哈希、金额等假数据
	"time"      // 定时器（Ticker），控制模拟出块节奏

	amqp "github.com/rabbitmq/amqp091-go" // RabbitMQ 客户端
)

// runMockWatcher 按照给定间隔定时生成模拟链上事件并发布到 RabbitMQ。
//
// 参数：
//   ch         — RabbitMQ Channel，消息通过它发送
//   chain      — 链名 (eth/bsc/sol)，影响生成的金额范围
//   interval   — 模拟出块间隔（eth=12秒, bsc=3秒, sol=400毫秒）
//   startBlock — 起始区块号，每次生成事件后自增1
func runMockWatcher(ch *amqp.Channel, chain string, interval time.Duration, startBlock uint64) {
	// ============================================================
	// 【Go 并发核心：defer + recover = 故障恢复】
	// panic 如果没有被 recover 捕获，会沿着调用栈一路传播，
	// 最终导致整个进程崩溃。
	// 这里的 defer + recover 组合实现了"故障隔离"：
	//   如果某个链的 Watcher 代码出了 bug 导致 panic，
	//   它不会拖垮其他链的 Watcher，而是自动重启自己。
	// ============================================================
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("[Watcher-%s] Panic recovered: %v, restarting...\n", chain, r)
			time.Sleep(2 * time.Second) // 等2秒再重启，避免疯狂重启
			go runMockWatcher(ch, chain, interval, startBlock)
		}
	}()

	fmt.Printf("[Watcher-%s] Started in MOCK mode, interval: %v\n", chain, interval)

	blockNumber := startBlock

	// ============================================================
	// 【Go 并发核心：time.NewTicker — 定时触发器】
	// time.NewTicker(interval) 创建一个"滴答器"，
	// 它每隔 interval 时间就向它的 .C 通道发送一个当前时间值。
	// 配合 for range 可以实现精确的定时执行。
	//
	// ticker.Stop() 很重要！如果不停止，ticker 的 goroutine 会一直运行，
	// 造成 goroutine 泄漏（goroutine 只增不减，最终内存耗尽）。
	// defer 确保函数退出时一定会 Stop。
	// ============================================================
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// 【Go 语法：for range 通道】
	// for range ticker.C 会阻塞等待，每次 ticker.C 有值就执行一次循环体。
	// 这个循环会一直运行，直到 ticker 被 Stop 且通道被关闭。
	for range ticker.C {
		// generateMockEvents 是本文件中的函数，生成 1~3 条模拟事件
		events := generateMockEvents(chain, blockNumber)

		// 遍历这批事件，逐条序列化并发送到 RabbitMQ
		for _, ev := range events {
			// 【Go 语法：for range 切片】
			// for i, v := range slice
			//   i = 索引（0, 1, 2...），下划线 _ 表示忽略
			//   v = 元素的副本（注意是副本，修改 v 不会影响原切片）

			body, err := ev.toJSON() // 把 Go 结构体转为 JSON 字节
			if err != nil {
				continue // 序列化失败，跳过这条，继续下一条
			}

			// ch.Publish 向 RabbitMQ 发布一条消息
			// 参数：Exchange名, routingKey, mandatory, immediate, 消息体
			err = ch.Publish(
				"indexer.tx",
				ev.routingKey(), // 如 "token.transfer"、"nft.mint"
				false,           // mandatory: 无法路由时是否返回错误
				false,           // immediate: 已废弃，设 false
				amqp.Publishing{
					ContentType: "application/json", // 告诉消费者：消息体是 JSON
					Body:        body,               // 消息正文（字节切片）
					Timestamp:   time.Now(),         // 消息发送时间
				},
			)
			if err != nil {
				fmt.Printf("[Watcher-%s] Publish error: %v\n", chain, err)
				continue
			}

			// 打印日志，确认消息已发出
			fmt.Printf("[Watcher-%s] Published %s.%s tx=%s block=%d\n",
				chain, ev.Category, ev.EventType, ev.TxHash, ev.BlockNumber)
		}
		blockNumber++ // 模拟区块号递增
	}
}

// generateMockEvents 生成 1~3 条随机的模拟链上事件。
// 这是一个"测试数据工厂"，产生的数据虽然哈希和地址是随机的，
// 但在结构上和真实链上事件完全一致。
func generateMockEvents(chain string, blockNum uint64) []ChainEvent {
	// rand.Intn(3) 返回 [0, 2] 的随机整数，+1 后变成 [1, 3]
	count := rand.Intn(3) + 1

	// make 是 Go 的内置函数，用于创建 slice（切片）、map（映射）、channel（通道）
	// make([]ChainEvent, count) 创建长度为 count 的 ChainEvent 切片
	events := make([]ChainEvent, count)

	// 事件类型数组：ERC-20/NFT 合约中最常见的 4 种事件
	categories := []string{"token", "token", "nft"} // token 权重高于 nft（更常见）
	eventTypes := []string{"Transfer", "Mint", "Burn", "Approval"}

	for i := 0; i < count; i++ {
		// rand.Intn(len(arr)) 随机选一个元素
		category := categories[rand.Intn(len(categories))]
		evType := eventTypes[rand.Intn(len(eventTypes))]

		// 生成随机金额
		// Solana 代币精度通常 9 位（ETH 是 18 位），金额范围小一些
		amount := fmt.Sprintf("%d", rand.Int63n(1000000000000000))
		if chain == "sol" {
			amount = fmt.Sprintf("%d", rand.Int63n(1000000000))
		}

		tokenID := ""         // NFT Token ID（非 NFT 时为空）
		decimals := uint8(18) // 默认 18 位精度（ERC-20 标准）
		if category == "nft" {
			// NFT 每次转移 1 个，有 tokenID，精度为 0
			tokenID = fmt.Sprintf("%d", rand.Int63n(10000))
			decimals = 0
			amount = "1"
		}

		// 组装事件
		events[i] = ChainEvent{
			Chain:        chain,
			BlockNumber:  blockNum,
			BlockHash:    fmt.Sprintf("0x%s", randomHex(32)), // 32字节随机十六进制
			TxHash:       fmt.Sprintf("0x%s", randomHex(32)),
			LogIndex:     i,
			Category:     category,
			EventType:    evType,
			ContractAddr: fmt.Sprintf("0x%s", randomHex(20)), // 20字节随机地址
			FromAddr:     fmt.Sprintf("0x%s", randomHex(20)),
			ToAddr:       fmt.Sprintf("0x%s", randomHex(20)),
			TokenID:      tokenID,
			Amount:       amount,
			Decimals:     decimals,
			Timestamp:    time.Now().Unix(),
		}
		// raw_data 存一份完整 JSON，方便调试时查看原始数据
		events[i].RawData = events[i].toJSONString()
	}
	return events
}

// randomHex 生成指定长度的随机十六进制字符串。
// 例如 randomHex(16) 可能返回 "a3f7b2d8e901c4ab"。
//
// 【Go 语法：rune 类型】
// rune 是 Go 中表示 Unicode 码点的类型，实际上是 int32 的别名。
// 它能安全处理中文、emoji 等。这里用来存单字符也完全没问题。
func randomHex(n int) string {
	// 十六进制字符集：0-9 和 a-f
	letters := []rune("abcdef0123456789")

	// make([]rune, n) 创建长度为 n 的 rune 切片，初始值都是 0（rune 的零值）
	b := make([]rune, n)

	// 【Go 语法：for range 索引遍历】
	// for i := range b  只遍历索引（0, 1, 2...n-1），不需要值
	// 等价于 for i := 0; i < len(b); i++
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))] // 随机选一个字符
	}
	return string(b) // string(b) 把 rune 切片转为字符串
}
