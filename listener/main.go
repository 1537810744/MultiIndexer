package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ChainEvent 链上事件结构
type ChainEvent struct {
	Chain         string `json:"chain"`
	BlockNumber   uint64 `json:"block_number"`
	TxHash        string `json:"tx_hash"`
	LogIndex      int    `json:"log_index"`
	EventType     string `json:"event_type"`
	ContractAddr  string `json:"contract_address"`
	FromAddr      string `json:"from_address"`
	ToAddr        string `json:"to_address"`
	Amount        string `json:"amount"`
	Timestamp     int64  `json:"timestamp"`
}

func main() {
	amqpURL := getEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
	mode := getEnv("MODE", "mock")

	conn, ch := connectRabbitMQ(amqpURL)
	defer conn.Close()
	defer ch.Close()

	// 声明 topic exchange
	if err := ch.ExchangeDeclare(
		"indexer.tx", // name
		"topic",      // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	); err != nil {
		panic(fmt.Sprintf("Failed to declare exchange: %v", err))
	}

	fmt.Printf("[Listener] Started in %s mode\n", mode)
	fmt.Println("[Listener] Watching chains: ETH, BSC, SOL")

	// 三条链的配置：名称 + 出块间隔（mock 模式下模拟）
	chains := []struct {
		name     string
		interval time.Duration
		startBlk uint64
	}{
		{"eth", 12 * time.Second, 18000000},
		{"bsc", 3 * time.Second, 30000000},
		{"sol", 400 * time.Millisecond, 250000000},
	}

	for _, c := range chains {
		go runWatcher(ch, c.name, c.interval, c.startBlk, mode)
	}

	// 阻塞主 goroutine
	select {}
}

// runWatcher 每个链一个独立的 watcher goroutine
func runWatcher(ch *amqp.Channel, chain string, interval time.Duration, startBlock uint64, mode string) {
	// panic 恢复，避免单个 watcher 崩溃影响其他链
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("[Watcher-%s] Panic recovered: %v, restarting...\n", chain, r)
			time.Sleep(2 * time.Second)
			go runWatcher(ch, chain, interval, startBlock, mode)
		}
	}()

	fmt.Printf("[Watcher-%s] Started, interval: %v, mode: %s\n", chain, interval, mode)

	blockNumber := startBlock
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		if mode == "mock" {
			events := generateMockEvents(chain, blockNumber)
			for _, ev := range events {
				body, err := json.Marshal(ev)
				if err != nil {
					fmt.Printf("[Watcher-%s] JSON marshal error: %v\n", chain, err)
					continue
				}

				routingKey := chain + ".tx"
				err = ch.Publish(
					"indexer.tx",
					routingKey,
					false, // mandatory
					false, // immediate
					amqp.Publishing{
						ContentType: "application/json",
						Body:        body,
						Timestamp:   time.Now(),
					},
				)
				if err != nil {
					fmt.Printf("[Watcher-%s] Publish error: %v\n", chain, err)
					continue
				}
				fmt.Printf("[Watcher-%s] Published tx=%s block=%d type=%s\n",
					chain, ev.TxHash, ev.BlockNumber, ev.EventType)
			}
			blockNumber++
		} else {
			// real mode: 预留真实链监听逻辑
			fmt.Printf("[Watcher-%s] Real mode not implemented yet\n", chain)
		}
	}
}

// generateMockEvents 生成模拟链上事件
func generateMockEvents(chain string, blockNum uint64) []ChainEvent {
	count := rand.Intn(3) + 1 // 每个块 1~3 条事件
	events := make([]ChainEvent, count)

	eventTypes := []string{"Transfer", "Mint", "Burn", "Approval"}
	for i := 0; i < count; i++ {
		evType := eventTypes[rand.Intn(len(eventTypes))]
		amount := fmt.Sprintf("%d", rand.Int63n(1000000000000000))
		if chain == "sol" {
			amount = fmt.Sprintf("%d", rand.Int63n(1000000000))
		}

		events[i] = ChainEvent{
			Chain:        chain,
			BlockNumber:  blockNum,
			TxHash:       fmt.Sprintf("0x%s%d", randomHex(16), rand.Int63()),
			LogIndex:     i,
			EventType:    evType,
			ContractAddr: fmt.Sprintf("0x%s", randomHex(20)),
			FromAddr:     fmt.Sprintf("0x%s", randomHex(20)),
			ToAddr:       fmt.Sprintf("0x%s", randomHex(20)),
			Amount:       amount,
			Timestamp:    time.Now().Unix(),
		}
	}
	return events
}

func randomHex(n int) string {
	letters := []rune("abcdef0123456789")
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func connectRabbitMQ(amqpURL string) (*amqp.Connection, *amqp.Channel) {
	var conn *amqp.Connection
	var err error

	// 带指数退避的重试连接
	for i := 0; i < 10; i++ {
		conn, err = amqp.Dial(amqpURL)
		if err == nil {
			break
		}
		fmt.Printf("[RabbitMQ] Connect attempt %d failed: %v, retrying...\n", i+1, err)
		time.Sleep(time.Duration(i+1) * time.Second)
	}
	if err != nil {
		panic(fmt.Sprintf("Failed to connect to RabbitMQ after retries: %v", err))
	}

	ch, err := conn.Channel()
	if err != nil {
		panic(fmt.Sprintf("Failed to open channel: %v", err))
	}

	fmt.Println("[RabbitMQ] Connected successfully")
	return conn, ch
}

func getEnv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}
