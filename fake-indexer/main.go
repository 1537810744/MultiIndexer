package main

import (
	"fmt"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	amqpURL := getEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")

	conn, ch := connectRabbitMQ(amqpURL)
	defer conn.Close()
	defer ch.Close()

	// 声明 exchange（与 listener 保持一致）
	if err := ch.ExchangeDeclare(
		"indexer.tx",
		"topic",
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,
	); err != nil {
		panic(fmt.Sprintf("Failed to declare exchange: %v", err))
	}

	// 声明一个持久化队列
	q, err := ch.QueueDeclare(
		"fake-indexer-queue",
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,
	)
	if err != nil {
		panic(fmt.Sprintf("Failed to declare queue: %v", err))
	}

	// 绑定到三个链的 routing key
	bindings := []string{"eth.tx", "bsc.tx", "sol.tx"}
	for _, key := range bindings {
		if err := ch.QueueBind(q.Name, key, "indexer.tx", false, nil); err != nil {
			panic(fmt.Sprintf("Failed to bind queue %s: %v", key, err))
		}
		fmt.Printf("[FakeIndexer] Bound queue to routing key: %s\n", key)
	}

	// 开始消费
	msgs, err := ch.Consume(
		q.Name,
		"",    // consumer tag
		true,  // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,
	)
	if err != nil {
		panic(fmt.Sprintf("Failed to start consumer: %v", err))
	}

	fmt.Println("[FakeIndexer] ==========================================")
	fmt.Println("[FakeIndexer] Started, waiting for messages...")
	fmt.Println("[FakeIndexer] ==========================================")

	msgCount := 0
	for msg := range msgs {
		msgCount++
		fmt.Printf("\n[FakeIndexer] --- Message #%d ---\n", msgCount)
		fmt.Printf("[FakeIndexer] RoutingKey: %s\n", msg.RoutingKey)
		fmt.Printf("[FakeIndexer] Timestamp:  %s\n", msg.Timestamp.Format(time.RFC3339))
		fmt.Printf("[FakeIndexer] Body:\n%s\n", string(msg.Body))
		fmt.Println("[FakeIndexer] ------------------------")
	}
}

func connectRabbitMQ(amqpURL string) (*amqp.Connection, *amqp.Channel) {
	var conn *amqp.Connection
	var err error

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
