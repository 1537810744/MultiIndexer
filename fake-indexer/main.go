package main

// import 块：引入本程序需要的外部代码库
import (
	// fmt：标准格式化 I/O 库。提供 Printf（格式化打印）、Println（换行打印）等函数，
	// 是 Go 程序里输出日志、调试信息最常用的工具。
	"fmt"
	// os：操作系统接口。这里只用来读取环境变量，和 listener 里的 getEnv 配合使用。
	"os"
	// time：时间处理库。这里用于把时间戳（time.Time 类型）格式化成人类可读的字符串。
	"time"

	// amqp：RabbitMQ 官方 Go 客户端库（包名 github.com/rabbitmq/amqp091-go）。
	// 给包起别名 amqp，方便后续代码书写。
	// 本程序作为"消费者"，需要用到连接、声明队列、绑定路由键、消费消息等功能。
	amqp "github.com/rabbitmq/amqp091-go"
)

// ───────────────────────────────────────────────────────────────
// 主函数入口
// ───────────────────────────────────────────────────────────────

// main() 是 Go 可执行程序的唯一入口。
// 当容器启动时，Dockerfile 里的 CMD ["./fake-indexer"] 会执行这个函数。
func main() {
	// 读取环境变量 RABBITMQ_URL，默认指向本机默认安装的 RabbitMQ。
	// 在 docker-compose 里，这个变量会被覆盖成 "amqp://admin:admin@rabbitmq:5672/"
	// 因为 fake-indexer 和 RabbitMQ 在同一个 Docker 网络里，可以用服务名 "rabbitmq" 直接访问。
	amqpURL := getEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")

	// 调用封装的连接函数，获取 TCP 连接（conn）和 AMQP 逻辑通道（ch）。
	conn, ch := connectRabbitMQ(amqpURL)

	// defer 延迟关闭资源，确保程序退出时（即使是 panic），连接能被优雅释放，防止 TCP 句柄泄漏。
	// 注意 defer 是"后进先出"，所以 ch.Close() 先执行，conn.Close() 后执行。
	defer conn.Close()
	defer ch.Close()

	// ── 声明 Exchange（交换机）──
	//
	// 为什么消费者也要声明 Exchange？
	//   在 RabbitMQ 中，如果消费者先启动、生产者还没发消息，Exchange 可能还不存在。
	//   消费者自己声明一次，可以确保 Exchange 已就绪，避免绑定队列时报错。
	//   多次声明同一个 Exchange（参数相同）是幂等的，不会报错。
	//
	// 这里的参数和 listener/main.go 里完全一致（name="indexer.tx"，type="topic"，durable=true），
	// 这样才能匹配上生产者发过来的消息。
	if err := ch.ExchangeDeclare(
		"indexer.tx", // name：交换机名称，必须和 listener 端保持一致
		"topic",      // type：topic 类型，支持 routing key 的通配符匹配
		true,         // durable：持久化，RabbitMQ 重启后不丢失
		false,        // auto-deleted：没有队列绑定时也不自动删除
		false,        // internal：允许外部直接发消息进来
		false,        // no-wait：阻塞等待服务器确认
		nil,          // arguments：无额外参数
	); err != nil {
		panic(fmt.Sprintf("Failed to declare exchange: %v", err))
	}

	// ── 声明队列（Queue）──
	//
	// 【名词解释：Queue（队列）】
	// 队列是 RabbitMQ 存储消息的缓冲区。生产者把消息发到 Exchange，Exchange 按规则转发到队列，
	// 消费者再从队列里一条条取出消息处理。
	// 队列有一个名字，消费者靠名字来订阅。
	//
	// 为什么叫 "fake-indexer-queue"？
	//   这个名字是我们随便起的，只要在这个 RabbitMQ 实例里唯一就行。
	//   真实项目里可能按业务命名，比如 "token-processor-queue"。
	q, err := ch.QueueDeclare(
		"fake-indexer-queue", // name：队列名称
		true,                 // durable：队列持久化，RabbitMQ 重启后还在
		false,                // delete when unused：没有消费者连接时也不自动删除
		false,                // exclusive：不排他，允许多个消费者连接（这里虽然是单消费者，但习惯设 false）
		false,                // no-wait：阻塞等待服务器确认
		nil,                  // arguments：无额外参数（如死信队列、TTL 等高级特性在这里不涉及）
	)
	if err != nil {
		panic(fmt.Sprintf("Failed to declare queue: %v", err))
	}

	// ── 绑定队列到 Exchange（Binding）──
	//
	// 【名词解释：Binding（绑定）】
	// 绑定是队列和交换机之间的"订阅关系"，它告诉交换机：
	// "凡是 routing key 符合某某规则的消息，都请转发到我这个队列里。"
	//
	// Topic Exchange 的 routing key 通常用点号分隔的单词，比如 "eth.tx"、"bsc.tx"。
	// 消费者端可以用通配符来匹配：
	//   "*.tx"  —— 匹配任意一个单词后缀是 .tx 的 key（eth.tx、bsc.tx、sol.tx 都匹配）
	//   "eth.*" —— 只匹配 eth 前缀的消息
	//   "#"     —— 匹配所有消息（相当于 fanout）
	//
	// 这里我们明确绑定三个 routing key，语义清晰：只收这三条链的交易消息。
	bindings := []string{"eth.tx", "bsc.tx", "sol.tx"}
	for _, key := range bindings {
		// QueueBind 执行绑定操作。
		// 参数：队列名、routingKey、Exchange 名、noWait、额外参数。
		if err := ch.QueueBind(q.Name, key, "indexer.tx", false, nil); err != nil {
			panic(fmt.Sprintf("Failed to bind queue %s: %v", key, err))
		}
		fmt.Printf("[FakeIndexer] Bound queue to routing key: %s\n", key)
	}

	// ── 开始消费消息（Consume）──
	//
	// 【名词解释：Consumer（消费者）】
	// 消费者是消息队列架构里的接收端，负责从队列里拉取（或被动推送）消息并处理。
	// RabbitMQ 采用"推模式"：一旦有消息进入队列，服务器会主动推给消费者。
	//
	// ch.Consume 返回一个 Go 通道（chan amqp.Delivery），消息会源源不断地从这个通道里冒出来。
	msgs, err := ch.Consume(
		q.Name, // queue：要消费的队列名称
		"",     // consumer tag：消费者的标签名，设为空字符串让 RabbitMQ 自动生成。
		        //         标签用于区分同一个队列的多个消费者，管理（如取消）时用到。
		true,   // auto-ack：自动确认（acknowledgement）。
		        //         设为 true 表示消息一交给消费者，RabbitMQ 就认为"已处理"，立即从队列删除。
		        //         生产环境通常设为 false，手动 ack，防止消费者处理到一半崩溃导致消息丢失。
		false,  // exclusive：不独占队列（允许多个消费者同时订阅）。
		false,  // no-local：消费者不接收自己通过同一个连接发布的消息（对本场景无意义，设 false）。
		false,  // no-wait：阻塞等待服务器确认消费者注册成功。
		nil,    // arguments：无额外参数。
	)
	if err != nil {
		panic(fmt.Sprintf("Failed to start consumer: %v", err))
	}

	// 打印几条醒目的分割线，方便在日志里一眼找到程序启动的位置。
	fmt.Println("[FakeIndexer] ==========================================")
	fmt.Println("[FakeIndexer] Started, waiting for messages...")
	fmt.Println("[FakeIndexer] ==========================================")

	// msgCount 用来统计收到的消息总数，单纯用于日志展示，无业务逻辑作用。
	msgCount := 0

	// for msg := range msgs 是 Go 中"从通道里持续消费"的标准写法。
	// msgs 是一个 chan amqp.Delivery 类型的通道，当 RabbitMQ 有新消息时，
	// 会往这个通道里塞一个 amqp.Delivery 对象，for range 会立刻拿到它并执行循环体。
	// 如果通道被关闭，for range 会自动退出。
	for msg := range msgs {
		msgCount++

		// \n 是换行符，让每条消息的日志之间有空白，提升可读性。
		fmt.Printf("\n[FakeIndexer] --- Message #%d ---\n", msgCount)

		// msg.RoutingKey 是生产者发布消息时设置的 routing key（如 "eth.tx"）。
		// 消费者可以通过它判断消息来自哪条链，而不用解析 JSON 正文。
		fmt.Printf("[FakeIndexer] RoutingKey: %s\n", msg.RoutingKey)

		// msg.Timestamp 是生产者设置的发送时间（amqp.Publishing 里的 Timestamp 字段）。
		// 它类型是 time.Time，调用 Format 方法按 RFC3339 标准格式化成字符串，如 "2024-01-15T10:30:00+08:00"。
		fmt.Printf("[FakeIndexer] Timestamp:  %s\n", msg.Timestamp.Format(time.RFC3339))

		// msg.Body 是消息的正文，类型是 []byte（字节切片）。
		// 生产者发的是 JSON，所以这里用 string(msg.Body) 把字节切片转成字符串打印出来，
		// 就能直接看到完整的 JSON 内容。
		fmt.Printf("[FakeIndexer] Body:\n%s\n", string(msg.Body))

		fmt.Println("[FakeIndexer] ------------------------")
	}
}

// ───────────────────────────────────────────────────────────────
// 工具函数：连接 RabbitMQ（带指数退避重试）
// ───────────────────────────────────────────────────────────────

// connectRabbitMQ 和 listener 里的版本几乎一模一样，是微服务里常见的"复制但独立"模式。
// 因为 fake-indexer 是一个独立进程，它必须自己建立 TCP + AMQP 连接，不能复用 listener 的连接。
//
// 【设计思路】
// 在容器编排（docker-compose / K8s）中，服务启动顺序是不确定的。
// fake-indexer 有可能比 RabbitMQ 先启动，直接连接会失败。
// 通过重试机制，fake-indexer 会 patiently 等待 RabbitMQ 就绪，而不是一启动就崩溃。
func connectRabbitMQ(amqpURL string) (*amqp.Connection, *amqp.Channel) {
	var conn *amqp.Connection
	var err error

	// 最多重试 10 次，每次等待时间递增（1秒、2秒、3秒……）。
	for i := 0; i < 10; i++ {
		conn, err = amqp.Dial(amqpURL)
		if err == nil {
			break // 连接成功，立刻退出重试循环
		}
		fmt.Printf("[RabbitMQ] Connect attempt %d failed: %v, retrying...\n", i+1, err)
		time.Sleep(time.Duration(i+1) * time.Second)
	}
	if err != nil {
		panic(fmt.Sprintf("Failed to connect to RabbitMQ after retries: %v", err))
	}

	// 在 TCP 连接之上开辟 AMQP 通道（Channel）。
	// 一个连接可以开多个通道，各自独立收发，互不干扰。
	ch, err := conn.Channel()
	if err != nil {
		panic(fmt.Sprintf("Failed to open channel: %v", err))
	}

	fmt.Println("[RabbitMQ] Connected successfully")
	return conn, ch
}

// ───────────────────────────────────────────────────────────────
// 工具函数：读取环境变量
// ───────────────────────────────────────────────────────────────

// getEnv 是一个小型工具函数，优先从操作系统环境变量中取值；
// 如果环境变量未设置（返回空字符串），则使用调用方提供的默认值。
//
// 【为什么需要默认值？】
// 开发阶段直接在本地 go run 时，环境变量往往没设置，有默认值就能立刻跑起来。
// 生产环境（Docker / K8s）通过编排文件注入环境变量，覆盖默认值。
// 这种模式让同一份代码在"本地开发"和"生产部署"之间无缝切换。
func getEnv(key, defaultVal string) string {
	// os.Getenv 读取环境变量，返回 string。若不存在则返回 ""（空字符串）。
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}
