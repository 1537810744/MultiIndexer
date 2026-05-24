package main

// import 块：引入程序所需的外部代码库（标准库 + 第三方库）
import (
	// encoding/json：Go 标准库，负责将内存中的结构体（struct）转换成 JSON 字符串（序列化），
	// 或将 JSON 字符串还原成结构体（反序列化）。这里用于把链上事件打包成 JSON 发给 RabbitMQ。
	"encoding/json"
	// fmt：标准输入输出格式化库，类似 C 语言的 printf/scanf，这里用于打印日志。
	"fmt"
	// math/rand：伪随机数生成器。注意是"伪"随机，种子相同则序列相同。
	// 用于生成模拟的区块高度、交易哈希、金额等测试数据。
	"math/rand"
	// os：操作系统接口库，用于读取环境变量、操作文件描述符、进程控制等。
	"os"
	// time：时间处理库，提供定时器（Ticker）、时间戳、睡眠（Sleep）等功能。
	"time"

	// amqp：RabbitMQ 官方提供的 Go 客户端库（包路径 github.com/rabbitmq/amqp091-go）。
	// 它实现了 AMQP 0-9-1 协议，让 Go 程序可以连接 RabbitMQ 进行消息发布（Publish）和消费（Consume）。
	// 给包起别名 amqp，后面代码里就可以用 amqp.XXX 来调用，不用写全路径。
	amqp "github.com/rabbitmq/amqp091-go"
)

// ───────────────────────────────────────────────────────────────
// 数据结构区
// ───────────────────────────────────────────────────────────────

// ChainEvent 定义了一条"链上事件"的数据形状（Schema）。
// 在 Go 中，struct（结构体）是把多个不同类型的字段打包在一起的复合数据类型，
// 类似于其他语言里的 class / object / record。
//
// 每个字段后面的反引号 `` 里的内容叫"结构体标签"（Struct Tag）。
// `json:"chain"` 是 encoding/json 库专用的标签，告诉它：
//   把这个字段序列化成 JSON 时，对应的 key 名字叫做 "chain"。
//   反序列化时也会按这个 key 来匹配。
//
// 为什么要用 uint64、int 这些具体类型？
//   Go 是静态类型语言，每个变量在编译期就必须确定类型，这能提前发现很多错误。
//   uint64 = 无符号 64 位整数，范围 0 ~ 2^64-1，适合存区块高度（不会为负）。
//   int    = 有符号整数，平台相关（64位系统下是 64 位），适合存日志索引。
//   string = 字符串，Go 的字符串是不可变的字节序列，默认 UTF-8 编码。
//   int64  = 有符号 64 位整数，适合存 Unix 时间戳（1970年以来的秒数或毫秒数）。
type ChainEvent struct {
	Chain        string `json:"chain"`         // 链标识：eth / bsc / sol
	BlockNumber  uint64 `json:"block_number"`  // 区块编号，区块链的基本单位
	TxHash       string `json:"tx_hash"`       // 交易哈希，交易的唯一指纹
	LogIndex     int    `json:"log_index"`     // 交易内的事件日志索引
	EventType    string `json:"event_type"`    // 事件类型：Transfer / Mint / Burn / Approval
	ContractAddr string `json:"contract_address"` // 智能合约地址
	FromAddr     string `json:"from_address"`  // 发送方地址
	ToAddr       string `json:"to_address"`    // 接收方地址
	Amount       string `json:"amount"`        // 转账金额（用字符串避免大数精度丢失）
	Timestamp    int64  `json:"timestamp"`     // 事件产生的时间戳（Unix 秒）
}

// ───────────────────────────────────────────────────────────────
// 主函数入口
// ───────────────────────────────────────────────────────────────

// main() 是 Go 程序的唯一入口点，程序启动后操作系统第一个调用的函数。
// Go 程序必须有一个 main package 里的 main 函数才能编译成可执行文件。
func main() {
	// getEnv 是我们自己写的辅助函数（见文件底部），用来读取"环境变量"。
	//
	// 【名词解释：环境变量（Environment Variable）】
	// 环境变量是操作系统层面存储的键值对（key=value），在同一台机器上运行的不同程序都可以读取。
	// 常见用途：配置数据库地址、密码、运行模式等，避免把敏感信息硬编码进源代码。
	// 在 Docker / K8s 中，环境变量是最主流的传参方式。
	//
	// 第一个参数 "RABBITMQ_URL" 是要读取的变量名；
	// 第二个参数 "amqp://guest:guest@localhost:5672/" 是"默认值"，
	//   如果操作系统里没有设置这个变量，就返回默认值。
	//
	// amqp:// 是 RabbitMQ 的 URL 协议格式：
	//   amqp://用户名:密码@主机名:端口/虚拟主机
	amqpURL := getEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")

	// MODE 也是环境变量，控制程序的运行模式。
	// "mock" = 模拟模式，不连真实区块链，自己生成假数据，用于开发测试。
	// 未来可以扩展成 "real" 模式，连接真实的 ETH/BSC/SOL 节点。
	mode := getEnv("MODE", "mock")

	// connectRabbitMQ 是我们封装的函数，负责建立与 RabbitMQ 的连接。
	// 返回两个值：conn（连接对象）和 ch（通道对象）。
	// Go 函数支持返回多个值，这是 Go 的语法特色之一。
	conn, ch := connectRabbitMQ(amqpURL)

	// defer 是 Go 的关键字，意思是"延迟执行"。
	// 这行代码不会立即执行，而是注册到当前函数的退出清单里，
	// 等 main() 快要返回（结束）时，再按"后进先出"的顺序执行。
	// 作用：确保不管程序正常结束还是异常 panic，连接都能被关闭，防止资源泄漏。
	defer conn.Close()
	defer ch.Close()

	// ── 声明 RabbitMQ Exchange（交换机）──
	//
	// 【名词解释：Exchange（交换机）】
	// RabbitMQ 的消息不是直接发到队列的，而是先发到一个叫 Exchange 的中介。
	// Exchange 负责根据"路由规则"把消息分发给一个或多个队列。
	// Exchange 有四种类型：direct（精确匹配）、topic（模式匹配）、fanout（广播）、headers（头匹配）。
	// 这里用 "topic"，因为它支持用通配符（* 和 #）按 routing key 灵活路由，非常适合按链名分发的场景。
	//
	// 【名词解释：Durable（持久化）】
	// durable=true 表示 RabbitMQ 重启后这个 Exchange 还在。
	// 如果设为 false，RabbitMQ 一重启就消失，生产环境通常设 true。
	//
	// 其他参数解释：
	//   auto-deleted=false：当最后一个绑定的队列解绑后，不自动删除 Exchange。
	//   internal=false：允许外部客户端直接发消息到这个 Exchange（true 则只允许 Exchange 之间转发）。
	//   no-wait=false：阻塞等待 RabbitMQ 服务器的确认响应，确保声明成功。
	if err := ch.ExchangeDeclare(
		"indexer.tx", // name：Exchange 的名字，我们自己起的，和 fake-indexer 保持一致才能通
		"topic",      // type：topic 类型，支持 routing key 模式匹配
		true,         // durable：持久化
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments：额外参数，这里不需要
	); err != nil {
		// panic 是 Go 的内置函数，会立即终止当前 goroutine 的执行，
		// 向上抛出异常，如果被 recover 捕获可以恢复，否则程序崩溃退出。
		panic(fmt.Sprintf("Failed to declare exchange: %v", err))
	}

	// 打印启动信息到控制台（标准输出 stdout）。
	// %s 是 fmt 包的格式化占位符，表示"插入一个字符串"。
	fmt.Printf("[Listener] Started in %s mode\n", mode)
	fmt.Println("[Listener] Watching chains: ETH, BSC, SOL")

	// ── 定义三条链的配置参数 ──
	//
	// 这里用一个"匿名结构体切片"来存放三条链的配置。
	// []struct{...} 表示这是一个数组（准确说是切片 slice），每个元素是一个结构体。
	//
	// 【名词解释：Slice（切片）】
	// Go 的切片是对数组的动态封装，可以自动扩容，比固定长度的数组更常用。
	// 它由三部分组成：指向底层数组的指针、长度（len）、容量（cap）。
	//
	// 每条链的配置包含：
	//   name：链的简称，也是 RabbitMQ 里的 routing key 前缀。
	//   interval：模拟模式下产生新区块的间隔时间（ETH 12秒、BSC 3秒、SOL 400毫秒）。
	//   startBlk：模拟的起始区块号，用真实主网的大致高度，让日志看起来更真实。
	chains := []struct {
		name     string        // 链名
		interval time.Duration // 时间间隔类型，Go 里专门用来表示一段时间
		startBlk uint64        // 起始区块号
	}{
		{"eth", 12 * time.Second, 18000000},   // 以太坊主网大约 2023 年的区块高度
		{"bsc", 3 * time.Second, 30000000},    // BSC 出块更快
		{"sol", 400 * time.Millisecond, 250000000}, // Solana 出块约 400ms
	}

	// ── 为每条链启动一个独立的 Watcher 协程 ──
	//
	// 【核心概念：Goroutine（协程）】
	// Goroutine 是 Go 语言的轻量级线程，由 Go 运行时（runtime）自己调度，不是操作系统线程。
	// 开启一个 goroutine 只需要在函数调用前加关键字 go：go 函数名()
	// 它的栈初始只有约 2KB，可以成千上万个同时运行，成本极低。
	//
	// 为什么要每条链一个 goroutine？
	//   三条链的出块节奏完全不同（12秒 vs 3秒 vs 400毫秒），
	//   如果在同一个循环里顺序处理，快的链会被慢的链拖累。
	//   各自独立运行，互不阻塞，这是并发（Concurrency）的经典用法。
	for _, c := range chains {
		go runWatcher(ch, c.name, c.interval, c.startBlk, mode)
	}

	// select {} 是一个空的 select 语句，会永久阻塞当前 goroutine（这里是 main）。
	// 因为 main 一旦返回，整个进程就会退出，而我们需要让后台的 watcher goroutine 一直运行。
	// 这种写法是 Go 里保持主线程不退出的一种简洁惯用法。
	select {}
}

// ───────────────────────────────────────────────────────────────
// 核心逻辑：每条链的 Watcher（观察者）
// ───────────────────────────────────────────────────────────────

// runWatcher 模拟一条区块链的"监听节点"。
// 参数：
//   ch        —— RabbitMQ 通道，用于发消息
//   chain     —— 链名（eth / bsc / sol）
//   interval  —— 模拟出块间隔
//   startBlock—— 起始区块号
//   mode      —— 运行模式（目前只实现了 mock）
func runWatcher(ch *amqp.Channel, chain string, interval time.Duration, startBlock uint64, mode string) {
	// ── Panic 恢复机制（Defer + Recover）──
	//
	// defer 注册了一个匿名函数，在 runWatcher 退出时执行。
	// recover() 是 Go 的内置函数，用于捕获当前 goroutine 里的 panic。
	// 如果 recover() 返回非 nil，说明发生了 panic，我们可以选择恢复而不是让整个进程崩溃。
	//
	// 为什么要这样做？
	//   三条链跑在同一个进程里，如果某个链的代码出 bug 导致 panic，
	//   我们不希望把另外两条链也拖垮。recover 起到了"故障隔离"的作用。
	//   恢复后等待 2 秒，再重新启动一个 runWatcher goroutine，实现自愈。
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("[Watcher-%s] Panic recovered: %v, restarting...\n", chain, r)
			time.Sleep(2 * time.Second)
			go runWatcher(ch, chain, interval, startBlock, mode)
		}
	}()

	fmt.Printf("[Watcher-%s] Started, interval: %v, mode: %s\n", chain, interval, mode)

	// blockNumber 是当前模拟的区块高度，每次循环自增 1。
	blockNumber := startBlock

	// time.NewTicker 创建一个"滴答器"，每隔 interval 时间就往通道里发送一个当前时间。
	// 它非常适合做"定时轮询"或"定时触发"。
	// 注意：用完必须调用 Stop() 释放资源，否则会造成 goroutine 泄漏。
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// for range ticker.C 是 Go 中"定时消费通道"的惯用写法。
	// ticker.C 是一个只读通道（chan time.Time），每次滴答就产生一个值，
	// for range 会阻塞等待，直到有值进来，然后执行循环体。
	for range ticker.C {
		// 目前只实现了 mock（模拟）模式，real 模式留给未来扩展。
		if mode == "mock" {
			// generateMockEvents 生成一批随机的链上事件（1~3条）。
			events := generateMockEvents(chain, blockNumber)

			// 遍历这批事件，逐条发送到 RabbitMQ。
			for _, ev := range events {
				// json.Marshal 把 ChainEvent 结构体序列化成 []byte（字节切片）。
				// JSON 是跨语言的数据交换格式，RabbitMQ 消息体通常用 JSON 或二进制。
				body, err := json.Marshal(ev)
				if err != nil {
					fmt.Printf("[Watcher-%s] JSON marshal error: %v\n", chain, err)
					continue // 跳过这条错误数据，继续处理下一条
				}

				// routingKey 是 RabbitMQ topic Exchange 的路由键。
				// 格式："eth.tx"、"bsc.tx"、"sol.tx"
				// 在 topic Exchange 中，消费者可以用 "*.tx" 或 "eth.*" 等模式来匹配订阅。
				routingKey := chain + ".tx"

				// ch.Publish 向 Exchange 发布一条消息。
				// 参数解释：
				//   exchange   —— 要发送到的交换机名称
				//   routingKey —— 路由键，Exchange 根据它决定消息去往哪些队列
				//   mandatory  —— false，如果消息无法路由到任何队列，直接丢弃（true 则返回错误）
				//   immediate  —— false，已被 RabbitMQ 废弃，保持 false
				//   Publishing —— 消息体本身，包含内容类型、正文、时间戳等元数据
				err = ch.Publish(
					"indexer.tx",
					routingKey,
					false, // mandatory
					false, // immediate
					amqp.Publishing{
						ContentType: "application/json", // 告诉消费者：正文是 JSON 格式
						Body:        body,               // 消息正文（字节切片）
						Timestamp:   time.Now(),         // 消息发送时间（辅助调试）
					},
				)
				if err != nil {
					fmt.Printf("[Watcher-%s] Publish error: %v\n", chain, err)
					continue
				}

				// 打印日志，确认消息已发出。
				fmt.Printf("[Watcher-%s] Published tx=%s block=%d type=%s\n",
					chain, ev.TxHash, ev.BlockNumber, ev.EventType)
			}
			// 模拟完成一个区块的处理，区块号 +1。
			blockNumber++
		} else {
			fmt.Printf("[Watcher-%s] Real mode not implemented yet\n", chain)
		}
	}
}

// ───────────────────────────────────────────────────────────────
// 数据生成：模拟链上事件
// ───────────────────────────────────────────────────────────────

// generateMockEvents 是一个"测试数据工厂"。
// 在 mock 模式下，我们不连接真实区块链节点，而是自己伪造看起来像真实链上的交易数据。
// 这对早期开发非常有用：不需要 API Key、不需要等真实出块、可以控制数据量。
func generateMockEvents(chain string, blockNum uint64) []ChainEvent {
	// rand.Intn(3) 返回 0~2 的随机整数，+1 后变成 1~3。
	// 表示每个区块里随机产生 1 到 3 条事件，模拟真实区块链的并发交易。
	count := rand.Intn(3) + 1

	// make 是 Go 的内置函数，用于创建切片、map 或 channel，并分配内存、初始化长度。
	// make([]ChainEvent, count) 创建一个长度为 count 的 ChainEvent 切片，初始值都是零值。
	events := make([]ChainEvent, count)

	// 定义四种常见的事件类型，模拟 ERC-20 / NFT 合约里最常见的操作。
	eventTypes := []string{"Transfer", "Mint", "Burn", "Approval"}

	for i := 0; i < count; i++ {
		// 随机挑选一种事件类型。
		evType := eventTypes[rand.Intn(len(eventTypes))]

		// rand.Int63n(n) 返回一个 int64 类型的随机数，范围 [0, n)。
		// 这里模拟转账金额。用字符串存储是因为区块链里的金额可能非常大
		//（比如 ETH 的精度是 18 位小数，远超普通编程语言的 float64 精度范围，会失真）。
		amount := fmt.Sprintf("%d", rand.Int63n(1000000000000000))

		// Solana 的代币精度通常是 9 位（ETH 是 18 位），所以金额范围调小一些，更真实。
		if chain == "sol" {
			amount = fmt.Sprintf("%d", rand.Int63n(1000000000))
		}

		// 填充一条事件的所有字段。
		events[i] = ChainEvent{
			Chain:        chain,                                 // 链标识
			BlockNumber:  blockNum,                              // 当前区块号
			TxHash:       fmt.Sprintf("0x%s%d", randomHex(16), rand.Int63()), // 模拟 0x 开头的交易哈希
			LogIndex:     i,                                     // 事件在交易中的索引
			EventType:    evType,                                // 事件类型
			ContractAddr: fmt.Sprintf("0x%s", randomHex(20)),    // 模拟 20 字节的合约地址（40 个十六进制字符）
			FromAddr:     fmt.Sprintf("0x%s", randomHex(20)),    // 发送方
			ToAddr:       fmt.Sprintf("0x%s", randomHex(20)),    // 接收方
			Amount:       amount,                                // 金额（字符串）
			Timestamp:    time.Now().Unix(),                     // 当前 Unix 时间戳（秒）
		}
	}
	return events
}

// randomHex 生成指定长度的随机十六进制字符串。
// n 表示要生成多少个字符（不是字节）。
// 例如 randomHex(16) 生成 16 个十六进制字符，如 "a3f7b2d8e901c4ab"。
func randomHex(n int) string {
	// 定义可选字符集：0-9 和 a-f，共 16 个。
	letters := []rune("abcdef0123456789")

	// make([]rune, n) 创建一个 rune 切片，长度为 n。
	// rune 是 Go 中表示 Unicode 码点的类型，类似其他语言的 char，但占 4 字节，
	// 可以安全处理中文、emoji 等，这里用来存单个字符也完全没问题。
	b := make([]rune, n)

	// for range b 会遍历切片的索引，i 是下标（0 到 n-1）。
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

// ───────────────────────────────────────────────────────────────
// 工具函数：连接 RabbitMQ
// ───────────────────────────────────────────────────────────────

// connectRabbitMQ 封装了带"指数退避重试"的 RabbitMQ 连接逻辑。
//
// 【设计思路：为什么要重试？】
// 在分布式系统里，网络抖动、服务启动顺序不一致（比如 listener 比 RabbitMQ 先启动）是常态。
// 如果第一次连不上就直接崩溃，系统会很不稳定。
// 因此我们采用"指数退避"策略：第 1 次等 1 秒，第 2 次等 2 秒……最多重试 10 次。
// 这样既能容忍短暂故障，又不会无限重试耗尽资源。
func connectRabbitMQ(amqpURL string) (*amqp.Connection, *amqp.Channel) {
	var conn *amqp.Connection // 连接对象，维护 TCP 长连接 + AMQP 握手状态
	var err error             // Go 的错误处理是显式的，函数通常返回 (result, error) 元组

	// for i := 0; i < 10; i++ 是经典的 for 循环语法：初始化; 条件; 后置操作。
	for i := 0; i < 10; i++ {
		// amqp.Dial 建立到 RabbitMQ 服务器的 TCP + AMQP 协议连接。
		conn, err = amqp.Dial(amqpURL)
		if err == nil {
			break // 连接成功，跳出循环
		}
		fmt.Printf("[RabbitMQ] Connect attempt %d failed: %v, retrying...\n", i+1, err)
		// time.Duration(i+1) * time.Second 把整数转成时间间隔，实现退避等待。
		time.Sleep(time.Duration(i+1) * time.Second)
	}
	// 如果 10 次都失败了，err 不为 nil，程序无法继续，直接 panic。
	if err != nil {
		panic(fmt.Sprintf("Failed to connect to RabbitMQ after retries: %v", err))
	}

	// conn.Channel() 在已有 TCP 连接上开辟一个 AMQP 通道（Channel）。
	//
	// 【名词解释：Connection vs Channel】
	// Connection 是底层 TCP 连接，建立和销毁成本较高。
	// Channel 是逻辑上的"轻量级连接"，多个 Channel 可以复用同一个 TCP 连接，
	// 但各自独立收发消息，互不影响。推荐做法是一个线程/协程用一个 Channel。
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

// getEnv 是一个极常见的工具函数，几乎每个 Go 项目都会写类似的东西。
//
// 【名词解释：os.Getenv】
// os.Getenv 是 Go 标准库函数，读取操作系统环境变量的值，返回 string。
// 如果环境变量不存在，返回空字符串 ""（不是 nil，因为 string 在 Go 里不能为 nil）。
//
// 【函数签名解析】
// func getEnv(key, defaultVal string) string
//   key        —— 要读取的环境变量名（如 "RABBITMQ_URL"）
//   defaultVal —— 默认值，如果该变量未设置则返回它
//   string     —— 返回值类型，最终得到的字符串
//
// 【语法：if 的简写形式】
// if v := os.Getenv(key); v != "" { ... }
// 这是 Go 特有的语法：在 if 条件里先执行赋值，然后用分号隔开写判断条件。
// v 的作用域仅限于 if 块内部，出了块就访问不到，避免变量污染外部作用域。
func getEnv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}
