// ============================================================================
// main.go — 监听器（Listener）的入口文件
// ============================================================================
// 监听器是整个 MultiIndexer 系统的"眼睛"，负责：
//   1. 连接 RabbitMQ 消息队列
//   2. 启动多个"链观察者"（每个链一个独立的 goroutine）
//   3. 保持主进程不退出
//
// 运行模式有两种：
//   - mock 模式：不连真实区块链，程序内部生成假数据（开发测试用）
//   - real 模式：连接真实区块链 RPC 节点，拉取真实链上交易（生产用）
// ============================================================================

package main

import (
	"fmt"  // 格式化输入输出，类似 C 的 printf/scanf，用于打印日志
	"os"   // 操作系统接口，用于读取环境变量（给程序传递配置参数）
	"time" // 时间处理，用于设置轮询间隔和重试等待

	amqp "github.com/rabbitmq/amqp091-go" // RabbitMQ Go 客户端，别名 amqp 方便使用
)

// ============================================================================
// main() — Go 程序的唯一入口
// ============================================================================
// Go 程序从 main package 的 main() 函数开始执行，操作系统调用它。
// main() 没有参数，没有返回值。退出意味着进程结束。
func main() {
	// ============================================================
	// 第一步：读取配置（环境变量）
	// ============================================================
	// getEnv 是我们自己写的工具函数（见文件底部），从操作系统环境变量中取值。
	// 如果环境变量不存在，就使用第二个参数作为默认值。
	// 这样同一份代码在开发环境和生产环境可以有不同的配置，不需要改代码。
	amqpURL := getEnv("RABBITMQ_URL", "amqp://admin:admin@localhost:5672/")
	mode := getEnv("MODE", "mock")                   // mock（模拟）或 real（真实链）
	ethRPC := getEnv("ETH_RPC_URL", "https://ethereum-rpc.publicnode.com")
	bscRPC := getEnv("BSC_RPC_URL", "https://bsc-rpc.publicnode.com")
	solRPC := getEnv("SOL_RPC_URL", "https://solana-rpc.publicnode.com")
	ethWallet := getEnv("ETH_WALLET", "") // 要特别监控的钱包地址（告警功能使用）
	solWallet := getEnv("SOL_WALLET", "") // 要特别监控的 Solana 钱包地址

	// ============================================================
	// 第二步：连接 RabbitMQ
	// ============================================================
	// connectRabbitMQ 是带"指数退避重试"的连接函数（见文件底部）。
	// 它返回两个值：
	//   conn — TCP 长连接（Connection），程序退出前需要关闭
	//   ch   — AMQP 逻辑通道（Channel），所有消息收发都通过它
	//
	// 【RabbitMQ 概念：Connection vs Channel】
	// Connection 是底层的 TCP 连接，一个客户端通常只有一个 Connection。
	// Channel 是在 Connection 之上的"虚拟连接"，轻量、可以开多个。
	// 推荐每个 goroutine 用自己独立的 Channel（本程序每条链的 Watcher 共用一个 Channel）。
	conn, ch := connectRabbitMQ(amqpURL)

	// defer 是 Go 的"延迟执行"关键字。
	// defer 后面的语句不会立刻执行，而是等到当前函数（main）返回时才执行。
	// 多个 defer 按"后进先出"的顺序执行（像叠盘子，后放的先拿）。
	// 作用：确保不管程序正常退出还是 panic，资源都会被释放。
	defer conn.Close() // 关闭 RabbitMQ TCP 连接
	defer ch.Close()   // 关闭 RabbitMQ Channel

	// ============================================================
	// 第三步：声明 RabbitMQ Exchange（交换机）
	// ============================================================
	// 【RabbitMQ 概念：Exchange】
	// Exchange 是消息的中转站。生产者不直接把消息发到队列，而是发到 Exchange，
	// Exchange 根据 routing key 把消息转发到匹配的队列。
	// 类型为 "topic" 意味着支持通配符路由（* 匹配一个词，# 匹配零个或多个词）。
	//
	// 参数说明：
	//   "indexer.tx" — Exchange 名称（所有链的事件共用一个 Exchange）
	//   "topic"      — Exchange 类型（topic 支持按 routing key 模式匹配）
	//   true         — durable（持久化），RabbitMQ 重启后 Exchange 还在
	//   false        — auto-delete（无队列绑定时自动删除）
	//   false        — internal（只允许 Exchange 之间转发，不允许外部直接发）
	//   false        — no-wait（不等待服务器确认，false 表示阻塞等待确认）
	//   nil          — 额外参数（不需要）
	//
	// Go 的 if 语句可以在条件前加一个简短的赋值语句：
	//   if err := ch.ExchangeDeclare(...); err != nil { ... }
	//   err 的作用域仅限于 if-else 块，出了块就访问不到。
	if err := ch.ExchangeDeclare(
		"indexer.tx", "topic", true, false, false, false, nil,
	); err != nil {
		// panic 是 Go 的"致命错误"函数，会立即终止程序并打印调用栈。
		// 只在不可恢复的错误时使用（比如 RabbitMQ 不可用，程序继续运行没意义）。
		panic(fmt.Sprintf("Failed to declare exchange: %v", err))
	}

	// ============================================================
	// 第四步：打印启动横幅
	// ============================================================
	fmt.Println("==========================================")
	fmt.Printf("[Listener] MultiIndexer Listener Service\n")
	fmt.Printf("[Listener] Mode: %s\n", mode)
	fmt.Println("==========================================")

	// ============================================================
	// 第五步：根据运行模式启动 Watcher（链观察者）
	// ============================================================
	if mode == "mock" {
		runMockMode(ch) // 模拟模式：程序内部生成假数据
	} else {
		runRealMode(ch, ethRPC, bscRPC, solRPC, ethWallet, solWallet) // 真实模式：连接区块链节点
	}
}

// ============================================================================
// runRealMode — 启动真实区块链观察者
// ============================================================================
func runRealMode(ch *amqp.Channel, ethRPC, bscRPC, solRPC, ethWallet, solWallet string) {
	fmt.Println("[Listener] Starting real blockchain watchers...")

	// ============================================================
	// 【Go 并发核心概念：goroutine（协程）】
	// goroutine 是 Go 的轻量级"线程"，由 Go 运行时（runtime）管理，不是操作系统线程。
	// 一个 goroutine 的初始栈只有 ~2KB（操作系统线程通常 1-8MB），
	// 所以可以轻松创建成千上万个 goroutine 同时运行。
	//
	// 启动 goroutine 非常简单：在函数调用前加 go 关键字。
	//   go ethWatcher.Run()
	// 这行代码会创建一个新的 goroutine，在里面执行 Run() 方法，
	// 主 goroutine（main）不会等待它，会继续执行下一行代码。
	// ============================================================

	// 1. 以太坊观察者（每 12 秒轮询一次，匹配以太坊约 12 秒的出块时间）
	//    NewEVMWatcher 创建观察者，Run() 方法启动主循环
	ethWatcher := NewEVMWatcher("eth", ethRPC, 12*time.Second, ch, ethWallet)
	go ethWatcher.Run() // go 关键字启动新的 goroutine，不阻塞主线程

	// 2. BSC 观察者（每 3 秒轮询一次，BSC 出块约 3 秒）
	bscWatcher := NewEVMWatcher("bsc", bscRPC, 3*time.Second, ch, ethWallet)
	go bscWatcher.Run()

	// 3. Solana 观察者（每 1 秒轮询一次，Solana 出块约 0.4 秒）
	solWatcher := NewSolWatcher(solRPC, 1*time.Second, ch, solWallet)
	go solWatcher.Run()

	fmt.Println("[Listener] All watchers started. Watching for on-chain events...")
	fmt.Println("[Listener] Chains: ETH (12s), BSC (3s), SOL (1s)")

	// ============================================================
	// 【Go 并发核心概念：select {} 永久阻塞】
	// select {} 是一个空的 select 语句，它会永久阻塞当前 goroutine。
	// 因为 main() 一旦 return，整个进程就会退出（所有 goroutine 都会被杀死）。
	// 所以必须让 main goroutine 阻塞住，让后台的 watcher goroutine 持续运行。
	//
	// 这个写法的效果类似于：
	//   for { time.Sleep(time.Hour) }  // 死循环 + 睡眠
	// 但 select {} 更高效，不消耗 CPU。
	// ============================================================
	select {}
}

// ============================================================================
// runMockMode — 启动模拟区块链观察者（Phase 1 遗留，用于无 RPC 环境测试）
// ============================================================================
func runMockMode(ch *amqp.Channel) {
	fmt.Println("[Listener] Running in MOCK mode — generating simulated events")
	fmt.Println("[Listener] Watching chains: ETH, BSC, SOL")
	fmt.Println("[Listener] Set MODE=real to connect to live blockchains")

	// runMockWatcher 在 mock.go 里定义，启动三个模拟观察者
	// eth: 12秒间隔，起始区块 18000000
	// bsc: 3秒间隔，起始区块 30000000
	// sol: 400毫秒间隔（模拟 Solana 快速出块），起始 Slot 250000000
	go runMockWatcher(ch, "eth", 12*time.Second, 18000000)
	go runMockWatcher(ch, "bsc", 3*time.Second, 30000000)
	go runMockWatcher(ch, "sol", 400*time.Millisecond, 250000000)

	select {} // 永久阻塞，保持主线程不退出
}

// ============================================================================
// 工具函数
// ============================================================================

// getEnv 读取操作系统环境变量的值，如果环境变量不存在则返回默认值。
//
// 【Go 语法：函数签名】
// func getEnv(key, defaultVal string) string
//   key        — 环境变量名（如 "RABBITMQ_URL"）
//   defaultVal — 默认值
//   返回 string — 最终得到的值
//
// 为什么用环境变量而不是写死在代码里？
// - 部署到不同环境（开发/测试/生产）时，不需要改代码，只改变量值
// - Docker/Kubernetes 原生支持注入环境变量
// - 敏感信息（密码、API Key）不应该写在代码里提交到 Git
func getEnv(key, defaultVal string) string {
	// os.Getenv(key) 是标准库函数，读取环境变量，返回 string
	// 如果环境变量未设置，返回空字符串 ""
	if v := os.Getenv(key); v != "" {
		return v // 环境变量有值，优先使用
	}
	return defaultVal // 环境变量为空，使用默认值
}

// connectRabbitMQ 连接到 RabbitMQ，带"指数退避重试"机制。
//
// 【设计思路：为什么需要重试？】
// 在分布式系统中，服务启动顺序不确定（Listener 可能比 RabbitMQ 先启动），
// 网络也可能短暂抖动。如果一次连接失败就崩溃，系统非常脆弱。
// "指数退避"策略：第1次等1秒，第2次等2秒，第3次等3秒……最多重试10次。
// 这样既能在短暂故障后恢复，又不会无限重试耗尽资源。
//
// 【Go 语法：多返回值】
// (*amqp.Connection, *amqp.Channel) 表示这个函数返回两个值。
// 调用时可以用 conn, ch := connectRabbitMQ(url) 接收两个值。
func connectRabbitMQ(amqpURL string) (*amqp.Connection, *amqp.Channel) {
	var conn *amqp.Connection // 声明变量（初始值为 nil）
	var err error

	// 【Go 语法：for 循环】
	// for i := 0; i < 10; i++ { ... }
	// 初始化; 条件; 后置操作
	// Go 只有 for 一种循环关键字，没有 while，但可以模拟：
	//   for condition { ... }  // 相当于 while
	//   for { ... }            // 相当于 while(true) 无限循环
	for i := 0; i < 10; i++ {
		// amqp.Dial 建立到 RabbitMQ 的 TCP + AMQP 协议连接
		conn, err = amqp.Dial(amqpURL)
		if err == nil {
			break // 连接成功，跳出循环
		}
		// 连接失败，打印日志并等待后重试
		fmt.Printf("[RabbitMQ] Connect attempt %d failed: %v, retrying...\n", i+1, err)
		// time.Duration(i+1) * time.Second：
		//   第1次失败等 1秒，第2次等2秒，第3次等3秒...（线性退避）
		time.Sleep(time.Duration(i+1) * time.Second)
	}
	// 如果 10 次都失败了，err 不为 nil，直接 panic 终止程序
	if err != nil {
		panic(fmt.Sprintf("Failed to connect to RabbitMQ: %v", err))
	}

	// conn.Channel() 在已有的 TCP 连接上创建 AMQP 通道
	ch, err := conn.Channel()
	if err != nil {
		panic(fmt.Sprintf("Failed to open channel: %v", err))
	}

	fmt.Println("[RabbitMQ] Connected successfully")
	return conn, ch
}
