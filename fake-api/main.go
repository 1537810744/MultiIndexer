// ============================================================================
// main.go — Fake REST API（数据展示服务）
// ============================================================================
// Fake API 是一个临时的 HTTP 服务，用于在开发阶段实时展示系统中的数据。
// 它不是最终的生产 API，而是一个"数据窗口"，让你确认整个流程是否正确。
//
// 提供的功能：
//   1. HTTP 端点 — 查询 MySQL 和 Redis 中的数据
//   2. 实时控制台打印 — 订阅 RabbitMQ，将每条新事件打印到控制台
//   3. 简易网页仪表板 — 浏览器打开 localhost:9090 可以看到数据和链接
//
// 【为什么叫 "Fake" API？】
// 因为阶段 2 的目标是验证数据流（Listener → RabbitMQ → Processor → MySQL/Redis）
// 是否正确，而不是构建生产级的 REST API。
// 阶段 3 会用 Nginx + 正式的 RESTful API 替换这套临时方案。
//
// 【Go 语法：全局变量】
// var ( ... ) 是 Go 的"变量块"语法，可以一次声明多个变量。
// 全局变量在整个包内可见，但通常不推荐过多使用（增加耦合）。
// 这里为了方便 HTTP handler 访问数据库和 Redis 连接，使用了全局变量。
// ============================================================================

package main

import (
	"context"       // Go 上下文
	"database/sql"  // 数据库抽象层
	"encoding/json" // JSON
	"fmt"           // 格式化输出
	"io"            // IO 操作
	"net/http"      // HTTP 服务器
	"os"            // 环境变量
	"strings"       // 字符串操作
	"time"          // 时间处理

	_ "github.com/go-sql-driver/mysql"      // MySQL 驱动（空白导入注册）
	amqp "github.com/rabbitmq/amqp091-go"   // RabbitMQ 客户端
	"github.com/redis/go-redis/v9"          // Redis 客户端
)

// 全局变量：数据库、缓存、消息队列的连接
// HTTP handler 函数需要通过这些变量访问数据。
var (
	db    *sql.DB        // MySQL 数据库连接池
	rdb   *redis.Client  // Redis 客户端
	rmqCh *amqp.Channel  // RabbitMQ 通道（用于实时打印）
)

// ============================================================================
// main() — 入口函数
// ============================================================================
func main() {
	// ============================================================
	// 读取配置
	// ============================================================
	amqpURL := getEnv("RABBITMQ_URL", "amqp://admin:admin@localhost:5672/")
	mysqlDSN := getEnv("MYSQL_DSN", "indexer:indexerpass@tcp(localhost:3307)/indexer?parseTime=true")
	redisURL := getEnv("REDIS_URL", "localhost:6379")
	port := getEnv("LISTEN_PORT", "9090") // 监听端口，默认 9090（避免和常见端口冲突）

	fmt.Println("==========================================")
	fmt.Println("[FakeAPI] MultiIndexer Data Display Service")
	fmt.Println("==========================================")

	// ============================================================
	// 连接 MySQL（可选：连不上也继续运行，只是不提供 MySQL 数据）
	// ============================================================
	var err error
	db, err = sql.Open("mysql", mysqlDSN)
	if err != nil {
		fmt.Printf("[FakeAPI] MySQL connection error: %v (continuing without DB)\n", err)
	} else {
		db.SetMaxOpenConns(10)              // 最大连接数
		db.SetMaxIdleConns(3)               // 空闲连接数
		db.SetConnMaxLifetime(3 * time.Minute)
		if err := db.Ping(); err != nil {
			fmt.Printf("[FakeAPI] MySQL ping error: %v (continuing without DB)\n", err)
		} else {
			fmt.Println("[FakeAPI] MySQL connected")
		}
	}

	// ============================================================
	// 连接 Redis（可选：连不上也继续运行）
	// ============================================================
	rdb = redis.NewClient(&redis.Options{
		Addr:         redisURL,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		fmt.Printf("[FakeAPI] Redis connection error: %v (continuing without Redis)\n", err)
	} else {
		fmt.Println("[FakeAPI] Redis connected")
	}

	// ============================================================
	// 连接 RabbitMQ 并订阅所有事件（用于实时控制台打印）
	// ============================================================
	rmqConn, rmqCh, err := connectRabbitMQ(amqpURL)
	if err != nil {
		fmt.Printf("[FakeAPI] RabbitMQ connection error: %v (continuing without RMQ)\n", err)
	} else {
		defer rmqConn.Close()
		defer rmqCh.Close()

		// 声明一个临时队列接收所有事件
		// exclusive=true：这个队列只属于当前连接，连接断开自动删除
		// autoDelete=true：没有消费者时自动删除
		q, err := rmqCh.QueueDeclare(
			"fake-api-display-queue", // 队列名
			false,                     // durable = false（临时队列，不持久化）
			true,                      // autoDelete = true（没有消费者自动删除）
			true,                      // exclusive = true（独占队列）
			false,                     // no-wait
			nil,
		)
		if err == nil {
			// 绑定所有 routing key（# 匹配一切）
			rmqCh.QueueBind(q.Name, "#", "indexer.tx", false, nil)

			// 开始消费（auto-ack=true，因为只是打印，不需要可靠确认）
			msgs, err := rmqCh.Consume(
				q.Name,
				"",   // 空消费者标签
				true, // auto-ack = true（只是显示，不需要可靠投递）
				false, false, false, nil,
			)
			if err == nil {
				fmt.Println("[FakeAPI] Subscribed to all RabbitMQ events for real-time display")
				// 在后台 goroutine 中实时打印
				go realTimePrinter(msgs)
			}
		}
	}

	// ============================================================
	// 注册 HTTP 路由并启动服务器
	// ============================================================
	// 【Go 语法：http.NewServeMux 和路由注册】
	// ServeMux 是 Go 标准库的 HTTP 路由器，将 URL 路径映射到处理函数。
	// http.HandleFunc 注册一个路径对应的处理函数。
	// 注意：Go 的 ServeMux 不支持路径参数（/api/events/:chain），
	// 所以 /api/events/ 用了前缀匹配，在 handler 里手动解析路径。
	mux := http.NewServeMux()
	mux.HandleFunc("/", handleIndex)               // 首页（仪表板 HTML）
	mux.HandleFunc("/api/health", handleHealth)     // 健康检查
	mux.HandleFunc("/api/events", handleEvents)     // 获取所有事件（/api/events 精确匹配）
	mux.HandleFunc("/api/events/", handleEventsByChain) // 按链获取事件（/api/events/ 前缀匹配）
	mux.HandleFunc("/api/alerts", handleAlerts)     // 获取告警
	mux.HandleFunc("/api/stats", handleStats)       // 获取统计
	mux.HandleFunc("/api/redis", handleRedisData)   // 获取 Redis 原始数据

	addr := ":" + port
	fmt.Printf("[FakeAPI] HTTP server starting on http://localhost%s\n", addr)
	fmt.Println("[FakeAPI] Endpoints:")
	fmt.Println("  GET /              - Overview & links")
	fmt.Println("  GET /api/health    - Health check")
	fmt.Println("  GET /api/events    - Latest events (all chains)")
	fmt.Println("  GET /api/events/eth - Events by chain (eth/bsc/sol)")
	fmt.Println("  GET /api/alerts    - Latest alerts")
	fmt.Println("  GET /api/stats     - Statistics")
	fmt.Println("  GET /api/redis     - Redis cache data")
	fmt.Println("==========================================")

	// ============================================================
	// 【Go 语法：http.ListenAndServe】
	// 启动 HTTP 服务器，监听指定地址。这个函数会阻塞，直到进程被终止。
	// 如果出错（如端口被占用），返回 error。
	// ============================================================
	if err := http.ListenAndServe(addr, mux); err != nil {
		panic(fmt.Sprintf("HTTP server error: %v", err))
	}
}

// ============================================================================
// HTTP Handlers（处理函数）
// ============================================================================
// 每个 handler 的函数签名必须是：func(http.ResponseWriter, *http.Request)
// ResponseWriter：用于写响应（状态码、Header、Body）
// Request：包含请求的所有信息（URL、Header、Body 等）

// handleIndex — 返回仪表板 HTML 页面。
func handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	// io.WriteString 是向 Writer 写字符串的简便方法
	io.WriteString(w, `<!DOCTYPE html>
<html><head><title>MultiIndexer Phase 2 - Data Display</title>
<style>
  body { font-family: monospace; background: #0d1117; color: #c9d1d9; margin: 20px; }
  a { color: #58a6ff; text-decoration: none; }
  a:hover { text-decoration: underline; }
  h1 { color: #f0883e; border-bottom: 2px solid #30363d; padding-bottom: 10px; }
  h2 { color: #7ee787; }
  .container { max-width: 800px; margin: 0 auto; }
  pre { background: #161b22; padding: 10px; border-radius: 6px; overflow-x: auto; }
  .status { color: #7ee787; }
  .warning { color: #d2991d; }
  .error { color: #f85149; }
  .endpoint { margin: 8px 0; padding: 8px; background: #161b22; border-radius: 4px; }
  .method { color: #79c0ff; font-weight: bold; }
</style></head><body><div class="container">
<h1>MultiIndexer Phase 2</h1>
<h2>Data Display Dashboard</h2>
<p>Real-time blockchain event indexer — ETH / BSC / SOL</p>

<h2>API Endpoints</h2>
<div class="endpoint"><span class="method">GET</span> <a href="/api/health">/api/health</a> — Health check</div>
<div class="endpoint"><span class="method">GET</span> <a href="/api/events">/api/events</a> — Latest 50 events (all chains)</div>
<div class="endpoint"><span class="method">GET</span> <a href="/api/events/eth">/api/events/eth</a> — Ethereum events</div>
<div class="endpoint"><span class="method">GET</span> <a href="/api/events/bsc">/api/events/bsc</a> — BSC events</div>
<div class="endpoint"><span class="method">GET</span> <a href="/api/events/sol">/api/events/sol</a> — Solana events</div>
<div class="endpoint"><span class="method">GET</span> <a href="/api/alerts">/api/alerts</a> — Latest 50 alerts</div>
<div class="endpoint"><span class="method">GET</span> <a href="/api/stats">/api/stats</a> — Statistics summary</div>
<div class="endpoint"><span class="method">GET</span> <a href="/api/redis">/api/redis</a> — Redis cache snapshot</div>

<h2>System Status</h2>
<pre id="health">Loading...</pre>

<h2>Real-time Console Output</h2>
<p>Check the console running <code>fake-api.exe</code> for live event streaming.</p>
</div>
<script>
fetch('/api/health').then(r=>r.json()).then(d=>{
  document.getElementById('health').textContent = JSON.stringify(d, null, 2);
});
</script>
</body></html>`)
}

// handleHealth — 健康检查，返回各组件连接状态。
func handleHealth(w http.ResponseWriter, r *http.Request) {
	resp := map[string]interface{}{
		"status": "ok",
		"time":   time.Now().Format(time.RFC3339), // RFC3339 = "2006-01-02T15:04:05Z07:00"
	}

	// 检查 MySQL 连接
	dbOK := false
	if db != nil {
		dbOK = db.Ping() == nil
	}
	resp["mysql"] = dbOK

	// 检查 Redis 连接
	redisOK := false
	if rdb != nil {
		redisOK = rdb.Ping(context.Background()).Err() == nil
	}
	resp["redis"] = redisOK

	writeJSON(w, resp)
}

// handleEvents — 获取所有链的最近事件。
// 优先从 MySQL 读取（完整数据），MySQL 不可用时回退到 Redis。
func handleEvents(w http.ResponseWriter, r *http.Request) {
	// 优先尝试 MySQL
	if db != nil {
		rows, err := db.Query(`
			SELECT chain, block_number, tx_hash, category, event_type,
				contract_address, from_address, to_address, amount, symbol, created_at
			FROM events ORDER BY id DESC LIMIT 50
		`)
		if err == nil {
			defer rows.Close()
			events := scanEvents(rows)
			writeJSON(w, map[string]interface{}{
				"source": "mysql",
				"count":  len(events),
				"events": events,
			})
			return
		}
	}

	// 回退到 Redis
	if rdb != nil {
		ctx := context.Background()
		events := []string{}
		for _, chain := range []string{"eth", "bsc", "sol"} {
			vals, _ := rdb.LRange(ctx, "events:"+chain, 0, 19).Result()
			events = append(events, vals...)
		}
		writeJSON(w, map[string]interface{}{
			"source": "redis",
			"count":  len(events),
			"events": events,
		})
		return
	}

	writeJSON(w, map[string]string{"error": "no data source available"})
}

// handleEventsByChain — 获取指定链的事件（/api/events/eth, /api/events/bsc, /api/events/sol）
func handleEventsByChain(w http.ResponseWriter, r *http.Request) {
	// 【Go 语法：strings.TrimPrefix】
	// 从 URL 路径中去掉 "/api/events/" 前缀，剩下的就是链名
	chain := strings.TrimPrefix(r.URL.Path, "/api/events/")
	if chain == "" || (chain != "eth" && chain != "bsc" && chain != "sol") {
		writeJSON(w, map[string]string{"error": "invalid chain, use eth/bsc/sol"})
		return
	}

	if db != nil {
		rows, err := db.Query(`
			SELECT chain, block_number, tx_hash, category, event_type,
				contract_address, from_address, to_address, amount, symbol, created_at
			FROM events WHERE chain = ? ORDER BY id DESC LIMIT 50
		`, chain)
		if err == nil {
			defer rows.Close()
			events := scanEvents(rows)
			writeJSON(w, map[string]interface{}{
				"source": "mysql",
				"chain":  chain,
				"count":  len(events),
				"events": events,
			})
			return
		}
	}

	writeJSON(w, map[string]string{"error": "no data source available"})
}

// handleAlerts — 获取最近告警。
func handleAlerts(w http.ResponseWriter, r *http.Request) {
	if db != nil {
		rows, err := db.Query(`
			SELECT chain, alert_type, tx_hash, severity, detail, created_at
			FROM alerts ORDER BY id DESC LIMIT 50
		`)
		if err == nil {
			defer rows.Close()
			var alerts []map[string]interface{}
			for rows.Next() {
				var chain, alertType, txHash, severity, detail string
				var createdAt time.Time
				rows.Scan(&chain, &alertType, &txHash, &severity, &detail, &createdAt)
				alerts = append(alerts, map[string]interface{}{
					"chain":      chain,
					"alert_type": alertType,
					"tx_hash":    txHash,
					"severity":   severity,
					"detail":     detail,
					"created_at": createdAt.Format(time.RFC3339),
				})
			}
			writeJSON(w, map[string]interface{}{
				"source": "mysql",
				"count":  len(alerts),
				"alerts": alerts,
			})
			return
		}
	}

	writeJSON(w, map[string]string{"error": "no data source available"})
}

// handleStats — 获取统计信息（MySQL + Redis 数据汇总）。
func handleStats(w http.ResponseWriter, r *http.Request) {
	stats := make(map[string]interface{})

	// MySQL 统计
	if db != nil {
		var totalEvents, totalAlerts, totalBlocks int
		db.QueryRow("SELECT COUNT(*) FROM events").Scan(&totalEvents)
		db.QueryRow("SELECT COUNT(*) FROM alerts").Scan(&totalAlerts)
		db.QueryRow("SELECT COUNT(*) FROM blocks").Scan(&totalBlocks)
		stats["mysql_total_events"] = totalEvents
		stats["mysql_total_alerts"] = totalAlerts
		stats["mysql_total_blocks"] = totalBlocks

		// 按链分组统计
		rows, _ := db.Query("SELECT chain, COUNT(*) FROM events GROUP BY chain")
		if rows != nil {
			defer rows.Close()
			for rows.Next() {
				var c string
				var n int
				rows.Scan(&c, &n)
				stats["mysql_events_"+c] = n
			}
		}
	}

	// Redis 统计
	if rdb != nil {
		ctx := context.Background()
		for _, chain := range []string{"eth", "bsc", "sol"} {
			if val, err := rdb.Get(ctx, "latest_block:"+chain).Result(); err == nil {
				stats["redis_"+chain+"_latest_block"] = val
			}
			if val, err := rdb.Get(ctx, "stats:"+chain+":event_count").Result(); err == nil {
				stats["redis_"+chain+"_events"] = val
			}
			if val, err := rdb.Get(ctx, "stats:"+chain+":alert_count").Result(); err == nil {
				stats["redis_"+chain+"_alerts"] = val
			}
		}
	}

	stats["time"] = time.Now().Format(time.RFC3339)
	writeJSON(w, stats)
}

// handleRedisData — 获取 Redis 中的原始缓存数据（用于调试）。
func handleRedisData(w http.ResponseWriter, r *http.Request) {
	if rdb == nil {
		writeJSON(w, map[string]string{"error": "redis not connected"})
		return
	}

	ctx := context.Background()
	data := make(map[string]interface{})

	// 获取每条链的最近事件
	for _, chain := range []string{"eth", "bsc", "sol"} {
		events, _ := rdb.LRange(ctx, "events:"+chain, 0, 9).Result()
		if len(events) > 0 {
			data["events_"+chain] = events
		}
	}

	// 获取最近告警
	alerts, _ := rdb.LRange(ctx, "recent_alerts", 0, 9).Result()
	if len(alerts) > 0 {
		data["recent_alerts"] = alerts
	}

	// 获取每条链的最新区块
	for _, chain := range []string{"eth", "bsc", "sol"} {
		block, _ := rdb.Get(ctx, "latest_block:"+chain).Result()
		if block != "" {
			data[chain+"_latest_block"] = block
		}
	}

	data["source"] = "redis"
	writeJSON(w, data)
}

// ============================================================================
// realTimePrinter — 实时控制台打印器
// ============================================================================
// 【Go 并发：后台 goroutine 消费 RabbitMQ 消息并实时打印】
// 这个函数在一个独立的 goroutine 中运行，不断从 RabbitMQ 接收消息
// 并格式化打印到控制台。这样你可以看到数据正在实时流动。
func realTimePrinter(msgs <-chan amqp.Delivery) {
	count := 0
	for msg := range msgs {
		count++

		// 解析为通用 map（这样即使字段不全也能显示）
		var ev map[string]interface{}
		json.Unmarshal(msg.Body, &ev)

		// 类型断言：从 interface{} 中提取实际类型
		// 【Go 语法：类型断言 val, ok := x.(string)】
		// 如果 x 确实是 string 类型，ok=true，val 是字符串值
		// 如果 x 不是 string，ok=false，val 是空字符串
		chain, _ := ev["chain"].(string)
		category, _ := ev["category"].(string)
		eventType, _ := ev["event_type"].(string)
		txHash, _ := ev["tx_hash"].(string)
		from, _ := ev["from_address"].(string)
		to, _ := ev["to_address"].(string)
		amount, _ := ev["amount"].(string)
		symbol, _ := ev["symbol"].(string)

		unit := symbol
		if unit == "" {
			unit = chain
		}
		fmt.Printf("\n[FakeAPI-REALTIME] #%d [%s] %s.%s\n", count, chain, category, eventType)
		fmt.Printf("  TX:   %s\n", txHash)
		fmt.Printf("  From: %s\n", from)
		fmt.Printf("  To:   %s\n", to)
		fmt.Printf("  Amount: %s %s\n", amount, unit)
		fmt.Println("  --------------------")
	}
}

// ============================================================================
// 工具函数
// ============================================================================

// scanEvents 将 database/sql 的 Rows 转换为 []map 格式。
// 通用格式方便 JSON 序列化返回给前端。
func scanEvents(rows *sql.Rows) []map[string]interface{} {
	var events []map[string]interface{}
	for rows.Next() {
		var chain, txHash, category, eventType, contractAddr string
		var blockNumber uint64
		var fromAddr, toAddr, amount, symbol sql.NullString
		var createdAt time.Time

		if err := rows.Scan(&chain, &blockNumber, &txHash, &category, &eventType,
			&contractAddr, &fromAddr, &toAddr, &amount, &symbol, &createdAt); err != nil {
			fmt.Printf("[FakeAPI] scanEvents error: %v\n", err)
			continue
		}

		events = append(events, map[string]interface{}{
			"chain":            chain,
			"block_number":     blockNumber,
			"tx_hash":          txHash,
			"category":         category,
			"event_type":       eventType,
			"contract_address": contractAddr,
			"from_address":     fromAddr.String,
			"to_address":       toAddr.String,
			"amount":           amount.String,
			"symbol":           symbol.String,
			"created_at":       createdAt.Format(time.RFC3339),
		})
	}
	return events
}

// writeJSON 统一的 JSON 响应写入函数。
// 设置 Content-Type、CORS 头，然后序列化并写入响应。
func writeJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Access-Control-Allow-Origin", "*") // 允许跨域访问（开发阶段方便调试）
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ") // 格式化输出（缩进 2 空格），方便人类阅读
	enc.Encode(data)
}

// getEnv 读取环境变量，不存在时返回默认值。
func getEnv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

// connectRabbitMQ 连接 RabbitMQ，带指数退避重试。
// 返回 (Connection, Channel, error)，与其他组件不同的是它不 panic 而是返回 error，
// 因为 fake-api 设计为容忍 RabbitMQ 不可用（降级到只用 MySQL/Redis 显示数据）。
func connectRabbitMQ(amqpURL string) (*amqp.Connection, *amqp.Channel, error) {
	var conn *amqp.Connection
	var err error

	// 指数退避重试
	for i := 0; i < 10; i++ {
		conn, err = amqp.Dial(amqpURL)
		if err == nil {
			break
		}
		fmt.Printf("[RabbitMQ] Connect attempt %d failed: %v, retrying...\n", i+1, err)
		time.Sleep(time.Duration(i+1) * time.Second)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("connect failed: %v", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close() // Channel 创建失败要关闭 Connection
		return nil, nil, fmt.Errorf("channel failed: %v", err)
	}

	fmt.Println("[RabbitMQ] Connected successfully")
	return conn, ch, nil
}
