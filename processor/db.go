// ============================================================================
// db.go — MySQL 数据库操作层
// ============================================================================
// DBPool 封装了 MySQL 连接池和所有数据库操作方法。
//
// 【Go 语法：database/sql 包】
// database/sql 是 Go 标准库中的数据库抽象层。它不提供具体数据库驱动，
// 而是定义了统一的接口。具体驱动（如 MySQL）通过 "import _" 的方式注册。
//
// 【Go 语法：import _ "github.com/go-sql-driver/mysql"】
// 下划线 _ 是"空白导入"：只执行包的 init() 函数，不使用包的任何导出符号。
// MySQL 驱动的 init() 函数会把自己注册到 database/sql 包中。
// 之后 sql.Open("mysql", dsn) 就能找到它。
//
// 【数据库概念：连接池（Connection Pool）】
// sql.DB 不是单个连接，而是一个连接池。它维护一组可复用的数据库连接。
// 当调用 db.Exec/Query 时，从池中借一个连接，用完还回去。
// 这样避免了频繁创建/销毁 TCP 连接的开销。
// ============================================================================

package main

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql" // 空白导入：注册 MySQL 驱动
)

// ============================================================================
// DBPool — 数据库连接池包装
// ============================================================================
type DBPool struct {
	db *sql.DB // Go 标准库的数据库连接池
}

// ============================================================================
// NewDBPool() — 创建 MySQL 连接池
// ============================================================================
// DSN（Data Source Name）格式：
//   username:password@tcp(host:port)/database?params
//   例如：indexer:indexerpass@tcp(localhost:3306)/indexer?parseTime=true
func NewDBPool(dsn string) (*DBPool, error) {
	// sql.Open 不会立即连接数据库，只是初始化连接池
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("sql.Open: %v", err)
	}

	// 连接池配置
	db.SetMaxOpenConns(25)              // 最多同时打开 25 个连接
	db.SetMaxIdleConns(5)               // 空闲连接池保留 5 个连接
	db.SetConnMaxLifetime(5 * time.Minute) // 连接最长存活 5 分钟（避免 MySQL 端超时断开）

	// Ping 才是真正验证连接是否可用
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("db.Ping: %v", err)
	}

	fmt.Println("[MySQL] Connected successfully")
	return &DBPool{db: db}, nil
}

// Close 关闭数据库连接池。
func (p *DBPool) Close() {
	if p.db != nil {
		p.db.Close()
	}
}

// ============================================================================
// InsertBlock() — 插入区块记录（幂等）
// ============================================================================
// 【MySQL 语法：INSERT IGNORE】
// 如果违反唯一索引（uk_chain_number），不报错，静默跳过。
// 这保证了同一个区块不会被重复插入。
func (p *DBPool) InsertBlock(chain string, blockNumber uint64, blockHash string, blockTime int64, txCount int) error {
	_, err := p.db.Exec(`
		INSERT IGNORE INTO blocks (chain, block_number, block_hash, block_time, tx_count)
		VALUES (?, ?, ?, FROM_UNIXTIME(?), ?)
	`, chain, blockNumber, blockHash, blockTime, txCount)
	// FROM_UNIXTIME() 是 MySQL 函数：Unix 时间戳 → DATETIME 类型
	return err
}

// ============================================================================
// InsertEvent() — 插入事件记录（幂等更新）
// ============================================================================
// 【MySQL 语法：ON DUPLICATE KEY UPDATE】
// 如果违反唯一索引（uk_tx_log: tx_hash + log_index），
// 不报错，而是更新 block_number 和 raw_data 字段。
// 这保证了同一笔交易的同一个 log 不会重复插入，但数据可以更新。
//
// 【Go 语法：参数化查询（Prepared Statement）】
// Exec 方法中的 ? 是占位符，Go 数据库驱动会把参数安全地填入，
// 自动做转义，防止 SQL 注入。永远不要用 fmt.Sprintf 拼接 SQL！
func (p *DBPool) InsertEvent(ev *ChainEvent) error {
	_, err := p.db.Exec(`
		INSERT INTO events (chain, block_number, tx_hash, log_index, category, event_type,
			contract_address, from_address, to_address, token_id, amount, symbol, decimals, raw_data)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			block_number = VALUES(block_number),
			raw_data = VALUES(raw_data)
	`, ev.Chain, ev.BlockNumber, ev.TxHash, ev.LogIndex, ev.Category, ev.EventType,
		ev.ContractAddr, ev.FromAddr, ev.ToAddr, ev.TokenID, ev.Amount, ev.Symbol, ev.Decimals, ev.RawData)
	return err
}

// ============================================================================
// InsertAlert() — 插入告警记录
// ============================================================================
// 告警没有唯一键约束，同一笔交易可以产生多个告警（如同时触发大额和监控地址）
func (p *DBPool) InsertAlert(chain, alertType, txHash, severity, detail string) error {
	_, err := p.db.Exec(`
		INSERT INTO alerts (chain, alert_type, tx_hash, severity, detail)
		VALUES (?, ?, ?, ?, ?)
	`, chain, alertType, txHash, severity, detail)
	return err
}

// ============================================================================
// UpdateIndexerState() — 更新索引状态
// ============================================================================
// 记录每条链最近处理到哪个区块了。
// 【MySQL 语法：ON DUPLICATE KEY UPDATE】
// chain 是 PRIMARY KEY，如果已存在就更新 last_block 和 updated_at。
func (p *DBPool) UpdateIndexerState(chain string, lastBlock uint64) error {
	_, err := p.db.Exec(`
		INSERT INTO indexer_state (chain, last_block) VALUES (?, ?)
		ON DUPLICATE KEY UPDATE last_block = VALUES(last_block), updated_at = CURRENT_TIMESTAMP
	`, chain, lastBlock)
	return err
}

// ============================================================================
// QueryRecentEvents() — 查询最近的事件（fake-api 使用）
// ============================================================================
// 返回 []map[string]interface{} — 通用的"字典列表"格式，方便 JSON 序列化。
func (p *DBPool) QueryRecentEvents(limit int) ([]map[string]interface{}, error) {
	rows, err := p.db.Query(`
		SELECT chain, block_number, tx_hash, category, event_type,
			contract_address, from_address, to_address, amount, symbol, created_at
		FROM events ORDER BY id DESC LIMIT ?
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close() // 【重要】rows 必须关闭，否则数据库连接不会归还连接池

	var results []map[string]interface{}
	for rows.Next() {
		var chain, txHash, category, eventType, contractAddr string
		var blockNumber uint64
		var createdAt time.Time
		// 【Go 语法：sql.NullString】
		// SQL 中的 NULL 不能直接映射到 Go 的 string（string 的零值是 ""，不是 nil）。
		// sql.NullString 有两个字段：String（值）和 Valid（是否为 NULL）。
		var nullableSymbol, nullableFrom, nullableTo, nullableAmount sql.NullString

		if err := rows.Scan(&chain, &blockNumber, &txHash, &category, &eventType,
			&contractAddr, &nullableFrom, &nullableTo, &nullableAmount, &nullableSymbol, &createdAt); err != nil {
			continue // 某行扫描失败，跳过
		}

		results = append(results, map[string]interface{}{
			"chain":            chain,
			"block_number":     blockNumber,
			"tx_hash":          txHash,
			"category":         category,
			"event_type":       eventType,
			"contract_address": contractAddr,
			"from_address":     nullableFrom.String,
			"to_address":       nullableTo.String,
			"amount":           nullableAmount.String,
			"symbol":           nullableSymbol.String,
			"created_at":       createdAt.Format(time.RFC3339),
		})
	}
	return results, nil
}

// ============================================================================
// QueryRecentAlerts() — 查询最近的告警
// ============================================================================
func (p *DBPool) QueryRecentAlerts(limit int) ([]map[string]interface{}, error) {
	rows, err := p.db.Query(`
		SELECT chain, alert_type, tx_hash, severity, detail, created_at
		FROM alerts ORDER BY id DESC LIMIT ?
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		var chain, alertType, txHash, severity, detail string
		var createdAt time.Time
		if err := rows.Scan(&chain, &alertType, &txHash, &severity, &detail, &createdAt); err != nil {
			continue
		}
		results = append(results, map[string]interface{}{
			"chain":      chain,
			"alert_type": alertType,
			"tx_hash":    txHash,
			"severity":   severity,
			"detail":     detail,
			"created_at": createdAt.Format(time.RFC3339),
		})
	}
	return results, nil
}

// ============================================================================
// QueryStats() — 查询整体统计数据
// ============================================================================
func (p *DBPool) QueryStats() (map[string]interface{}, error) {
	stats := make(map[string]interface{}) // 【Go 语法】make 初始化 map

	// 统计总数
	var totalEvents, totalAlerts, totalBlocks int
	p.db.QueryRow("SELECT COUNT(*) FROM events").Scan(&totalEvents)
	p.db.QueryRow("SELECT COUNT(*) FROM alerts").Scan(&totalAlerts)
	p.db.QueryRow("SELECT COUNT(*) FROM blocks").Scan(&totalBlocks)

	stats["total_events"] = totalEvents
	stats["total_alerts"] = totalAlerts
	stats["total_blocks"] = totalBlocks

	// 按链统计事件数
	rows, err := p.db.Query("SELECT chain, COUNT(*) FROM events GROUP BY chain")
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var chain string
			var count int
			rows.Scan(&chain, &count)
			stats["events_"+chain] = count
		}
	}

	// 按类别统计（token / nft）
	rows2, err := p.db.Query("SELECT category, COUNT(*) FROM events GROUP BY category")
	if err == nil {
		defer rows2.Close()
		for rows2.Next() {
			var cat string
			var count int
			rows2.Scan(&cat, &count)
			stats["category_"+cat] = count
		}
	}

	return stats, nil
}
