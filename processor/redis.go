// ============================================================================
// redis.go — Redis 缓存操作层
// ============================================================================
// RedisStore 封装了 Redis 客户端连接和所有缓存操作方法。
//
// 【Redis 是什么？】
// Redis（Remote Dictionary Server）是一个内存中的键值数据库。
// 它的数据存在内存里，读写非常快（微秒级），适合用作缓存。
// 在本系统中，Redis 存"热数据"（最近的交易），MySQL 存"冷数据"（全部历史）。
//
// 【本系统使用的 Redis 数据结构】
// 1. List（列表）— 存储最近的事件和告警
//    LPUSH：从列表左侧推入新元素
//    LTRIM：只保留前 N 个元素（实现"最近 N 条"的滚动窗口）
//    LRANGE：获取指定范围内的元素
//
// 2. String（字符串）— 存储计数器、最新区块号
//    SET：设置键值
//    GET：获取值
//    INCR：原子自增（用于计数器）
//
// 3. Pipeline（管道）— 批量执行命令
//    把多个 Redis 命令打包成一个请求发给服务器，减少网络往返。
//    不是事务（不保证原子性），但能大幅提升性能。
//
// 【Go 语法：context.Context】
// Context 是 Go 中传递"上下文"的标准方式。它可以在调用链中传递：
//   - 超时时间（到时间自动取消）
//   - 取消信号（手动取消正在进行的操作）
//   - 请求范围的值（如 trace ID）
// context.Background() 是最顶层的空 Context，没有超时也没有值。
// ============================================================================

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9" // Redis Go 客户端 v9
)

// ============================================================================
// RedisStore — Redis 缓存存储
// ============================================================================
type RedisStore struct {
	client *redis.Client // Redis 客户端连接
}

// ============================================================================
// NewRedisStore() — 创建 Redis 连接
// ============================================================================
func NewRedisStore(addr string) (*RedisStore, error) {
	// redis.Options 配置 Redis 连接参数
	client := redis.NewClient(&redis.Options{
		Addr:         addr,              // Redis 地址（如 localhost:6379）
		Password:     "",                // 密码（本系统没设密码）
		DB:           0,                 // 使用 0 号数据库（Redis 有 0-15 共 16 个数据库）
		DialTimeout:  5 * time.Second,  // 建立连接的超时时间
		ReadTimeout:  3 * time.Second,  // 读操作的超时时间
		WriteTimeout: 3 * time.Second,  // 写操作的超时时间
	})

	// 带超时的 Ping 检查连接是否可用
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel() // 释放计时器资源

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %v", err)
	}

	fmt.Println("[Redis] Connected successfully")
	return &RedisStore{client: client}, nil
}

// Close 关闭 Redis 连接。
func (r *RedisStore) Close() {
	if r.client != nil {
		r.client.Close()
	}
}

// ============================================================================
// CacheEvent() — 缓存事件到 Redis
// ============================================================================
// 【Redis 概念：Pipeline（管道）】
// 以下操作被包装在一个 Pipeline 中，一次网络往返完成全部 7 个命令：
//   1. 将事件 JSON 推入 events:{chain} 列表左侧
//   2. 裁剪列表只保留前 1000 条
//   3. 更新 latest_block:{chain} 字符串
//   4. 自增 stats:{chain}:event_count 计数器
//   5. 如果发送方不是零地址，缓存到 addr:{chain}:{from} 列表
//   6. 如果接收方不是零地址，缓存到 addr:{chain}:{to} 列表
//   7. 裁剪地址列表只保留前 100 条
//
// Pipeline 适合需要一次执行多条命令但不需要事务保证的场景。
func (r *RedisStore) CacheEvent(ctx context.Context, ev *ChainEvent) error {
	pipe := r.client.Pipeline() // 创建管道

	// 按链缓存最近事件（每个链一个列表，最多保留 1000 条）
	key := fmt.Sprintf("events:%s", ev.Chain)
	eventJSON, _ := json.Marshal(ev)       // 事件转 JSON
	pipe.LPush(ctx, key, string(eventJSON)) // 推到列表左侧（最新的在前面）
	pipe.LTrim(ctx, key, 0, 999)            // 只保留前 1000 条（索引 0-999）

	// 记录每条链最新处理到的区块号
	blockKey := fmt.Sprintf("latest_block:%s", ev.Chain)
	pipe.Set(ctx, blockKey, ev.BlockNumber, 0) // 0 表示永不过期

	// 自增事件计数器
	counterKey := fmt.Sprintf("stats:%s:event_count", ev.Chain)
	pipe.Incr(ctx, counterKey)

	// 按地址缓存最近事件（用于按地址查询）
	// 排除零地址（0x000...0），因为那是 Mint/Burn 的虚拟地址，不是真实用户
	zeroAddr := "0x0000000000000000000000000000000000000000"
	if ev.FromAddr != "" && ev.FromAddr != zeroAddr {
		addrKey := fmt.Sprintf("addr:%s:%s", ev.Chain, ev.FromAddr)
		pipe.LPush(ctx, addrKey, string(eventJSON))
		pipe.LTrim(ctx, addrKey, 0, 99) // 每个地址只保留最近 100 条
	}
	if ev.ToAddr != "" && ev.ToAddr != zeroAddr {
		addrKey := fmt.Sprintf("addr:%s:%s", ev.Chain, ev.ToAddr)
		pipe.LPush(ctx, addrKey, string(eventJSON))
		pipe.LTrim(ctx, addrKey, 0, 99)
	}

	// 执行管道中的所有命令
	_, err := pipe.Exec(ctx)
	return err
}

// ============================================================================
// CacheAlert() — 缓存告警到 Redis
// ============================================================================
func (r *RedisStore) CacheAlert(ctx context.Context, chain, alertType, txHash, severity, detail string) error {
	pipe := r.client.Pipeline()

	// 构建告警数据
	alertData := map[string]string{
		"chain":      chain,
		"alert_type": alertType,
		"tx_hash":    txHash,
		"severity":   severity,
		"detail":     detail,
		"time":       time.Now().Format(time.RFC3339),
	}
	alertJSON, _ := json.Marshal(alertData)

	// 推入最近告警列表，保留 500 条
	key := "recent_alerts"
	pipe.LPush(ctx, key, string(alertJSON))
	pipe.LTrim(ctx, key, 0, 499)

	// 自增告警计数器（按链）
	alertCounter := fmt.Sprintf("stats:%s:alert_count", chain)
	pipe.Incr(ctx, alertCounter)

	_, err := pipe.Exec(ctx)
	return err
}

// ============================================================================
// GetRecentEvents() — 获取指定链的最近事件（fake-api 用）
// ============================================================================
// LRANGE 获取列表中指定范围的元素，索引从 0 开始。
// 例如 LRANGE events:eth 0 19 返回最近 20 条 eth 事件。
func (r *RedisStore) GetRecentEvents(ctx context.Context, chain string, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 20 // 默认 20 条
	}
	key := fmt.Sprintf("events:%s", chain)
	return r.client.LRange(ctx, key, 0, int64(limit-1)).Result()
}

// ============================================================================
// GetRecentAlerts() — 获取最近告警
// ============================================================================
func (r *RedisStore) GetRecentAlerts(ctx context.Context, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 20
	}
	return r.client.LRange(ctx, "recent_alerts", 0, int64(limit-1)).Result()
}

// ============================================================================
// GetStats() — 获取 Redis 中的统计信息
// ============================================================================
func (r *RedisStore) GetStats(ctx context.Context) map[string]string {
	stats := make(map[string]string)

	for _, chain := range []string{"eth", "bsc", "sol"} {
		// 最新区块号
		if val, err := r.client.Get(ctx, fmt.Sprintf("latest_block:%s", chain)).Result(); err == nil {
			stats[chain+"_latest_block"] = val
		}
		// 事件计数
		if val, err := r.client.Get(ctx, fmt.Sprintf("stats:%s:event_count", chain)).Result(); err == nil {
			stats[chain+"_event_count"] = val
		}
		// 告警计数
		if val, err := r.client.Get(ctx, fmt.Sprintf("stats:%s:alert_count", chain)).Result(); err == nil {
			stats[chain+"_alert_count"] = val
		}
	}

	return stats
}

// Client 返回底层的 Redis 客户端（供需要直接使用的场景）。
func (r *RedisStore) Client() *redis.Client {
	return r.client
}
