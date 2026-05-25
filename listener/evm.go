// ============================================================================
// evm.go — EVM 兼容链观察者（Ethereum + BSC）
// ============================================================================
// 这个文件实现了对以太坊（Ethereum）和币安智能链（BSC）的监听。
// 因为 BSC 兼容 EVM（以太坊虚拟机），所以它们的事件日志结构完全一样，
// 可以用同一套代码处理，区别只有 RPC 地址和轮询间隔。
//
// 【区块链概念：EVM 事件日志（Event Log）】
// 智能合约在执行时可以"发射"事件（emit Event），这些事件会被永久记录在区块链上。
// 每个事件日志包含：
//   - address：发射事件的合约地址（20字节）
//   - topics[]：事件的主题数组（每个32字节），用于索引和过滤
//     - topics[0]：事件签名哈希（keccak256("EventName(type1,type2,...)"）
//     - topics[1..n]：被标记为 indexed 的参数值
//   - data：未被 indexed 的参数值（ABI 编码的二进制数据）
//
// 【区块链概念：ERC-20 代币标准】
// ERC-20 是以太坊上最常用的"同质化代币"标准（如同美元，每张都一样）。
// 核心事件是 Transfer(address indexed from, address indexed to, uint256 value)
// - topics[0]：Transfer 事件签名哈希
// - topics[1]：发送方地址（indexed 参数，存在 topic 里）
// - topics[2]：接收方地址（indexed 参数）
// - data：转账金额 value（非 indexed，存在 data 里）
// - 一共 3 个 topic
//
// 【区块链概念：ERC-721 NFT 标准】
// ERC-721 是"非同质化代币"标准（如同房产证，每张都独一无二）。
// 它的 Transfer 事件签名和 ERC-20 一样！但多了一个 indexed 参数 tokenId：
// Transfer(address indexed from, address indexed to, uint256 indexed tokenId)
// - topics[0]：事件签名哈希（和 ERC-20 一样！）
// - topics[1]：发送方地址
// - topics[2]：接收方地址
// - topics[3]：Token ID（每个 NFT 的唯一编号）
// - 一共 4 个 topic
//
// **区分 ERC-20 和 ERC-721 的方法：数 topic 数量**
// 3 个 topic → ERC-20（token transfer）
// 4 个 topic → ERC-721（NFT transfer）
// ============================================================================

package main

import (
	"context"  // 上下文包，用于控制 RPC 请求的超时和取消
	"fmt"      // 格式化输出
	"math/big" // 大整数运算（区块链的数值经常超过 64 位，需要用大整数）
	"strings"  // 字符串操作（大小写转换等）
	"time"     // 时间处理

	"github.com/ethereum/go-ethereum"                // 以太坊 Go 客户端库
	"github.com/ethereum/go-ethereum/common"          // 通用类型：Address、Hash
	"github.com/ethereum/go-ethereum/core/types"      // 核心类型：Log（事件日志）
	"github.com/ethereum/go-ethereum/ethclient"       // 以太坊 RPC 客户端
	amqp "github.com/rabbitmq/amqp091-go"             // RabbitMQ 客户端
)

// ============================================================================
// 全局常量：已知的事件签名哈希
// ============================================================================
// 事件签名通过 keccak256 哈希算法计算，结果是固定的 32 字节十六进制字符串。
// 例如：keccak256("Transfer(address,address,uint256)")
//       = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
//
// 【Go 语法：var (...) 变量组】
// 用括号把多个 var 声明分组，比单独写 var 更整洁。
var (
	// ERC-20 / ERC-721 都用这个签名
	transferTopic = common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")

	// ERC-20 / ERC-721 的授权事件
	approvalTopic = common.HexToHash("0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925")

	// ERC-1155 的 TransferSingle 事件（多代币标准，一次转一种）
	transferSingleTopic = common.HexToHash("0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62")

	// ERC-1155 的 TransferBatch 事件（多代币标准，一次转多种）
	transferBatchTopic = common.HexToHash("0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb")

	// 零地址：0x0000000000000000000000000000000000000000
	// 在区块链中，from=零地址表示"铸造（Mint）"，to=零地址表示"销毁（Burn）"
	zeroAddr  = common.HexToAddress("0x0000000000000000000000000000000000000000")
	// zeroTopic 是零地址的 32 字节哈希表示（用于和 event topics 比较）
	// 因为 topics 中的地址会被左填充到 32 字节（Address 是 20 字节，Hash/Topic 是 32 字节）
	zeroTopic = common.BytesToHash(zeroAddr.Bytes())
)

// ============================================================================
// EVMWatcher — EVM 兼容链的观察者
// ============================================================================
type EVMWatcher struct {
	chain    string        // 链名（eth / bsc）
	rpcURL   string        // RPC 端点地址
	interval time.Duration // 轮询间隔
	ch       *amqp.Channel // RabbitMQ 通道

	client     *ethclient.Client            // 以太坊 RPC 客户端（go-ethereum 提供的封装）
	lastBlock  uint64                       // 上一次处理到的区块号（避免重复处理）
	watchAddrs map[common.Address]bool      // 要监控的钱包地址集合（用于告警高亮）
}

// NewEVMWatcher 创建一个新的 EVM 链观察者。
func NewEVMWatcher(chain, rpcURL string, interval time.Duration, ch *amqp.Channel, watchAddr string) *EVMWatcher {
	w := &EVMWatcher{
		chain:      chain,
		rpcURL:     rpcURL,
		interval:   interval,
		ch:         ch,
		watchAddrs: make(map[common.Address]bool), // make 初始化 map，必须有，否则是 nil map（写入会 panic）
	}
	// 如果配置了监控地址，加入观察集合
	if watchAddr != "" {
		// strings.ToLower 转为小写（以太坊地址大小写混合，但比较时统一用小写）
		w.watchAddrs[common.HexToAddress(strings.ToLower(watchAddr))] = true
	}
	return w
}

// ============================================================================
// Run — 观察者的主循环（运行在独立的 goroutine 中）
// ============================================================================
func (w *EVMWatcher) Run() {
	// defer + recover = 故障自愈（同 mock.go 中的解释）
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("[Watcher-%s] Panic recovered: %v, restarting in 5s...\n", w.chain, r)
			time.Sleep(5 * time.Second)
			go w.Run()
		}
	}()

	fmt.Printf("[Watcher-%s] Connecting to RPC: %s\n", w.chain, w.rpcURL)

	// 连接到 RPC 端点，带重试
	var err error
	for i := 0; i < 10; i++ {
		// ethclient.Dial 创建以太坊 RPC 客户端（支持 HTTP 和 WebSocket）
		w.client, err = ethclient.Dial(w.rpcURL)
		if err == nil {
			break
		}
		fmt.Printf("[Watcher-%s] RPC connect attempt %d failed: %v, retrying...\n", w.chain, i+1, err)
		time.Sleep(time.Duration(i+1) * time.Second)
	}
	if err != nil {
		fmt.Printf("[Watcher-%s] Failed to connect after retries: %v\n", w.chain, err)
		return // 连接失败，退出（但 defer 中的 recover 不会触发，因为没有 panic）
	}
	defer w.client.Close() // 确保退出时关闭 RPC 连接

	// 获取当前最新区块号，作为起始点
	// context.Background() 创建一个空的上下文（没有超时、没有取消信号）
	// context.WithTimeout 创建一个带超时的上下文（15秒后自动取消）
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	header, err := w.client.HeaderByNumber(ctx, nil) // nil 表示"最新区块"
	cancel() // 立即取消上下文（请求完成后不再需要）
	if err != nil {
		fmt.Printf("[Watcher-%s] Failed to get header: %v\n", w.chain, err)
		return
	}
	w.lastBlock = header.Number.Uint64() // big.Int → uint64
	fmt.Printf("[Watcher-%s] Connected. Current block: %d, polling every %v\n",
		w.chain, w.lastBlock, w.interval)

	// ============================================================
	// 主轮询循环
	// 每 interval 时间触发一次，拉取新区块中的事件
	// ============================================================
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for range ticker.C {
		w.pollBlocks() // 每次滴答就轮询一次新区块
	}
}

// ============================================================================
// pollBlocks — 轮询新区块并提取事件
// ============================================================================
func (w *EVMWatcher) pollBlocks() {
	// 创建带 30 秒超时的上下文
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 获取当前最新区块号
	currentHeader, err := w.client.HeaderByNumber(ctx, nil)
	if err != nil {
		fmt.Printf("[Watcher-%s] Failed to get current block: %v\n", w.chain, err)
		return
	}
	currentBlock := currentHeader.Number.Uint64()

	// 如果上次处理到的区块 >= 当前最新区块，说明没有新区块，直接返回
	if currentBlock <= w.lastBlock {
		return
	}

	// 计算要处理的区块范围
	fromBlock := w.lastBlock + 1
	toBlock := currentBlock

	// 【并发/性能设计：批量限制】
	// 如果服务刚启动或长时间断连，可能积压了大量区块。
	// 为防止一次性处理太多导致 OOM 或打爆 RPC，每轮最多处理 50 个区块。
	if toBlock-fromBlock > 50 {
		toBlock = fromBlock + 49
	}

	// 拉取事件日志
	events, err := w.fetchEvents(ctx, fromBlock, toBlock)
	if err != nil {
		fmt.Printf("[Watcher-%s] Failed to fetch events for blocks %d-%d: %v\n",
			w.chain, fromBlock, toBlock, err)
		return
	}

	// 逐条事件发布到 RabbitMQ
	for _, ev := range events {
		body, err := ev.toJSON()
		if err != nil {
			continue
		}

		err = w.ch.Publish(
			"indexer.tx",
			ev.routingKey(), // 如 "token.transfer"
			false, false,
			amqp.Publishing{
				ContentType: "application/json",
				Body:        body,
				Timestamp:   time.Now(),
			},
		)
		if err != nil {
			fmt.Printf("[Watcher-%s] Publish error: %v\n", w.chain, err)
			continue
		}

		// 如果涉及监控地址，加特殊标记方便查看
		marker := ""
		if w.isWatched(ev.FromAddr) || w.isWatched(ev.ToAddr) {
			marker = " *** WATCHED ***"
		}

		fmt.Printf("[Watcher-%s] %s.%s tx=%s block=%d from=%s to=%s%s\n",
			w.chain, ev.Category, ev.EventType, ev.TxHash, ev.BlockNumber,
			shortAddr(ev.FromAddr), shortAddr(ev.ToAddr), marker)
	}

	fmt.Printf("[Watcher-%s] Processed blocks %d-%d: %d events\n",
		w.chain, fromBlock, toBlock, len(events))

	w.lastBlock = toBlock // 更新处理进度
}

// ============================================================================
// fetchEvents — 用 eth_getLogs 拉取事件日志（逐区块查询）
// ============================================================================
// 为什么逐区块查询而不是批量查询？
// publicnode.com 等公共 RPC 对 eth_getLogs 有限制：
//   多区块范围的查询会被拒绝（返回 -32701 错误）
//   单区块查询不受限制
// 由于 ETH 每 12 秒才一个新区块，逐区块查询的额外开销可忽略不计。
func (w *EVMWatcher) fetchEvents(ctx context.Context, fromBlock, toBlock uint64) ([]ChainEvent, error) {
	var events []ChainEvent // 未初始化时为 nil，append 会自动处理

	for bn := fromBlock; bn <= toBlock; bn++ {
		// 构造查询条件：只查这一个区块，过滤 Transfer/Approval 类型的事件
		query := ethereum.FilterQuery{
			FromBlock: new(big.Int).SetUint64(bn), // 起始区块（大整数）
			ToBlock:   new(big.Int).SetUint64(bn), // 结束区块（同一区块）
			Topics: [][]common.Hash{ // 事件主题过滤器
				{
					transferTopic,       // ERC-20/ERC-721 Transfer
					transferSingleTopic, // ERC-1155 TransferSingle
					transferBatchTopic,  // ERC-1155 TransferBatch
					approvalTopic,       // Approval
				},
			},
		}

		// FilterLogs 调用 eth_getLogs RPC 方法
		logs, err := w.client.FilterLogs(ctx, query)
		if err != nil {
			fmt.Printf("[Watcher-%s] Failed to fetch logs for block %d: %v\n", w.chain, bn, err)
			continue // 跳过这个区块，继续下一个
		}

		// 解析每条日志
		for _, vLog := range logs {
			ev := w.parseLog(vLog)
			if ev != nil {
				events = append(events, *ev) // append 将元素追加到切片末尾
			}
		}
	}
	return events, nil
}

// ============================================================================
// parseLog — 将 EVM 事件日志解析为 ChainEvent
// ============================================================================
// 参数 vLog 是 go-ethereum 库的 types.Log 类型，包含：
//   Address     — 发射事件的合约地址
//   Topics      — 事件主题数组（每个 32 字节）
//   Data        — 非 indexed 参数的 ABI 编码数据
//   BlockNumber — 区块号
//   TxHash      — 交易哈希
//   Index       — 日志在交易中的序号
func (w *EVMWatcher) parseLog(vLog types.Log) *ChainEvent {
	topic0 := vLog.Topics[0] // 第一个 topic 永远是事件签名哈希
	blockTime := time.Now().Unix()

	// 根据事件签名分发到不同的解析函数
	switch topic0 {
	case transferTopic:
		// Transfer 事件：可能是 ERC-20（3 topic）或 ERC-721（4 topic）
		return w.parseTransfer(vLog, len(vLog.Topics), blockTime)

	case transferSingleTopic:
		// ERC-1155 Transfer 单个
		return w.parseERC1155Single(vLog, blockTime)

	case transferBatchTopic:
		// ERC-1155 Transfer 批量
		return w.parseERC1155Batch(vLog, blockTime)

	case approvalTopic:
		// Approval 授权事件
		return w.parseApproval(vLog, blockTime)
	}

	return nil // 不认识的事件类型，忽略
}

// ============================================================================
// parseTransfer — 解析 Transfer 事件（ERC-20 或 ERC-721）
// ============================================================================
func (w *EVMWatcher) parseTransfer(vLog types.Log, numTopics int, blockTime int64) *ChainEvent {
	ev := &ChainEvent{
		Chain:        w.chain,
		BlockNumber:  vLog.BlockNumber,
		BlockHash:    vLog.BlockHash.Hex(), // .Hex() 将 Hash 类型转为 0x... 字符串
		TxHash:       vLog.TxHash.Hex(),
		LogIndex:     int(vLog.Index),
		ContractAddr: vLog.Address.Hex(),
		Timestamp:    blockTime,
		EventType:    "Transfer",
	}

	if numTopics == 4 {
		// ============================================================
		// 4 个 topic → ERC-721 NFT 转账
		// Transfer(address indexed from, address indexed to, uint256 indexed tokenId)
		// topics[1] = from, topics[2] = to, topics[3] = tokenId
		// ============================================================
		ev.Category = "nft"
		ev.FromAddr = common.HexToAddress(vLog.Topics[1].Hex()).Hex()
		ev.ToAddr = common.HexToAddress(vLog.Topics[2].Hex()).Hex()
		// Topics[3] 是 tokenId（作为 256 位整数存储的），.Big() 转为 *big.Int
		ev.TokenID = vLog.Topics[3].Big().String()
		ev.Amount = "1" // NFT 每次转移数量总是 1
		ev.Decimals = 0 // NFT 没有小数位

		// from = 零地址 → 铸造（Mint），凭空产生新 NFT
		if vLog.Topics[1] == zeroTopic {
			ev.EventType = "Mint"
		} else if vLog.Topics[2] == zeroTopic {
			// to = 零地址 → 销毁（Burn），把 NFT 送入黑洞
			ev.EventType = "Burn"
		}
	} else {
		// ============================================================
		// 3 个 topic → ERC-20 代币转账
		// Transfer(address indexed from, address indexed to, uint256 value)
		// topics[1] = from, topics[2] = to
		// data = value（uint256，32 字节）
		// ============================================================
		ev.Category = "token"
		ev.FromAddr = common.HexToAddress(vLog.Topics[1].Hex()).Hex()
		ev.ToAddr = common.HexToAddress(vLog.Topics[2].Hex()).Hex()
		ev.TokenID = ""
		ev.Symbol, ev.Decimals = w.lookupToken(ev.ContractAddr)

		// Data 字段包含 amount 值（ABI 编码的 uint256 = 32 字节）
		if len(vLog.Data) > 0 {
			// 【区块链概念：big.Int 大整数】
			// 区块链上的数值经常远超 64 位整数范围（比如 1 ETH = 10^18 wei），
			// 所以 Go 的 int64/uint64 存不下，必须用 math/big 的大整数。
			rawAmount := new(big.Int).SetBytes(vLog.Data).String()
			ev.Amount = NormalizeAmount(rawAmount, ev.Decimals)
		} else {
			ev.Amount = "0"
		}

		// 同样检测铸造和销毁
		if vLog.Topics[1] == zeroTopic {
			ev.EventType = "Mint"
		} else if vLog.Topics[2] == zeroTopic {
			ev.EventType = "Burn"
		}
	}

	ev.RawData = ev.toJSONString()                    // 保存原始数据
	// Symbol 已在上面通过 lookupToken 设置
	return ev
}

// parseERC1155Single 解析 ERC-1155 TransferSingle 事件。
// ERC-1155 是"多代币标准"：一个合约可以管理多种代币（包括同质化和非同质化）。
// TransferSingle 一次转移一种代币。
func (w *EVMWatcher) parseERC1155Single(vLog types.Log, blockTime int64) *ChainEvent {
	ev := &ChainEvent{
		Chain:        w.chain,
		BlockNumber:  vLog.BlockNumber,
		BlockHash:    vLog.BlockHash.Hex(),
		TxHash:       vLog.TxHash.Hex(),
		LogIndex:     int(vLog.Index),
		ContractAddr: vLog.Address.Hex(),
		Timestamp:    blockTime,
		Category:     "nft",     // 1155 常用于 NFT
		EventType:    "Transfer",
		Decimals:     0,
	}

	// TransferSingle 的 topic 结构：topic0=签名, topic1=operator, topic2=from, topic3=to
	// data = id (uint256) + value (uint256)，各 32 字节，共 64 字节
	if numTopics := len(vLog.Topics); numTopics >= 4 {
		ev.FromAddr = common.HexToAddress(vLog.Topics[2].Hex()).Hex()
		ev.ToAddr = common.HexToAddress(vLog.Topics[3].Hex()).Hex()
	}

	if len(vLog.Data) >= 64 {
		// Data 的前 32 字节是 id，后 32 字节是 value
		ev.TokenID = new(big.Int).SetBytes(vLog.Data[0:32]).String()
		ev.Amount = new(big.Int).SetBytes(vLog.Data[32:64]).String()
	}

	if vLog.Topics[2] == zeroTopic {
		ev.EventType = "Mint"
	} else if vLog.Topics[3] == zeroTopic {
		ev.EventType = "Burn"
	}

	ev.RawData = ev.toJSONString()
	return ev
}

// parseERC1155Batch 解析 ERC-1155 TransferBatch 事件（简化版）。
// TransferBatch 一次转移多种代币，data 中包含 id[] 和 value[] 数组。
// 为简化处理，这里只记录一条事件（标记 tokenID="batch"）。
func (w *EVMWatcher) parseERC1155Batch(vLog types.Log, blockTime int64) *ChainEvent {
	ev := &ChainEvent{
		Chain:        w.chain,
		BlockNumber:  vLog.BlockNumber,
		BlockHash:    vLog.BlockHash.Hex(),
		TxHash:       vLog.TxHash.Hex(),
		LogIndex:     int(vLog.Index),
		ContractAddr: vLog.Address.Hex(),
		Timestamp:    blockTime,
		Category:     "nft",
		EventType:    "Transfer",
		Decimals:     0,
		TokenID:      "batch", // 标记为批量操作
		Amount:       "1",
	}

	if len(vLog.Topics) >= 4 {
		ev.FromAddr = common.HexToAddress(vLog.Topics[2].Hex()).Hex()
		ev.ToAddr = common.HexToAddress(vLog.Topics[3].Hex()).Hex()
	}

	ev.RawData = ev.toJSONString()
	return ev
}

// parseApproval 解析 ERC-20 Approval（授权）事件。
// Approval(address indexed owner, address indexed spender, uint256 value)
// 表示 owner 授权 spender 可以从自己账户转走最多 value 数量的代币。
func (w *EVMWatcher) parseApproval(vLog types.Log, blockTime int64) *ChainEvent {
	ev := &ChainEvent{
		Chain:        w.chain,
		BlockNumber:  vLog.BlockNumber,
		BlockHash:    vLog.BlockHash.Hex(),
		TxHash:       vLog.TxHash.Hex(),
		LogIndex:     int(vLog.Index),
		ContractAddr: vLog.Address.Hex(),
		Timestamp:    blockTime,
		Category:     "token",
		EventType:    "Approval",
	}
	ev.Symbol, ev.Decimals = w.lookupToken(ev.ContractAddr)

	if len(vLog.Topics) >= 3 {
		ev.FromAddr = common.HexToAddress(vLog.Topics[1].Hex()).Hex() // owner
		ev.ToAddr = common.HexToAddress(vLog.Topics[2].Hex()).Hex()   // spender
	}

	if len(vLog.Data) > 0 {
		rawAmount := new(big.Int).SetBytes(vLog.Data).String(); ev.Amount = NormalizeAmount(rawAmount, ev.Decimals) // value
	}

	ev.RawData = ev.toJSONString()
	return ev
}

// guessSymbol 根据合约地址返回已知的代币符号。
// 这是一个硬编码的"知名代币列表"，覆盖以太坊和 BSC 主网上最常见的代币。
// 生产环境应该从链上查询合约的 symbol() 方法，而不是用硬编码列表。
func (w *EVMWatcher) lookupToken(addr string) (symbol string, decimals uint8) {
	lower := strings.ToLower(addr)
	known := map[string]struct {
		sym string
		dec uint8
	}{
		// --- 以太坊主网 ---
		"0xdac17f958d2ee523a2206206994597c13d831ec7": {"USDT", 6},   // Tether USD
		"0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": {"USDC", 6},   // USD Coin
		"0x6b175474e89094c44da98b954eedeac495271d0f": {"DAI", 18},   // Dai Stablecoin
		"0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": {"WETH", 18},  // Wrapped Ether
		"0x2260fac5e5542a773aa44fbcfedf7c193bc2c599": {"WBTC", 8},   // Wrapped Bitcoin
		"0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce": {"SHIB", 18},  // Shiba Inu
		"0x7d1afa7b718fb893db30a3abc0cfc608aacfebb0": {"MATIC", 18}, // Polygon
		// --- BSC 主网 ---
		"0x55d398326f99059ff775485246999027b3197955": {"USDT", 18},  // Binance-Peg USDT
		"0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d": {"USDC", 18},  // Binance-Peg USDC
		"0xe9e7cea3dedca5984780bafc599bd69add087d56": {"BUSD", 18},  // Binance USD
		"0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c": {"WBNB", 18},  // Wrapped BNB
	}
	if t, ok := known[lower]; ok {
		return t.sym, t.dec
	}
	return "", 18 // 不认识的合约默认 18 位精度
}

// isWatched 检查地址是否在监控列表中。
func (w *EVMWatcher) isWatched(addr string) bool {
	return w.watchAddrs[common.HexToAddress(strings.ToLower(addr))]
}

// shortAddr 将长地址缩短为"前6位...后4位"的格式，方便日志查看。
// 例如：0x2afd3dbabfa1ea4554697a7f2be41e2ee81eeb23 → 0x2afd...eb23
func shortAddr(addr string) string {
	if len(addr) >= 12 {
		return addr[:6] + "..." + addr[len(addr)-4:]
	}
	return addr
}
