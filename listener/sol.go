// ============================================================================
// sol.go — Solana 区块链观察者（Solana Watcher）
// ============================================================================
// Solana 是一条高性能公链，使用"槽位（Slot）"代替"区块（Block）"的概念。
// 出块速度约 0.4 秒（400ms），是目前最快的 L1 公链之一。
//
// 和 EVM 链（ETH/BSC）的关键区别：
//   1. Solana 不使用 Solidity，也不生成 EVM 日志（Log/Topic）。
//      它使用 "parsed instruction"（解析后的指令）来描述链上操作。
//   2. Solana 的代币标准是 SPL Token（类似于 ERC-20）和 Metaplex NFT。
//      转账信息嵌入在交易的 instructions 数组中，JSON-RPC 可以直接解析。
//   3. Solana 没有"交易收据"的概念，所有信息都在区块数据里。
//   4. 本程序使用 "jsonParsed" 编码，让 RPC 节点帮我们解析好指令类型。
// ============================================================================

package main

import (
	"bytes"          // 字节缓冲区，用于构建 HTTP 请求体和比较空响应
	"encoding/json"  // JSON 序列化/反序列化
	"fmt"            // 格式化输出日志
	"io"             // 读取 HTTP 响应体
	"net/http"       // HTTP 客户端，Solana RPC 走 HTTP（不像 ETH 走 WebSocket）
	"time"           // 定时器和超时控制

	amqp "github.com/rabbitmq/amqp091-go" // RabbitMQ 客户端
)

// ============================================================================
// SolWatcher — Solana 链观察者结构体
// ============================================================================
// 【Go 语法：struct 字段】
// 小写字母开头的字段是"未导出"的（包外不可访问），这是 Go 的封装机制。
// 大写 = public，小写 = private（但限制在包级别，不是类级别）。
type SolWatcher struct {
	rpcURL    string          // Solana JSON-RPC 节点地址（如 https://solana-rpc.publicnode.com）
	interval  time.Duration   // 轮询间隔（建议 1 秒，因为 Solana 出块约 0.4 秒）
	ch        *amqp.Channel   // RabbitMQ 通道，用于发布事件消息
	lastSlot  uint64          // 上次处理到哪个 Slot，避免重复处理

	watchAddr string          // 要特别监控的钱包地址（告警用）

	httpClient *http.Client   // HTTP 客户端（带超时），复用连接
}

// ============================================================================
// Solana JSON-RPC 通信类型定义
// ============================================================================
// 【区块链概念：JSON-RPC】
// 区块链节点通过 JSON-RPC 协议对外提供服务。
// JSON-RPC 是一种无状态的、基于 JSON 的远程过程调用协议。
// 请求格式：{"jsonrpc":"2.0", "id":1, "method":"getBlock", "params":[...]}
// 响应格式：{"jsonrpc":"2.0", "id":1, "result":{...}} 或包含 error 字段。
//
// 【Go 语法：struct tag `json:"xxx"`】
// 这些标签告诉 encoding/json 包：序列化/反序列化时，JSON 字段名是什么。
// 例如 JSONRPC 字段在 JSON 中显示为 "jsonrpc"（小写）。

// solRequest 是 JSON-RPC 请求的通用结构体。
// Params 用 interface{} 因为不同方法的参数结构不同（有时是数组，有时是对象）。
type solRequest struct {
	JSONRPC string      `json:"jsonrpc"` // 协议版本，固定 "2.0"
	ID      int         `json:"id"`      // 请求编号（响应会带回来，用于匹配请求和响应）
	Method  string      `json:"method"`  // 调用的 RPC 方法名（如 "getBlock"、"getSlot"）
	Params  interface{} `json:"params"`  // 方法参数（可以是数组、对象等任意类型）
}

// solResponse 是 JSON-RPC 响应的通用结构体。
// Result 用 json.RawMessage（原始 JSON 字节），因为不同方法的返回值结构不同。
type solResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int             `json:"id"`
	Result  json.RawMessage `json:"result"`          // 【Go 语法】json.RawMessage 是 []byte 别名，延迟解析
	Error   *solRPCError    `json:"error,omitempty"` // omitempty：error 为空时不序列化这个字段
}

// solRPCError 是 JSON-RPC 错误对象。
type solRPCError struct {
	Code    int    `json:"code"`    // 错误码（如 -32000 表示服务端错误）
	Message string `json:"message"` // 错误描述
}

// ============================================================================
// Solana "jsonParsed" 编码下的区块结构
// ============================================================================
// 【区块链概念：jsonParsed 编码】
// Solana RPC 支持多种编码格式。jsonParsed 是最友好的一种：
// 节点会自动解析 instructions，告诉我们这是什么类型的操作（转账、质押等）。
// 不用自己解析二进制数据，直接拿到 transfer 的 source/destination/amount。

// solParsedBlock 是 jsonParsed 编码下的区块结构。
type solParsedBlock struct {
	Blockhash     string        `json:"blockhash"`     // 区块的哈希值（Solana 的"区块指纹"）
	ParentSlot    uint64        `json:"parentSlot"`    // 父槽位编号（指向前一个 Slot）
	BlockTime     *int64        `json:"blockTime"`     // 区块时间戳（Unix 秒），*int64 是指针，可能为 null
	Transactions  []solParsedTx `json:"transactions"`  // 交易列表
}

// solParsedTx 是 jsonParsed 编码下的交易结构。
type solParsedTx struct {
	Transaction solTxDetail `json:"transaction"` // 交易详情
	Meta        *solTxMeta  `json:"meta"`        // 交易元数据（包含执行状态），*指针可能为 null
}

// solTxDetail 是交易的核心信息。
type solTxDetail struct {
	Signatures []string      `json:"signatures"` // 交易签名（Solana 可以有多个签名者）
	Message    solTxMessage  `json:"message"`    // 交易消息体
}

// solTxMessage 包含交易中的指令列表。
type solTxMessage struct {
	Instructions []solParsedInstruction `json:"instructions"` // 指令列表
}

// solParsedInstruction 是单条解析后的指令。
// 【区块链概念：Solana Instruction（指令）】
// Solana 交易由一条或多条指令组成。每条指令指定了：
//   - ProgramID：要调用的程序（合约）地址
//   - Parsed：如果程序是已知的（如 SPL Token 程序），RPC 自动解析参数
//   - Accounts：指令涉及的账户列表
//   - Data：如果程序未知，这里存原始二进制数据（Base58 编码）
type solParsedInstruction struct {
	ProgramID string          `json:"programId"`         // 程序（合约）地址
	Parsed    json.RawMessage `json:"parsed,omitempty"`  // 已解析的指令数据（json.RawMessage = 延迟解析）
	Accounts  []string        `json:"accounts,omitempty"` // 涉及的账户地址列表
	Data      string          `json:"data,omitempty"`     // 原始指令数据（Base58）
}

// solParsedTransfer 是解析后的 SPL Token 转账指令。
// 【区块链概念：SPL Token 转账类型】
// "transfer" — 普通 SPL Token 转账（使用 amount，不需要 decimals）
// "transferChecked" — 带精度检查的转账（额外包含 token 地址，用于 NFT 等有 tokenId 的代币）
type solParsedTransfer struct {
	Type string                `json:"type"` // "transfer" 或 "transferChecked"
	Info solParsedTransferInfo `json:"info"` // 转账详细信息
}

// solParsedTransferInfo 是转账的具体数据。
type solParsedTransferInfo struct {
	Source      string `json:"source"`          // 发送方地址
	Destination string `json:"destination"`     // 接收方地址
	Authority   string `json:"authority,omitempty"` // 授权方（可选）
	Amount      string `json:"amount"`          // 转账金额（原始单位，如 1000000 表示 1 USDC）
	Token       string `json:"token,omitempty"` // 代币的 Mint 地址（仅 transferChecked 有）
}

// solTxMeta 是交易的执行元数据。
// Err 字段判断交易是否成功：nil 表示成功，非 nil 表示失败。
type solTxMeta struct {
	Err interface{} `json:"err"` // 错误信息，nil=成功
}

// ============================================================================
// NewSolWatcher — 构造函数
// ============================================================================
// 【Go 语法：构造函数模式】
// Go 没有构造函数关键字。约定俗成用 NewXxx() 函数来创建和初始化结构体。
// 返回 *SolWatcher 指针，避免复制整个结构体。
func NewSolWatcher(rpcURL string, interval time.Duration, ch *amqp.Channel, watchAddr string) *SolWatcher {
	return &SolWatcher{
		rpcURL:     rpcURL,
		interval:   interval,
		ch:         ch,
		watchAddr:  watchAddr,
		httpClient: &http.Client{Timeout: 30 * time.Second}, // 30秒超时，防止 RPC 无响应导致 goroutine 卡死
	}
}

// ============================================================================
// Run() — 主循环（在 goroutine 中运行）
// ============================================================================
// 【Go 并发核心：goroutine + defer + recover 三位一体】
// Run() 设计为在独立的 goroutine 中运行（在 main.go 里用 go w.Run() 启动）。
// defer + recover 确保任何 panic 都不会让整个程序崩溃，而是自动重启观察者。
func (w *SolWatcher) Run() {
	// 【Go 语法：defer + recover 故障恢复模式】
	// recover() 必须在 defer 函数中调用才有效。
	// 如果当前 goroutine 发生了 panic，recover() 会捕获它并返回 panic 的值。
	// 这样我们就可以打日志然后重启，而不是让进程崩溃。
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("[Watcher-sol] Panic recovered: %v, restarting in 5s...\n", r)
			time.Sleep(5 * time.Second) // 等 5 秒再重启，避免 RPC 持续故障时疯狂重试
			go w.Run()                  // 重启自己（新的 goroutine）
		}
	}()

	fmt.Printf("[Watcher-sol] Connecting to RPC: %s\n", w.rpcURL)

	// 获取当前最新的 Slot 作为起始点
	slot, err := w.getSlot()
	if err != nil {
		fmt.Printf("[Watcher-sol] Failed to get initial slot: %v\n", err)
		return
	}
	w.lastSlot = slot
	fmt.Printf("[Watcher-sol] Connected. Current slot: %d, polling every %v\n",
		w.lastSlot, w.interval)

	// 【Go 并发核心：time.NewTicker 定时器】
	// 每隔 interval 时间，ticker.C 通道就会收到一个时间值。
	// 用 for range 消费这个通道，实现定时轮询。
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop() // 函数退出时停止 ticker，防止 goroutine 泄漏

	for range ticker.C {
		w.pollSlots() // 每次滴答都拉取新 Slot
	}
}

// ============================================================================
// pollSlots() — 拉取并处理新的 Solana Slot
// ============================================================================
func (w *SolWatcher) pollSlots() {
	// 第一步：获取当前最新 Slot 编号
	currentSlot, err := w.getSlot()
	if err != nil {
		fmt.Printf("[Watcher-sol] Failed to get current slot: %v\n", err)
		return
	}

	// 【设计思路：Slot 延迟（Lag）】
	// 公共 RPC 节点在 Slot 刚产生时，区块数据可能还没完全可用。
	// 所以故意滞后 50 个 Slot（约 20 秒），确保区块数据已经写入节点。
	// 如果不滞后，会频繁遇到 "Block not available" 错误。
	const slotLag uint64 = 50
	if currentSlot > slotLag {
		currentSlot -= slotLag // 减去延迟，查询已确认的老 Slot
	}

	// 没有新 Slot，直接返回
	if currentSlot <= w.lastSlot {
		return
	}

	// 【设计思路：批量限制】
	// 如果程序停了一段时间，积压了几千个 Slot，一次性全部拉取会：
	//   1. 打爆 RPC 节点（被限流）
	//   2. 占用大量内存
	// 所以每次最多处理 10 个 Slot。
	fromSlot := w.lastSlot + 1
	toSlot := currentSlot
	if toSlot-fromSlot > 10 {
		toSlot = fromSlot + 9 // 每次最多 10 个
	}

	// 获取这些 Slot 中的事件
	events, err := w.fetchBlockEvents(fromSlot, toSlot)
	if err != nil {
		fmt.Printf("[Watcher-sol] Failed to fetch block events for slots %d-%d: %v\n",
			fromSlot, toSlot, err)
		return
	}

	// 逐条发布到 RabbitMQ（和 EVM 观察者使用完全相同的消息格式）
	for _, ev := range events {
		body, err := ev.toJSON()
		if err != nil {
			continue
		}

		err = w.ch.Publish(
			"indexer.tx",       // 发往同一个 Exchange
			ev.routingKey(),    // routing key 如 "token.transfer"、"nft.mint"
			false, false,
			amqp.Publishing{
				ContentType: "application/json",
				Body:        body,
				Timestamp:   time.Now(),
			},
		)
		if err != nil {
			fmt.Printf("[Watcher-sol] Publish error: %v\n", err)
			continue
		}

		// 如果涉及监控地址，打标记
		marker := ""
		if ev.FromAddr == w.watchAddr || ev.ToAddr == w.watchAddr {
			marker = " *** WATCHED ***"
		}

		fmt.Printf("[Watcher-sol] %s.%s tx=%s slot=%d from=%s to=%s%s\n",
			ev.Category, ev.EventType, shortAddr(ev.TxHash),
			ev.BlockNumber, shortAddr(ev.FromAddr), shortAddr(ev.ToAddr), marker)
	}

	if len(events) > 0 {
		fmt.Printf("[Watcher-sol] Processed slots %d-%d: %d events\n",
			fromSlot, toSlot, len(events))
	}

	w.lastSlot = toSlot // 更新进度
}

// ============================================================================
// fetchBlockEvents() — 获取区块中的事件
// ============================================================================
// 【设计思路：逐个 Slot 查询】
// Solana RPC 不支持像 ETH 那样的范围查询，只能逐个 Slot 获取区块数据。
// 这对于 Solana 来说还好，因为每个 Slot 间隔只有 400ms，slotLag=50 也就 20 秒的数据。
func (w *SolWatcher) fetchBlockEvents(fromSlot, toSlot uint64) ([]ChainEvent, error) {
	var allEvents []ChainEvent

	// 逐个 Slot 获取区块并提取事件
	for slot := fromSlot; slot <= toSlot; slot++ {
		block, err := w.getBlock(slot)
		if err != nil {
			fmt.Printf("[Watcher-sol] Failed to get block %d: %v\n", slot, err)
			continue // 单个 Slot 失败不影响其他
		}
		if block == nil {
			continue // Slot 可能为空（没有交易）
		}

		// 遍历区块中的所有交易
		for txIdx, tx := range block.Transactions {
			// 跳过失败的交易（err 不为 nil）
			if tx.Meta != nil && tx.Meta.Err != nil {
				continue
			}

			// 获取交易哈希（Solana 交易有签名列表，取第一个作为 txHash）
			txHash := ""
			if len(tx.Transaction.Signatures) > 0 {
				txHash = tx.Transaction.Signatures[0]
			}

			// 遍历交易中的每条指令
			for instIdx, inst := range tx.Transaction.Message.Instructions {
				ev := w.parseInstruction(inst, slot, block.Blockhash, txHash, txIdx, instIdx, block.BlockTime)
				if ev != nil {
					allEvents = append(allEvents, *ev)
				}
			}
		}
	}
	return allEvents, nil
}

// ============================================================================
// parseInstruction() — 将 Solana 指令转换为 ChainEvent
// ============================================================================
// 【区块链概念：Solana 的 Token 识别】
// Solana 不像 EVM 那样用标准事件签名。SPL Token 转账是通过调用
// Token Program（地址：TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA）
// 的 transfer 指令实现的。jsonParsed 编码下，节点会帮我们解析出：
//   - type: "transfer" 或 "transferChecked"
//   - info.source: 发送方
//   - info.destination: 接收方
//   - info.amount: 金额
//
// 【区块链概念：NFT 识别启发式算法】
// Solana 上没有原生的 NFT 事件（不像 ERC-721 有 Transfer 事件 log）。
// 我们使用启发式规则：transferChecked + amount=1 很可能就是 NFT 转账。
// 因为 Metaplex NFT（Solana 最主流的 NFT 标准）用 SPL Token 表示，
// 每个 NFT 的 supply 为 1，所以转账金额永远是 1。
func (w *SolWatcher) parseInstruction(
	inst solParsedInstruction,
	slot uint64, blockhash, txHash string,
	txIdx, instIdx int,
	blockTime *int64,
) *ChainEvent {
	// 如果 Parsed 字段为空，说明 RPC 无法解析这条指令，跳过
	if inst.Parsed == nil {
		return nil
	}

	// 尝试解析为 SPL Token 转账指令
	var transfer solParsedTransfer
	if err := json.Unmarshal(inst.Parsed, &transfer); err != nil {
		return nil // 解析失败，不是 transfer 指令
	}

	// 只关心 transfer 和 transferChecked 类型
	if transfer.Type != "transfer" && transfer.Type != "transferChecked" {
		return nil
	}

	// 金额为空或为 0，不关心（可能是失败的转账或代币初始化）
	info := transfer.Info
	if info.Amount == "" || info.Amount == "0" {
		return nil
	}

	// 时间戳：优先用区块时间，没有就用当前时间
	ts := time.Now().Unix()
	if blockTime != nil {
		ts = *blockTime
	}

	eventType := "Transfer"
	fromAddr := info.Source
	toAddr := info.Destination

	// 默认归类为 Token 转账
	category := "token"
	decimals := uint8(9) // Solana SPL Token 常用 9 位精度（ETH 是 18 位）
	tokenID := ""

	// NFT 启发式检测：transferChecked + amount=1 → 很可能是 NFT
	if transfer.Type == "transferChecked" && info.Amount == "1" {
		category = "nft"
		decimals = 0
		tokenID = info.Token // Token 字段存的是代币的 Mint 地址（每个 NFT 都是独立的 Mint）
		eventType = "Transfer"
	}

	// 组装 ChainEvent（使用和 EVM 观察者完全相同的结构体）
	ev := &ChainEvent{
		Chain:        "sol",
		BlockNumber:  slot,                      // Solana 的 Slot 编号填充到 BlockNumber
		BlockHash:    blockhash,
		TxHash:       txHash,
		LogIndex:     txIdx*100 + instIdx,       // Solana 没有 LogIndex 概念，用 txIdx*100+instIdx 模拟唯一序号
		Category:     category,
		EventType:    eventType,
		ContractAddr: inst.ProgramID,            // ProgramID 就是被调用的合约地址
		FromAddr:     fromAddr,
		ToAddr:       toAddr,
		TokenID:      tokenID,
		Amount:       NormalizeAmount(info.Amount, decimals),
		Symbol:       "",                        // Solana 代币暂不识别 symbol
		Decimals:     decimals,
		Timestamp:    ts,
	}

	ev.RawData = ev.toJSONString() // 保留原始 JSON 方便调试
	return ev
}

// ============================================================================
// getSlot() — 获取当前 Solana 最新已确认的 Slot 编号
// ============================================================================
// 【Solana 概念：Commitment（确认级别）】
// "finalized" 是最高确认级别，意味着 Slot 已经被超过 2/3 的验证者确认。
// 其他选项："processed"（刚收到）、"confirmed"（多数确认）。
// 用 finalized 可以避免处理后来被回滚的 Slot（虽然 Solana 很少回滚）。
func (w *SolWatcher) getSlot() (uint64, error) {
	// 构建 JSON-RPC 请求
	req := solRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "getSlot",
		Params:  []interface{}{map[string]string{"commitment": "finalized"}},
	}

	var resp solResponse
	if err := w.rpcCall(req, &resp); err != nil {
		return 0, err
	}
	if resp.Error != nil {
		return 0, fmt.Errorf("RPC error: %s", resp.Error.Message)
	}

	// getSlot 返回的是一个数字，直接反序列化为 uint64
	var slot uint64
	if err := json.Unmarshal(resp.Result, &slot); err != nil {
		return 0, fmt.Errorf("unmarshal slot: %v", err)
	}
	return slot, nil
}

// ============================================================================
// getBlock() — 获取指定 Slot 的区块数据（jsonParsed 编码）
// ============================================================================
func (w *SolWatcher) getBlock(slot uint64) (*solParsedBlock, error) {
	params := []interface{}{
		slot,
		map[string]interface{}{
			"encoding":                       "jsonParsed", // 让 RPC 帮我们解析指令
			"maxSupportedTransactionVersion": 0,            // 只支持版本 0 的交易（最通用）
			"transactionDetails":             "full",       // 返回完整交易详情
			"rewards":                        false,        // 不返回质押奖励（不需要，省带宽）
		},
	}

	req := solRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "getBlock",
		Params:  params,
	}

	var resp solResponse
	if err := w.rpcCall(req, &resp); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return nil, fmt.Errorf("RPC error: %s", resp.Error.Message)
	}

	// 空 Slot 返回 null，不是错误，返回 nil
	if bytes.Equal(resp.Result, []byte("null")) {
		return nil, nil
	}

	var block solParsedBlock
	if err := json.Unmarshal(resp.Result, &block); err != nil {
		return nil, fmt.Errorf("unmarshal block: %v", err)
	}
	return &block, nil
}

// ============================================================================
// rpcCall() — 通用的 JSON-RPC 调用函数
// ============================================================================
// 【Go 语法：HTTP Client 的使用】
// http.NewRequest 创建请求 → client.Do 发送 → io.ReadAll 读取响应体
// 注意：响应体必须关闭（defer resp.Body.Close()），否则连接无法复用，导致 fd 泄漏。
func (w *SolWatcher) rpcCall(req solRequest, resp *solResponse) error {
	body, err := json.Marshal(req) // Go 结构体 → JSON 字节
	if err != nil {
		return err
	}

	httpReq, err := http.NewRequest("POST", w.rpcURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json") // 告诉服务器：我发的是 JSON

	httpResp, err := w.httpClient.Do(httpReq) // 发送 HTTP 请求
	if err != nil {
		return err
	}
	defer httpResp.Body.Close() // 【重要】确保响应体关闭，否则连接泄漏

	respBytes, err := io.ReadAll(httpResp.Body) // 读取全部响应体
	if err != nil {
		return err
	}

	// HTTP 非 200 时返回错误，截取前 200 字节响应体方便排查
	if httpResp.StatusCode != 200 {
		return fmt.Errorf("HTTP %d: %s", httpResp.StatusCode, string(respBytes[:min(len(respBytes), 200)]))
	}

	// JSON 字节 → Go 结构体
	return json.Unmarshal(respBytes, resp)
}
