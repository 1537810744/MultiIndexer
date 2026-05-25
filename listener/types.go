// ============================================================================
// types.go — 整个系统的核心数据结构定义
// ============================================================================
// 这个文件定义了 ChainEvent（链上事件）结构体，它是最核心的数据类型。
// 监听器（Listener）从区块链上解析出 ChainEvent，序列化为 JSON，
// 通过 RabbitMQ 发送给处理器（Processor），最终写入 MySQL 和 Redis。
// ============================================================================

package main // package main 表示这是一个可执行程序（而不是可被其他项目引用的库）

// encoding/json 是 Go 标准库中处理 JSON 的包。
// 它能将 Go 结构体（struct）序列化为 JSON 字符串（Marshal），
// 也能将 JSON 字符串反序列化为 Go 结构体（Unmarshal）。
import (
	"encoding/json"
	"math/big"
)

// ChainEvent 代表一条从区块链上解析出的事件。
//
// 【Go 语法：struct（结构体）】
// struct 是把不同类型的数据打包在一起的复合类型，类似于其他语言里的 class/object。
// Go 没有"类"的概念，用 struct + 方法（method）来实现面向对象。
//
// 【Go 语法：struct tag（结构体标签）】
// 每个字段后面 `json:"xxx"` 是"结构体标签"（struct tag），
// 告诉 encoding/json 包：序列化时这个字段在 JSON 里的 key 应该叫 xxx。
// 例如 Chain 字段在 JSON 里会变成 "chain"。
//
// 【Go 语法：字段类型】
// string  = 字符串，Go 的字符串是不可变的 UTF-8 字节序列
// uint64  = 无符号64位整数，范围 0 ~ 18446744073709551615，适合存区块高度（不会为负）
// int     = 有符号整数，在64位系统上是64位，适合存日志索引
// int64   = 有符号64位整数，适合存 Unix 时间戳（从1970年1月1日至今的秒数）
// uint8   = 无符号8位整数，范围 0~255，适合存代币精度（小数位数）
type ChainEvent struct {
	// --- 基础标识 ---
	Chain       string `json:"chain"`        // 链标识：eth / bsc / sol
	BlockNumber uint64 `json:"block_number"` // 区块编号（Solana 叫 Slot）
	BlockHash   string `json:"block_hash"`   // 区块哈希值，区块的唯一指纹
	TxHash      string `json:"tx_hash"`      // 交易哈希，交易的唯一指纹（32字节的十六进制字符串）
	LogIndex    int    `json:"log_index"`    // 事件日志在交易中的序号（一笔交易可能产生多条事件日志）

	// --- 事件分类 ---
	Category  string `json:"category"`   // 资产类别：token（代币） / nft（非同质化代币）
	EventType string `json:"event_type"` // 事件类型：Transfer（转账）/ Mint（铸造）/ Burn（销毁）/ Approval（授权）

	// --- 涉及地址 ---
	ContractAddr string `json:"contract_address"` // 智能合约的地址（ERC-20/ERC-721 代币的合约地址）
	FromAddr     string `json:"from_address"`     // 发送方地址
	ToAddr       string `json:"to_address"`       // 接收方地址

	// --- 金额与代币信息 ---
	TokenID  string `json:"token_id"` // NFT 的 Token ID（ERC-721 里每个 NFT 的唯一编号）
	Amount   string `json:"amount"`   // 转账金额（用 string 存大数，防止数值溢出）
	Symbol   string `json:"symbol"`   // 代币符号（如 "USDT"、"WETH"），如果认识这个合约的话
	Decimals uint8  `json:"decimals"` // 代币精度（小数位数），ERC-20 通常是 18，USDT 是 6

	// --- 其他 ---
	RawData   string `json:"raw_data"` // 原始事件数据的 JSON 字符串（完整保留，便于调试）
	Timestamp int64  `json:"timestamp"` // 事件发生的时间（区块的 Unix 时间戳，单位：秒）
}

// ============================================================================
// 方法（Methods）
// ============================================================================

// routingKey 根据事件类型和分类生成 RabbitMQ 的 routing key。
//
// 【Go 语法：方法（Method）】
// func (e *ChainEvent) routingKey() string
//       ^^^^^^^^^^^^^^^^ 这叫"接收者（receiver）"，把函数绑定到 ChainEvent 类型上。
//       *ChainEvent 是指针接收者，可以避免复制整个结构体（高效），
//       也能在方法内部修改结构体的字段。
// Go 没有 class，但通过 struct + method 可以实现面向对象编程。
//
// 【RabbitMQ 概念：routing key】
// routing key 是消息的路由标签。RabbitMQ 的 Topic Exchange 根据它决定
// 消息投递到哪个队列。格式："category.event_type"（如 "token.transfer"、 "nft.mint"）
func (e *ChainEvent) routingKey() string {
	et := e.EventType
	// 【Go 语法：switch】
	// switch 是多分支选择语句，不需要 break（Go 的 case 默认自带 break，不会穿透）。
	switch et {
	case "Transfer":
		et = "transfer"
	case "Mint":
		et = "mint"
	case "Burn":
		et = "burn"
	case "Approval":
		et = "approval"
	default:
		et = "unknown"
	}
	return e.Category + "." + et // 例如：token.transfer、nft.mint
}

// toJSON 将 ChainEvent 序列化为 JSON 字节切片。
//
// 【Go 语法：多返回值】
// Go 的函数可以返回多个值。这里的签名是 func(...) ([]byte, error)
// 第一个返回值是成功时的结果（JSON 字节），第二个是错误。
// 这种"把错误当作正常返回值"的模式是 Go 的核心哲学，而不是用 try-catch。
//
// 【Go 语法：error 接口】
// error 是 Go 内置的接口类型，只有一个方法 Error() string。
// 任何实现了 Error() 方法的类型都可以作为 error 返回。
func (e *ChainEvent) toJSON() ([]byte, error) {
	return json.Marshal(e) // json.Marshal 将 Go 结构体转为 JSON 字节切片
}

// toJSONString 将 ChainEvent 序列化为 JSON 字符串（用于存入 raw_data 字段）。
func (e *ChainEvent) toJSONString() string {
	b, _ := json.Marshal(e) // _ 是"空白标识符"，表示忽略这个返回值（这里忽略了 error）
	return string(b)        // string(b) 将 []byte 转为 string 类型
}

// ============================================================================
// NormalizeAmount — 将原始链上金额转换为人类可读的数值
// ============================================================================
// 区块链上所有金额都是原始单位的整数（最小不可分割单位）：
//   ETH/BSC: 1 ETH = 10^18 wei（原始值 1000000000000000000 = 1.0 ETH）
//   USDC:    1 USDC = 10^6 最小单位（原始值 1000000 = 1.0 USDC）
//   NFT:     精度为 0（amount=1 就是 1 个 NFT）
//
// json.RawMessage 是 []byte 的别名，用于存储未解析的 JSON 数据。
// RawData 字段保存事件的完整 JSON，方便后续调试和审计。
//
// 此函数将原始金额除以 10^decimals，得到人类可读的数值字符串。
// 使用 math/big 包保证精度不丢失。
//
// 示例：
//   NormalizeAmount("1500000", 6)  → "1.5"    (1.5 USDC)
//   NormalizeAmount("1000000000000000000", 18) → "1"      (1 ETH)
//   NormalizeAmount("3849019226689897600000", 18) → "3849.0192266898976" (3849 ETH)
func NormalizeAmount(rawAmount string, decimals uint8) string {
	if decimals == 0 || rawAmount == "" || rawAmount == "0" {
		return rawAmount
	}
	amount := new(big.Int)
	if _, ok := amount.SetString(rawAmount, 10); !ok {
		return rawAmount // 解析失败，返回原值
	}
	// divisor = 10^decimals
	divisor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)
	// 用 big.Rat 做精确除法
	rat := new(big.Rat).SetFrac(amount, divisor)
	return rat.FloatString(int(decimals))
}
