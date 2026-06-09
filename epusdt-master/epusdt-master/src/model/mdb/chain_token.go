package mdb

const (
	ChainTokenStatusEnable  = 1
	ChainTokenStatusDisable = 2
)

// ChainToken describes a token to watch on a given chain. Replaces the
// hardcoded USDT/USDC contract addresses in task/listen_eth.go. Scanners
// load this table on startup (and refresh periodically) to learn which
// contracts to subscribe to.
type ChainToken struct {
	Network         string  `gorm:"column:network;size:32;uniqueIndex:chain_tokens_network_symbol_uindex,priority:1" json:"network" example:"tron"`
	Symbol          string  `gorm:"column:symbol;size:32;uniqueIndex:chain_tokens_network_symbol_uindex,priority:2" json:"symbol" example:"USDT"`
	ContractAddress string  `gorm:"column:contract_address;size:128" json:"contract_address" example:"TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"`
	Decimals        int     `gorm:"column:decimals;default:6" json:"decimals" example:"6"`
	Enabled         bool    `gorm:"column:enabled;default:true" json:"enabled" example:"true"`
	MinAmount       float64 `gorm:"column:min_amount;default:0" json:"min_amount" example:"1.0"`
	BaseModel
}

func (c *ChainToken) TableName() string {
	return "chain_tokens"
}
