package mdb

// Chain represents an enabled blockchain network.
// Scanner tasks read this table on each polling tick to decide whether to
// process the network. When Enabled=false the scanner skips the chain
// entirely without restarting.
type Chain struct {
	Network          string `gorm:"column:network;uniqueIndex:chains_network_uindex;size:32" json:"network" example:"tron"`
	DisplayName      string `gorm:"column:display_name;size:64" json:"display_name" example:"Tron"`
	Enabled          bool   `gorm:"column:enabled;default:true" json:"enabled" example:"true"`
	MinConfirmations int    `gorm:"column:min_confirmations;default:1" json:"min_confirmations" example:"20"`
	ScanIntervalSec  int    `gorm:"column:scan_interval_sec;default:5" json:"scan_interval_sec" example:"5"`
	Extra            string `gorm:"column:extra;type:text" json:"extra" example:"{}"`
	BaseModel
}

func (c *Chain) TableName() string {
	return "chains"
}
