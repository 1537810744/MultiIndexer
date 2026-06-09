package mdb

import "time"

type TransactionLock struct {
	ID              uint64    `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	Network         string    `gorm:"column:network;uniqueIndex:transaction_lock_network_address_token_amount_uindex,priority:1" json:"network"`
	Address         string    `gorm:"column:address;uniqueIndex:transaction_lock_network_address_token_amount_uindex,priority:2" json:"address"`
	Token           string    `gorm:"column:token;uniqueIndex:transaction_lock_network_address_token_amount_uindex,priority:3" json:"token"`
	AmountScaled    int64     `gorm:"column:amount_scaled;uniqueIndex:transaction_lock_network_address_token_amount_uindex,priority:4" json:"amount_scaled"`
	AmountText      string    `gorm:"column:amount_text" json:"amount_text"`
	AmountPrecision int       `gorm:"column:amount_precision;default:2;uniqueIndex:transaction_lock_network_address_token_amount_uindex,priority:5" json:"amount_precision"`
	TradeId         string    `gorm:"column:trade_id;index:transaction_lock_trade_id_index" json:"trade_id"`
	ExpiresAt       time.Time `gorm:"column:expires_at;index:transaction_lock_expires_at_index" json:"expires_at"`
	CreatedAt       time.Time `gorm:"column:created_at" json:"created_at"`
	UpdatedAt       time.Time `gorm:"column:updated_at" json:"updated_at"`
}

func (t *TransactionLock) TableName() string {
	return "transaction_lock"
}
