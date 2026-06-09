package data

import (
	"github.com/GMWalletApp/epusdt/model/dao"
	"github.com/GMWalletApp/epusdt/model/mdb"
	"gorm.io/gorm"
)

func GetProviderOrderByTradeIDAndProvider(tradeID string, provider string) (*mdb.ProviderOrder, error) {
	row := new(mdb.ProviderOrder)
	err := dao.Mdb.Model(row).
		Where("trade_id = ?", tradeID).
		Where("provider = ?", provider).
		Limit(1).
		Find(row).Error
	return row, err
}

func CreateProviderOrderWithTransaction(tx *gorm.DB, row *mdb.ProviderOrder) error {
	return tx.Model(row).Create(row).Error
}

func UpdateProviderOrderCreated(tradeID string, provider string, providerOrderID string, payURL string) error {
	return dao.Mdb.Model(&mdb.ProviderOrder{}).
		Where("trade_id = ?", tradeID).
		Where("provider = ?", provider).
		Updates(map[string]interface{}{
			"provider_order_id": providerOrderID,
			"pay_url":           payURL,
			"status":            mdb.ProviderOrderStatusPending,
		}).Error
}

func MarkProviderOrderFailed(tradeID string, provider string) error {
	return dao.Mdb.Model(&mdb.ProviderOrder{}).
		Where("trade_id = ?", tradeID).
		Where("provider = ?", provider).
		Update("status", mdb.ProviderOrderStatusFailed).Error
}

func SaveProviderOrderNotify(tradeID string, provider string, notifyRaw string) error {
	return dao.Mdb.Model(&mdb.ProviderOrder{}).
		Where("trade_id = ?", tradeID).
		Where("provider = ?", provider).
		Update("notify_raw", notifyRaw).Error
}

func MarkProviderOrderPaid(tradeID string, provider string, notifyRaw string) error {
	return dao.Mdb.Model(&mdb.ProviderOrder{}).
		Where("trade_id = ?", tradeID).
		Where("provider = ?", provider).
		Updates(map[string]interface{}{
			"status":     mdb.ProviderOrderStatusPaid,
			"notify_raw": notifyRaw,
		}).Error
}

func MarkProviderOrderExpired(tradeID string, provider string) error {
	return dao.Mdb.Model(&mdb.ProviderOrder{}).
		Where("trade_id = ?", tradeID).
		Where("provider = ?", provider).
		Where("status IN ?", []string{mdb.ProviderOrderStatusCreating, mdb.ProviderOrderStatusPending}).
		Update("status", mdb.ProviderOrderStatusExpired).Error
}

func GetSubOrderByTokenPayProvider(parentTradeID string, token string, payProvider string) (*mdb.Orders, error) {
	order := new(mdb.Orders)
	err := dao.Mdb.Model(order).
		Where("parent_trade_id = ?", parentTradeID).
		Where("token = ?", token).
		Where("pay_provider = ?", payProvider).
		Where("status = ?", mdb.StatusWaitPay).
		Limit(1).
		Find(order).Error
	return order, err
}
