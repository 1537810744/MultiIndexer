package request

import "github.com/gookit/validate"

// CreateTransactionRequest 创建交易请求
type CreateTransactionRequest struct {
	OrderId     string  `json:"order_id" form:"order_id" validate:"required|maxLen:32" example:"ORD20260416001"`
	Currency    string  `json:"currency" form:"currency" validate:"required" example:"cny"` // 法币 如：cny
	Token       string  `json:"token" form:"token" validate:"required" example:"usdt"`      // 币种 如：usdt
	Network     string  `json:"network" form:"network" validate:"required" example:"tron"`  // 网络 如：tron
	Amount      float64 `json:"amount" form:"amount" validate:"required|isFloat|gt:0.01" example:"100.00"`
	NotifyUrl   string  `json:"notify_url" form:"notify_url" validate:"required" example:"https://example.com/notify"`
	Signature   string  `json:"signature" form:"signature" validate:"required" example:"a1b2c3d4e5f6..."`
	RedirectUrl string  `json:"redirect_url" form:"redirect_url" example:"https://example.com/success"`
	Name        string  `json:"name" form:"name" example:"VIP月卡"`
	PaymentType string  `json:"payment_type" form:"payment_type" example:"Epay"`
}

func (r CreateTransactionRequest) Translates() map[string]string {
	return validate.MS{
		"OrderId":   "订单号",
		"Currency":  "货币",
		"Token":     "币种",
		"Network":   "网络",
		"Amount":    "支付金额",
		"NotifyUrl": "异步回调网址",
		"Signature": "签名",
	}
}

// OrderProcessingRequest 订单处理
type OrderProcessingRequest struct {
	ReceiveAddress     string
	Currency           string
	Token              string
	Network            string
	Amount             float64
	TradeId            string
	BlockTransactionId string
}

// SwitchNetworkRequest 切换支付网络
type SwitchNetworkRequest struct {
	TradeId string `json:"trade_id" validate:"required" example:"T2026041612345678"`
	Token   string `json:"token" validate:"required" example:"USDT"`
	Network string `json:"network" validate:"required" example:"okpay,tron,solana,ethereum"`
}

func (r SwitchNetworkRequest) Translates() map[string]string {
	return validate.MS{
		"TradeId": "订单号",
		"Token":   "币种",
		"Network": "网络",
	}
}
