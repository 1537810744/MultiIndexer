package mdb

const (
	NotificationTypeTelegram = "telegram"
	NotificationTypeWebhook  = "webhook"
	NotificationTypeEmail    = "email"
)

const (
	NotificationStatusEnable  = 1
	NotificationStatusDisable = 2
)

// Notification events (JSON keys in Events column).
const (
	NotifyEventPaySuccess   = "pay_success"
	NotifyEventOrderExpired = "order_expired"
	NotifyEventDailyReport  = "daily_report"
)

// NotificationChannel represents one push target instance.
// Config/Events are JSON strings consumed by the notify dispatcher.
// For telegram, Config = {"bot_token":"...","chat_id":"...","proxy":"..."}.
// For webhook, Config = {"url":"...","headers":{...},"secret":"..."}.
type NotificationChannel struct {
	Type    string `gorm:"column:type;size:32;index:notification_channels_type_enabled_index,priority:1" json:"type" enums:"telegram,webhook,email" example:"telegram"`
	Name    string `gorm:"column:name;size:128" json:"name" example:"主通知群"`
	Config  string `gorm:"column:config;type:text" json:"config" example:"{\"bot_token\":\"123:ABC\",\"chat_id\":\"456\"}"`
	Events  string `gorm:"column:events;type:text" json:"events" example:"{\"pay_success\":true,\"order_expired\":true}"`
	Enabled bool   `gorm:"column:enabled;default:true;index:notification_channels_type_enabled_index,priority:2" json:"enabled" example:"true"`
	BaseModel
}

func (n *NotificationChannel) TableName() string {
	return "notification_channels"
}
