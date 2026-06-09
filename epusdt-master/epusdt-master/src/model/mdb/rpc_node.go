package mdb

import "github.com/dromara/carbon/v2"

const (
	RpcNodeTypeHttp = "http"
	RpcNodeTypeWs   = "ws"
)

const (
	RpcNodeStatusUnknown = "unknown"
	RpcNodeStatusOk      = "ok"
	RpcNodeStatusDown    = "down"
)

// RpcNode records one RPC endpoint for a network. Clients select a node
// by weight among rows where Enabled=true AND Status=ok. A periodic
// task/rpc_health_job writes Status/LastLatencyMs/LastCheckedAt.
type RpcNode struct {
	Network       string      `gorm:"column:network;size:32;index:rpc_nodes_network_status_index,priority:1" json:"network" example:"tron"`
	Url string `gorm:"column:url;size:512" json:"url" example:"https://api.trongrid.io"`
	// 连接类型 http=HTTP请求 ws=WebSocket长连接
	Type string `gorm:"column:type;size:16" json:"type" enums:"http,ws" example:"http"`
	Weight        int         `gorm:"column:weight;default:1" json:"weight" example:"1"`
	ApiKey        string      `gorm:"column:api_key;size:255" json:"api_key" example:"your-api-key"`
	Enabled bool `gorm:"column:enabled;default:true" json:"enabled" example:"true"`
	// 健康状态 unknown=未知 ok=正常 down=异常
	Status string `gorm:"column:status;size:16;default:unknown;index:rpc_nodes_network_status_index,priority:2" json:"status" enums:"unknown,ok,down" example:"ok"`
	LastLatencyMs int         `gorm:"column:last_latency_ms;default:-1" json:"last_latency_ms" example:"120"`
	LastCheckedAt carbon.Time `gorm:"column:last_checked_at" json:"last_checked_at" example:"2026-04-16 12:00:00"`
	BaseModel
}

func (r *RpcNode) TableName() string {
	return "rpc_nodes"
}
