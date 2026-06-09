package http_client

import (
	"time"

	"github.com/go-resty/resty/v2"
)

// ClientFactory is overridden in tests to stub outbound HTTP calls.
var ClientFactory = resty.New

// GetHttpClient 获取请求客户端
func GetHttpClient(proxys ...string) *resty.Client {
	client := ClientFactory()
	// 如果有代理
	if len(proxys) > 0 {
		proxy := proxys[0]
		client.SetProxy(proxy)
	}
	client.SetTimeout(time.Second * 10)
	return client
}
