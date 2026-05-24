// go.mod 是 Go Modules 的依赖管理清单文件。
// 它描述了模块身份、Go 版本要求、以及直接依赖的外部包。
// 当执行 go mod download 时，Go 工具链会读取这个文件，
// 从 Git 仓库或模块代理（如 https://proxy.golang.org）下载对应版本的依赖。

// module 指令定义模块名称。在微服务架构中，每个服务通常是一个独立模块，
// 各自维护自己的依赖，互不干扰。这里简单命名为 "fake-indexer"。
module fake-indexer

// go 1.22 表示本模块要求使用 Go 1.22 或更高版本的编译器。
// Go 1.22 带来了很多改进，比如 for 循环变量不再共享（修复了经典闭包陷阱）、
// net/http 性能提升、标准库新函数等。
go 1.22

// require 声明了一个直接依赖：RabbitMQ 的 Go 客户端 amqp091-go，版本 v1.9.0。
// 这个库实现了完整的 AMQP 0-9-1 协议，支持连接管理、通道、交换机、队列、发布、消费、
// QoS（流量控制）、确认（Confirm）等全部核心功能。
require github.com/rabbitmq/amqp091-go v1.9.0
