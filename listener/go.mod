// go.mod 是 Go Modules（Go 1.11+ 引入的官方依赖管理工具）的核心配置文件。
// 它的作用类似于 Node.js 的 package.json、Python 的 requirements.txt、Java 的 pom.xml。
// 里面记录了：模块名、Go 语言最低版本、直接依赖的第三方库及其精确版本。
//
// 【名词解释：Module（模块）】
// 在 Go 中，一个模块是一个由 go.mod 文件标记的代码集合，通常对应一个 Git 仓库。
// 模块可以被其他项目引用，实现代码复用。

// module 指令：声明本模块的名称（也叫"模块路径"）。
// 通常用代码托管地址 + 项目路径的形式，比如 github.com/yourname/project。
// 这里用短名 "listener"，因为在当前项目内部使用，不需要对外发布。
module listener

// go 指令：指定编译本模块所需的最低 Go 语言版本。
// 如果你用 Go 1.21 编译器去编译一个要求 go 1.22 的模块，编译器会报错并拒绝构建。
// 这保证了代码不会因为编译器版本过老而默默出现兼容性问题。
go 1.22

// require 指令：声明本模块直接依赖的第三方包及其最低版本。
// 这里的 amqp091-go 是 RabbitMQ 官方维护的 Go 客户端库。
// v1.9.0 是语义化版本号（SemVer）：v<主版本>.<次版本>.<补丁版本>。
//   主版本变化 = API 不兼容的breaking change
//   次版本变化 = 向后兼容的新功能
//   补丁版本变化 = 向后兼容的 bug 修复
require github.com/rabbitmq/amqp091-go v1.9.0
