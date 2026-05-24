# ───────────────────────────────────────────────────────────────
# Dockerfile：构建 Listener 微服务的容器镜像
# ───────────────────────────────────────────────────────────────
#
# 【名词解释：Dockerfile】
# Dockerfile 是一份"食谱"，告诉 Docker 如何从零开始构建一个容器镜像。
# 镜像类似于虚拟机模板，里面包含了运行程序所需的全部环境：操作系统、依赖库、配置文件、可执行文件。
# 有了镜像，任何人只要装 Docker，就能一键跑出和开发者一模一样的环境，消除"在我机器上能跑"的问题。
#
# 【名词解释：多阶段构建（Multi-Stage Build）】
# 这个 Dockerfile 用了多阶段构建技巧，分为两个阶段：
#   阶段一（builder）：用体积大的 golang 镜像编译出二进制可执行文件。
#   阶段二（runtime）：用体积小的 alpine 镜像运行编译好的文件。
# 好处：最终镜像体积从几百 MB 压缩到十几 MB，攻击面更小，启动更快。

# ═══════════════════════════════════════════════════════════════
# 阶段一：编译阶段（Builder Stage）
# ═══════════════════════════════════════════════════════════════

# FROM 指令：指定基础镜像（base image）。
# golang:1.22-alpine 是官方提供的 Go 语言开发镜像，基于 Alpine Linux：
#   - 自带 Go 1.22 编译器、工具链和标准库源码。
#   - alpine 表示底层操作系统是 Alpine Linux（只有 5MB 左右，极度精简）。
# AS builder 给这个阶段起了个名字叫 "builder"，后面可以用 --from=builder 引用它。
FROM golang:1.22-alpine AS builder

# WORKDIR 指令：设置容器内的当前工作目录。
# 如果目录不存在会自动创建。后续所有指令（COPY、RUN 等）都基于这个路径执行。
# 这里设成 /app，是容器内部的绝对路径，和宿主机无关。
WORKDIR /app

# COPY 指令：把宿主机（构建上下文）里的文件复制到容器内。
# 这里先只复制 go.mod 和 go.sum 两个文件（Go 的依赖描述文件）。
# 技巧：先单独复制这两个文件再执行 go mod download，
# 可以利用 Docker 的"层缓存"机制——只要依赖没变，下次构建时就不会重复下载，速度极快。
COPY go.mod go.sum ./

# RUN 指令：在容器内执行一条 shell 命令，并把结果固化到新镜像层里。
# go mod download 会根据 go.mod 里记录的依赖版本，把第三方库下载到本地缓存。
# 这里下载的是 amqp091-go（RabbitMQ 客户端）。
RUN go mod download

# COPY . . 把构建上下文里的所有剩余文件（main.go 等）复制到容器内的 /app 目录。
# 第一个 . 表示宿主机当前目录（即 listener/ 文件夹），第二个 . 表示容器内的 WORKDIR。
COPY . .

# RUN 执行编译命令，把 Go 源码编译成 Linux 可执行文件。
#
# 【参数解释】
#   CGO_ENABLED=0：关闭 CGO。CGO 允许 Go 调用 C 语言代码，但会带来对 glibc 的依赖。
#                   关闭后编译出的二进制是"纯静态链接"的，不依赖任何系统库，
#                   可以在 alpine 甚至 scratch（空镜像）里运行。
#   GOOS=linux：指定目标操作系统为 Linux。即使你在 Windows/Mac 上构建，
#               也能编译出 Linux 可执行文件，这就是 Go 的"交叉编译"能力。
#   go build -o listener .：
#               go build 是编译命令；
#               -o listener 指定输出文件名为 listener（Linux 下没有 .exe 后缀）；
#               . 表示编译当前目录下的 main package。
RUN CGO_ENABLED=0 GOOS=linux go build -o listener .

# ═══════════════════════════════════════════════════════════════
# 阶段二：运行阶段（Runtime Stage）
# ═══════════════════════════════════════════════════════════════

# FROM alpine:latest 开启一个新的阶段，基础镜像是 Alpine Linux 的最新版。
# 它只有约 5MB，不含 Go 编译器、源码、调试工具，非常适合当"运行容器"。
FROM alpine:latest

# RUN apk --no-cache add ca-certificates
# Alpine 默认不带 CA 证书，如果程序需要连 HTTPS（比如真实链的 RPC 节点），会报证书错误。
# 这行指令用 Alpine 的包管理器 apk 安装 ca-certificates 包，并清理缓存（--no-cache），
# 让镜像保持最小体积。
RUN apk --no-cache add ca-certificates

# 设置运行阶段的工作目录为 /root/。
WORKDIR /root/

# COPY --from=builder 是跨阶段复制的关键语法。
# 它从第一个阶段（builder）的 /app/listener 路径，把编译好的二进制文件复制到当前阶段的 /root/ 目录。
# 最终镜像里只有这一个可执行文件，没有任何源码和编译工具。
COPY --from=builder /app/listener .

# CMD 指令：指定容器启动时默认执行的命令。
# 这里执行 ./listener，也就是阶段一编译出来的二进制文件。
# 当 docker compose up 启动这个服务时，就会执行 listener/main.go 里的 main() 函数。
CMD ["./listener"]
