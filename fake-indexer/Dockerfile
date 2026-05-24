# ───────────────────────────────────────────────────────────────
# Dockerfile：构建 Fake-Indexer 微服务的容器镜像
# ───────────────────────────────────────────────────────────────
#
# 这个 Dockerfile 和 listener/Dockerfile 的结构几乎完全一致，
# 因为两个服务都是 Go 编写的微服务，构建流程相同。
# 唯一区别是编译输出的文件名不同（fake-indexer vs listener）。
# 在实际项目中，可以用一个通用 Dockerfile + 构建参数（ARG）来复用，但这里为了清晰保持独立。

# ═══════════════════════════════════════════════════════════════
# 阶段一：编译阶段（Builder Stage）
# ═══════════════════════════════════════════════════════════════

# 使用官方 Go 1.22 Alpine 镜像作为编译环境。
# AS builder 给本阶段命名，供后面 COPY --from=builder 引用。
FROM golang:1.22-alpine AS builder

# 设置容器内工作目录为 /app，后续操作都在这个路径下进行。
WORKDIR /app

# 先复制依赖描述文件，利用 Docker 层缓存加速构建。
# 如果 go.mod / go.sum 没有变化，下一行 RUN go mod download 会直接命中缓存，无需重复下载。
COPY go.mod go.sum ./

# 下载 Go 模块依赖（本服务只依赖 amqp091-go）。
RUN go mod download

# 复制剩余源码文件（main.go）到容器。
COPY . .

# 编译成 Linux 静态链接可执行文件。
# CGO_ENABLED=0 关闭 C 语言互操作，生成不依赖系统动态库的二进制，确保能在 alpine 中运行。
# GOOS=linux 指定目标操作系统（支持在 Windows/Mac 宿主机上交叉编译出 Linux 程序）。
# -o fake-indexer 指定输出文件名；. 表示编译当前目录的 main 包。
RUN CGO_ENABLED=0 GOOS=linux go build -o fake-indexer .

# ═══════════════════════════════════════════════════════════════
# 阶段二：运行阶段（Runtime Stage）
# ═══════════════════════════════════════════════════════════════

# 切换到 Alpine Linux 运行时镜像，体积极小（约 5MB），只保留运行程序所需的最小环境。
FROM alpine:latest

# 安装 CA 证书，防止程序未来在连接 HTTPS 端点时因证书缺失报错。
RUN apk --no-cache add ca-certificates

# 设置运行时工作目录。
WORKDIR /root/

# 从 builder 阶段复制编译好的二进制文件到当前镜像。
# 最终镜像里不包含 Go 编译器、源码或中间文件，体积极小且安全。
COPY --from=builder /app/fake-indexer .

# 容器启动时执行 ./fake-indexer，即运行 fake-indexer/main.go 里的 main() 函数。
CMD ["./fake-indexer"]
