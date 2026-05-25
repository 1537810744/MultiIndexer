# Docker 全面解答 — MultiIndexer 项目

---

## 1. 为什么微服务没有 Docker 化还要写 Dockerfile？

**三个 Go 程序目前确实没在 Docker 里跑**，但 Dockerfile 是为"生产部署"预留的。

当前开发模式（Windows 本地）：
```
你 → go run . → 直接运行在 Windows 上 → 连接 Docker 里的中间件
```

Dockerfile 支持的部署模式（Linux 服务器）：
```
你 → docker build -t listener . → 生成镜像 → docker run listener
```

**Dockerfile 做的是**：把编译好的 Go 二进制文件打包进一个极小的镜像（`FROM scratch`），
这样到 Linux 服务器上只需要 `docker run` 就行了，不需要装 Go 环境。

`FROM scratch` 是 Docker 里最小的基础镜像——**完全空的**，里面没有 Linux 发行版，
没有 shell，只有你放进去的一个二进制文件。最终镜像只有 ~10MB。

---

## 2. 微服务的两阶段构建 Dockerfile 写法是固定的吗？

**标准模式是固定的，细节可以变。** 两阶段（Multi-Stage Build）的标准写法：

```dockerfile
# ============ 阶段 1：构建（编译） ============
FROM golang:1.21-alpine AS builder        # 带 Go 编译器的镜像（~300MB）
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download                        # 下载依赖
COPY . .
RUN CGO_ENABLED=0 go build -o myapp .     # 编译成静态二进制

# ============ 阶段 2：运行 ============
FROM scratch                               # 空镜像（最终 ~10MB）
COPY --from=builder /app/myapp /          # 只从阶段1拷贝编译好的文件
CMD ["/myapp"]
```

**两个阶段的分工：**
| 阶段 | 做什么 | 镜像大小 |
|------|--------|----------|
| 阶段1 (builder) | 编译 Go 代码 | ~300MB（用完就扔） |
| 阶段2 (runtime) | 运行二进制 | ~10MB（真正部署的） |

**哪些可以变、哪些不能变：**
- `FROM golang:1.21-alpine` — 可变，用你喜欢的基础镜像
- `CGO_ENABLED=0` — **必须**，否则编译出的二进制依赖 glibc，在 scratch 里跑不了
- `FROM scratch` — 可变，也可用 `alpine`（~5MB，带 shell 方便调试）
- `COPY --from=builder` — **必须**，这是两阶段构建的核心语法

当前项目的 Dockerfile 是简化版（直接 COPY 编译好的文件），
完整的应该补上构建阶段。

---

## 3. Docker Compose 是什么？和 Desktop 手动启动等价吗？

**Docker Compose 是"一键启动多容器"的工具。**

对比：

| 方式 | 操作 | 适用场景 |
|------|------|----------|
| **Docker Desktop 手动** | GUI 里逐个搜索镜像、配置端口、设置环境变量、启动 | 临时测试单个容器 |
| **docker compose up** | 读 YAML 文件，自动完成以上所有步骤 | 项目开发（一键启动所有依赖） |
| **docker run 命令** | 命令行一个个参数传 | 快速启动单个容器 |

**等价关系**：Docker Desktop 手动配出来的效果和 `docker-compose.yml` **完全一样**，
但 YAML 文件可以**版本控制、团队共享、反复使用**。
你在 Desktop 里点半天配好的容器，换台电脑就没了。

**本项目一条命令等价于 Desktop 里手动操作 3 次**（RabbitMQ + MySQL + Redis）。

---

## 4. docker compose 和 docker 命令有何区别？

```
docker run ...          ← 启动一个容器（命令很长，参数很多）
docker compose up       ← 按 YAML 文件启动所有容器（一键）
```

**具体对比（启动 MySQL 为例）：**

docker 命令（手动）：
```bash
docker run -d \
  --name mysql-indexer \
  -p 3307:3306 \
  -e MYSQL_ROOT_PASSWORD=rootpass \
  -e MYSQL_DATABASE=indexer \
  -e MYSQL_USER=indexer \
  -e MYSQL_PASSWORD=indexerpass \
  -v mysql_data:/var/lib/mysql \
  -v ./init.sql:/docker-entrypoint-initdb.d/init.sql \
  --network indexer-net \
  mysql:latest
```

docker compose（读 YAML）：
```bash
docker compose up -d    # 一句话完成上面所有事
```

**关键区别**：docker compose 不只是启动工具，它还管理容器的**生命周期**：
- `docker compose up -d` — 启动
- `docker compose down` — 停止并删除所有容器
- `docker compose restart` — 重启
- `docker compose logs -f` — 查看所有容器日志
- `docker compose ps` — 查看容器状态

---

## 5. 为什么数据库端口写在 docker compose 里？

**因为端口映射只能在启动容器时指定，不能事后改。**

```yaml
ports:
  - "3307:3306"    # 宿主端口:容器端口
```

- **3307**（左边）：你本机 Windows 的端口，Go 程序连这个
- **3306**（右边）：容器内部 MySQL 监听的端口（MySQL 镜像的默认端口）

**为什么映射成 3307 而不是 3306？**
Windows 上可能已经装了一个 MySQL 占了 3306。映射到 3307 避免冲突。

**如果不在 docker-compose 里写端口会怎样？**
- 容器之间（如 processor 容器连 mysql 容器）可以通过容器名互相访问，不需要端口映射
- 但**宿主机上的 Go 程序**连不到容器里的 MySQL
- 所以端口映射是给**容器外的程序**用的

---

## 6. 密码和用户是在哪里设置的？

全部在 `docker-compose.yml` 的 `environment` 字段里：

```yaml
# RabbitMQ
rabbitmq:
  environment:
    RABBITMQ_DEFAULT_USER: admin      # 用户名
    RABBITMQ_DEFAULT_PASS: admin      # 密码

# MySQL
mysql:
  environment:
    MYSQL_ROOT_PASSWORD: rootpass     # root 密码
    MYSQL_DATABASE: indexer           # 自动创建的数据库名
    MYSQL_USER: indexer               # 应用用户名
    MYSQL_PASSWORD: indexerpass       # 应用用户密码

# Redis
redis:
  # Redis 默认无密码（开发环境）
```

**这些值怎么生效的？**
1. 容器启动时，Docker 把这些环境变量注入容器
2. MySQL/RabbitMQ 镜像的**入口脚本**读取环境变量，初始化用户和密码
3. Go 程序从自己的环境变量 `/ 默认值` 读取同样的凭据去连接

**对应关系（Go 代码中的默认值）：**
```go
// processor/main.go
amqpURL := getEnv("RABBITMQ_URL", "amqp://admin:admin@localhost:5672/")
mysqlDSN := getEnv("MYSQL_DSN", "indexer:indexerpass@tcp(localhost:3307)/indexer")
//                              ^^^^^^^  ^^^^^^^^^^^              ^^^^
//                              用户名    密码                    端口（映射后的）
```

---

## 7. 怎么容器化的？怎么启动的？

**容器化只有中间件（MySQL、RabbitMQ、Redis），Go 程序不在容器里。**

```
┌─────────────────────────────────────────────────┐
│                   你的 Windows                    │
│                                                  │
│  go run . (listener)   →   连接 5672  →  ┌──────┴──────┐  │
│  go run . (processor)  →   连接 5672  →  │  RabbitMQ   │  │
│                         →   连接 3307  →  │  MySQL      │  │
│  go run . (fake-api)   →   连接 6379  →  │  Redis      │  │
│                                           └─────────────┘  │
│                                            Docker 容器     │
└─────────────────────────────────────────────────┘
```

**启动全过程：**

```bash
# 第一步：启动中间件（一次性）
cd MultiIndexer
docker compose up -d

# 第二步：启动 Go 程序（三个终端窗口）
cd listener && go run .
cd processor && go run .
cd fake-api && go run .
```

---

## 8. 从最开始还没有容器，到 Redis/MySQL/RabbitMQ 启动，发生了什么？

详细时间线：

```
第 0 步：你执行 docker compose up -d
         │
         ▼
第 1 步：Docker Compose 读取 docker-compose.yml
         │
         ├─ 检查镜像：rabbitmq:3-management-alpine 在本地有没有？
         │   ├─ 有 → 跳过
         │   └─ 没有 → docker pull 从 Docker Hub 下载
         │
         ├─ 创建网络 indexer-net（bridge 模式）
         │   所有容器加入这个网络，可以通过容器名互相访问
         │
         ├─ 创建数据卷（rabbitmq_data, mysql_data, redis_data）
         │   第一次是空的，之后数据存在这里
         │
         └─ 按顺序启动容器（尊重 depends_on 顺序）
              │
              ▼
第 2 步：启动 RabbitMQ 容器
         │
         ├─ 创建容器 "mq-indexer"
         ├─ 注入环境变量 RABBITMQ_DEFAULT_USER=admin
         ├─ 挂载数据卷 rabbitmq_data → /var/lib/rabbitmq
         ├─ 映射端口 5672:5672, 15672:15672
         ├─ 容器内 RabbitMQ 进程启动
         │   ├─ 初始化用户 admin/admin
         │   ├─ 创建默认 vhost "/"
         │   └─ 监听 5672（AMQP）、15672（管理界面）
         └─ 健康检查：每隔 10 秒 ping 一次，5 次失败标记 unhealthy
              │
              ▼
第 3 步：启动 MySQL 容器
         │
         ├─ 创建容器 "mysql-indexer"
         ├─ 注入环境变量（rootpass, indexer, indexerpass, indexer）
         ├─ 挂载数据卷 mysql_data → /var/lib/mysql
         ├─ 挂载 init.sql → /docker-entrypoint-initdb.d/init.sql
         ├─ 映射端口 3307:3306
         ├─ 容器内 MySQL 进程启动
         │   ├─ 初始化 root 密码为 rootpass
         │   ├─ 创建数据库 indexer
         │   ├─ 创建用户 indexer（密码 indexerpass）
         │   ├─ 授权 indexer 用户访问 indexer 数据库
         │   ├─ 执行 /docker-entrypoint-initdb.d/ 下的所有 .sql 文件
         │   │   └─ init.sql → 创建 events、alerts、indexer_state、blocks 四张表
         │   └─ 监听 3306
         └─ 健康检查：mysqladmin ping，最多重试 10 次（MySQL 启动慢）
              │
              ▼
第 4 步：启动 Redis 容器
         │
         ├─ 创建容器 "redis-indexer"
         ├─ 挂载数据卷 redis_data → /data
         ├─ 映射端口 6379:6379
         ├─ 容器内 Redis 进程启动
         │   └─ 监听 6379，无密码
         └─ 健康检查：redis-cli ping → 应返回 PONG
              │
              ▼
第 5 步：三个容器全部 healthy
         │
         └─ docker compose up -d 返回成功
            三个中间件已就绪，等待 Go 程序连接
```

---

## 9. Docker 常见命令

### Docker 服务管理
```bash
docker compose up -d          # 启动 docker-compose.yml 中所有服务（后台运行）
docker compose down           # 停止并删除所有服务（数据卷保留）
docker compose down -v        # 停止并删除服务 + 数据卷（数据也删！危险！）
docker compose restart        # 重启所有服务
docker compose ps             # 查看服务状态
docker compose logs -f        # 实时查看所有服务日志
docker compose logs mq-indexer  # 只看 RabbitMQ 日志
```

### 容器操作
```bash
docker ps                     # 查看正在运行的容器
docker ps -a                  # 查看所有容器（包括已停止的）
docker start mysql-indexer    # 启动已停止的容器
docker stop mysql-indexer     # 停止容器
docker rm mysql-indexer       # 删除容器（数据在 volume 里不会丢）
docker exec -it mysql-indexer mysql -u root -p   # 进入容器执行命令
docker inspect mysql-indexer  # 查看容器详细信息（IP、挂载等）
```

### 镜像操作
```bash
docker images                 # 查看本地有哪些镜像
docker pull mysql:latest      # 下载镜像
docker rmi mysql:latest       # 删除镜像
docker build -t myapp .       # 用 Dockerfile 构建镜像
```

### 数据卷操作
```bash
docker volume ls              # 查看所有数据卷
docker volume rm mysql_data   # 删除数据卷（数据会丢！）
docker volume prune           # 删除所有未被使用的数据卷
```

### 网络操作
```bash
docker network ls             # 查看所有网络
docker network inspect indexer-net   # 查看网络详情（哪些容器连在上面）
```

### 调试
```bash
docker logs mysql-indexer     # 查看容器日志（排查启动失败）
docker logs -f mysql-indexer  # 实时跟踪日志
docker stats                  # 查看所有容器 CPU/内存占用
docker system prune -a        # 清理所有未使用的镜像、容器、网络、缓存
```

### 本项目常用组合
```bash
# 开发日常
docker compose up -d                    # 启动中间件
docker compose ps                       # 确认三个都是 healthy
docker compose logs -f --tail=50        # 看最近 50 行日志

# 完全重置（数据库清空重新来）
docker compose down -v                  # 删除容器+数据卷
docker compose up -d                    # 重新启动（MySQL 会重新执行 init.sql）

# 只重启某一个
docker compose restart mysql            # 只重启 MySQL

# 进入 MySQL 查数据
docker exec -it mysql-indexer mysql -u indexer -pindexerpass indexer
```

---

## 10. Docker 卷（Volume）详解

### 10.1 Docker 不是操作系统

先纠正一个常见误解：**Docker 不是一个操作系统**。

```
你的 Windows 电脑（宿主机）
│
├─ Docker Desktop（一个 Windows 应用程序）
│   │
│   ├─ Linux 虚拟机（WSL2，精简版 Linux 内核）
│   │   │
│   │   ├─ 容器 A：MySQL 进程
│   │   ├─ 容器 B：Redis 进程
│   │   └─ 容器 C：RabbitMQ 进程
│   │
│   └─ Docker 管理界面
```

- **宿主机（Host）**：你的 Windows，真实的操作系统
- **Docker Engine**：跑在 Linux 虚拟机里的容器管理器
- **容器（Container）**：一个被隔离的进程 + 它的文件系统，共享同一个 Linux 内核
- 容器不是虚拟机——它没有自己的操作系统，只是 Linux 上的一个隔离进程

### 10.2 三张图彻底搞懂：数据到底存在哪？

先搞清楚三个"位置"是什么关系：

```
┌─────────────────────────────────────────────────────────────────┐
│                   ① 你的 Windows 宿主机                          │
│                                                                 │
│   C:\Users\admin\Desktop\MultiIndexer\                          │
│   ├── docker-compose.yml                                        │
│   ├── init.sql             ← 这是你的项目文件夹（你能直接操作）    │
│   └── ...                                                       │
│                                                                 │
│   ┌─────────────────── WSL2 Linux 虚拟机 ────────────────────┐  │
│   │                                                          │  │
│   │   ┌── ② Docker Volume 层 ──┐                            │  │
│   │   │                        │                            │  │
│   │   │  /var/lib/docker/      │                            │  │
│   │   │  volumes/mysql_data/   │ ← Docker 帮你管理的文件夹    │  │
│   │   │  _data/                │   （你不用管具体路径）        │  │
│   │   │   ├── ibdata1          │                            │  │
│   │   │   ├── indexer/         │                            │  │
│   │   │   └── ...              │                            │  │
│   │   └────────────────────────┘                            │  │
│   │                    │ ↕ 挂载（mount）                      │  │
│   │   ┌── ③ 容器内部文件系统 ──┐                              │  │
│   │   │                        │                            │  │
│   │   │  /var/lib/mysql/       │ ← MySQL 进程看到的路径       │  │
│   │   │   ├── ibdata1  ←───────── 实际指向 Volume 里的文件    │  │
│   │   │   ├── indexer/ ←───────── 实际指向 Volume 里的文件    │  │
│   │   │   └── ...              │                            │  │
│   │   │                        │                            │  │
│   │   │  /etc/mysql/           │ ← 这些是容器自带的文件        │  │
│   │   │  /usr/bin/mysqld       │   容器删了这些也会没          │  │
│   │   └────────────────────────┘                            │  │
│   │                                                          │  │
│   │   MySQL 进程 跑在这个容器里                                 │  │
│   │   Redis 进程  跑在另一个容器里                               │  │
│   │   RabbitMQ 进程 跑在又一个容器里                             │  │
│   └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

**关键理解**：容器里 `/var/lib/mysql/` 这个目录，只是一个"入口"——它指向的真实位置在 Volume 层。MySQL 进程以为自己写的是 `/var/lib/mysql/xxx`，实际上数据被重定向到了 Volume 里。

### 10.3 三种存法对比（用具体路径说话）

**方式 1：写在容器里（❌ 不推荐）**

没有挂载任何东西，数据直接写在容器内部文件系统里：

```
docker run mysql → 自动创建容器内部文件系统
                   MySQL 把数据写到 → 容器文件系统 /var/lib/mysql/
                   
删容器 → 容器文件系统连同 /var/lib/mysql/ 全部销毁 → 数据没了
```

**方式 2：Bind Mount 挂载 Windows 文件夹**

把 Windows 上的一个文件夹"映射"进容器：

```yaml
volumes:
  - C:/myproject/mysql_data:/var/lib/mysql
    # ^^^^^^^^^^^^^^^^^^^^^  ^^^^^^^^^^^^^
    # 你 Windows 上的真实路径  容器内 MySQL 看到的路径
```

```
C:\myproject\mysql_data\          ← 真实的文件夹，在 Windows 上
    ↕ 双向映射
容器内 /var/lib/mysql/            ← 容器内只是"影子"，实际操作的都是 Windows 文件夹
```

- MySQL 写一条数据 → 文件出现在 `C:\myproject\mysql_data\` 里
- 你用 VS Code 删掉那个文件 → MySQL 里的数据也丢了
- **你能直接用资源管理器打开 `C:\myproject\mysql_data\` 看到所有数据库文件**

**方式 3：Docker Volume（✓ 推荐）**

```yaml
volumes:
  mysql_data:           # 顶层声明：给我一个叫 mysql_data 的卷
                         # Docker 在 WSL2 虚拟机里创建这个文件夹

services:
  mysql:
    volumes:
      - mysql_data:/var/lib/mysql
        # ^^^^^^^^^  ^^^^^^^^^^^^^
        # Volume 名   容器内 MySQL 看到的路径
```

```
WSL2 Linux 虚拟机内部
/var/lib/docker/volumes/mysql_data/_data/   ← 实际文件夹，Docker 管理
    ↕ 双向映射
容器内 /var/lib/mysql/                       ← 容器内的"入口"
```

- 数据存在 WSL2 虚拟机里，不占用你 Windows 的 C 盘路径
- 你不能直接用资源管理器打开（在 Linux 虚拟机里）
- 但 Docker 有专门命令管理：`docker volume ls`、备份等

### 10.4 本项目 docker-compose.yml 里的完整写法解读

```yaml
# ===== 顶层：声明 Volume（告诉 Docker 创建这些文件夹）=====
volumes:
  mysql_data:       # Volume 名，Docker 会创建 /var/lib/docker/volumes/mysql_data/_data/
  redis_data:       #                        /var/lib/docker/volumes/redis_data/_data/
  rabbitmq_data:    #                        /var/lib/docker/volumes/rabbitmq_data/_data/

# ===== 具体服务里：把 Volume 挂载到容器内的某个路径 =====
services:
  mysql:
    volumes:
      - mysql_data:/var/lib/mysql
      #  ^^^^^^^^^  ^^^^^^^^^^^^^
      #  Volume 名   容器内路径（MySQL 的默认数据目录）

      - ./init.sql:/docker-entrypoint-initdb.d/init.sql
      #  ^^^^^^^^^  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
      #  Bind Mount：Windows 上的文件，直接映射进容器
      #  容器启动时会自动执行这个 SQL 脚本
```

两条 `volumes` 配置，本质不同：

| 写法 | 类型 | 真实位置 | 用途 |
|------|------|---------|------|
| `mysql_data:/var/lib/mysql` | **Volume** | WSL2 虚拟机里 | 存 MySQL 产生的数据库文件 |
| `./init.sql:/docker-entrypoint-initdb.d/init.sql` | **Bind Mount** | 你 Windows 项目文件夹里 | 把一个文件"喂"给容器用 |

### 10.5 那容器内部的文件系统又是什么？

每个容器创建时，Docker 会从镜像复制一份文件系统给它：

```
镜像（mysql:latest）是一份"模板"，里面包含了：
  ├── /usr/bin/mysqld        ← MySQL 服务器程序
  ├── /etc/mysql/            ← MySQL 配置文件
  ├── /var/lib/mysql/        ← 默认数据目录（空的）
  └── ...（完整的 Linux 文件系统）

创建容器时：
  镜像的 /var/lib/mysql/ 被 Volume "覆盖"
  → 原来空的 /var/lib/mysql/ 现在指向 Volume 里的实际文件夹
  
  镜像的其他部分（/etc/mysql/、/usr/bin/mysqld 等）不变
  → 这些是容器自己的文件，删容器就没了
```

**一句话**：容器文件系统 = 镜像模板 + Volume 覆盖（如果有挂载的话）。没有 Volume 覆盖的部分，容器删了就没了；有 Volume 覆盖的部分，容器删了数据还在。

### 10.3 为什么必须用卷？不用会怎样？

**容器是无状态的**——删除容器时，容器内的所有文件都会消失。

用一个具体例子演示：

```
场景：你存了 10 万条链上事件到 MySQL

有 Volume（当前配置）：
  1. docker compose down          ← 删除容器
  2. docker compose up -d         ← 重新创建容器
  3. SELECT COUNT(*) FROM events  → 100000 条，数据还在 ✓

没有 Volume：
  1. docker compose down          ← 删除容器
  2. docker compose up -d         ← 重新创建容器
  3. SELECT COUNT(*) FROM events  → 0 条，全丢了 ❌
```

**容器就像一次性饭盒——吃完就扔，但卷就是你的冰箱。**

### 10.4 卷实际存在哪里？

在你的 Windows 上，卷存在 WSL2 虚拟机里：

```
真实路径（不要直接操作）：
  \\wsl$\docker-desktop-data\data\docker\volumes\

实际位置（Docker 管理的）：
  /var/lib/docker/volumes/mysql_data/_data/
  /var/lib/docker/volumes/redis_data/_data/
  /var/lib/docker/volumes/rabbitmq_data/_data/
```

**你不需要知道具体路径**，Docker 替你管理。你只需要在 `docker-compose.yml` 里声明卷名就行。

如果需要备份：
```bash
# 把 MySQL 数据卷的内容导出到当前目录
docker run --rm -v mysql_data:/data -v .:/backup alpine cp -r /data /backup/mysql_backup
```

### 10.5 本项目三个卷分别存什么

```
rabbitmq_data → 容器内 /var/lib/rabbitmq
  ├─ 消息队列数据（未消费的消息）
  ├─ 用户配置（admin/admin）
  └─ Exchange/Queue/Binding 定义

mysql_data → 容器内 /var/lib/mysql
  ├─ indexer 数据库的所有表
  │   ├─ events（链上事件）
  │   ├─ alerts（告警记录）
  │   ├─ indexer_state（索引进度）
  │   └─ blocks（区块数据）
  ├─ MySQL 系统表（用户、权限）
  └─ binlog（二进制日志）

redis_data → 容器内 /data
  ├─ dump.rdb（Redis 定期快照）
  └─ 缓存的热数据（最新事件、告警）
```

### 10.6 Bind Mount vs Volume 对比

本项目还用了 **Bind Mount**（挂载宿主机文件）：

```yaml
volumes:
  - ./init.sql:/docker-entrypoint-initdb.d/init.sql
  #  ^^^^^^^^    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  #  相对路径     容器内路径
  #  (Windows)   (Linux 容器内)
```

| | Bind Mount | Volume |
|---|---|---|
| **路径** | Windows 上的 `./init.sql` | Docker 管理的 `mysql_data` |
| **用途** | 你主动提供的文件（配置、初始化脚本） | 容器自己产生的数据（数据库文件） |
| **你能直接编辑吗** | ✓ 用 VS Code 打开就能改 | ✗ 在 WSL2 虚拟机里，不方便直接操作 |
| **删容器后会丢吗** | 不会（文件在你 Windows 上） | 不会（除非加 `-v` 强制删除） |

### 10.7 卷的生命周期

```
1. 第一次 docker compose up -d
   → Docker 自动创建 mysql_data、redis_data、rabbitmq_data 三个卷
   → 此时是空的

2. MySQL 启动，写入数据
   → 卷里出现数据库文件

3. docker compose down
   → 容器被删除
   → 卷还在，数据保留 ✓

4. docker compose up -d（再次启动）
   → 新容器挂载同一个卷
   → MySQL 看到之前的数据库文件，数据还在 ✓

5. docker compose down -v
   → 容器删除 + 卷也被删除
   → 数据彻底消失 ❌（除非有备份）
```

---

## 11. 关键概念总结

| 概念 | 一句话解释 | 本项目中的应用 |
|------|-----------|---------------|
| **Image（镜像）** | 容器的"安装包"，只读模板 | `mysql:latest`, `redis:latest` |
| **Container（容器）** | 镜像的运行实例——一个被隔离的 Linux 进程 | `mysql-indexer`, `redis-indexer` |
| **Volume（数据卷）** | Docker 管理的持久化文件夹，容器删了数据还在 | 存 MySQL 数据、Redis 快照、RabbitMQ 消息 |
| **Bind Mount** | 把宿主机文件/文件夹直接映射进容器 | `./init.sql` → MySQL 初始化脚本 |
| **Port Mapping（端口映射）** | 让宿主机能访问容器内的端口 | `3307:3306`、`5672:5672`、`6379:6379` |
| **Network（网络）** | 让多个容器能互相通信 | `indexer-net`（bridge 模式） |
| **Environment（环境变量）** | 向容器内传递配置 | 数据库密码、用户名、默认数据库 |
| **Healthcheck（健康检查）** | 确认容器内部的服务是否真的就绪了 | `mysqladmin ping`、`redis-cli ping` |
| **Docker Compose** | 一键管理多个容器的工具 | 一个 YAML 管三个中间件 |
| **Dockerfile** | 构建自定义镜像的配方 | 把 Go 二进制打包进 scratch |
| **Multi-Stage Build** | 编译和运行分离，最终镜像极小 | `golang` 编译 + `scratch` 运行 |
