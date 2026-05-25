-- ============================================================================
-- init.sql — MultiIndexer 数据库初始化脚本
-- ============================================================================
-- 这个脚本在 MySQL 容器首次启动时自动执行（因为挂载到了
-- docker-entrypoint-initdb.d 目录）。
--
-- 【MySQL 概念：数据库 vs 表】
-- 数据库（Database）是一个容器，包含多张表（Table）。
-- 表是真正存储数据的地方，每张表由列（字段）和行（记录）组成。
-- 一个 MySQL 实例可以有多个数据库，本项目全部数据存在 indexer 数据库中。
--
-- 【MySQL 概念：存储引擎 InnoDB】
-- ENGINE=InnoDB 是 MySQL 的默认存储引擎，支持：
--   - 事务（ACID）：要么全部成功，要么全部回滚
--   - 行级锁：高并发写入时性能好
--   - 外键约束：保证数据一致性
--
-- 【MySQL 概念：字符集 utf8mb4】
-- CHARSET=utf8mb4 支持完整的 Unicode（包括 emoji）。
-- 不要用 utf8（MySQL 的 utf8 只支持 3 字节，不支持 emoji）。
-- ============================================================================

-- 创建数据库（如果不存在）
CREATE DATABASE IF NOT EXISTS indexer;
USE indexer;

-- ============================================================================
-- blocks 表 — 区块头表
-- ============================================================================
-- 记录每条链上已经处理过的区块。主要用于：
--   1. 防止重复处理同一个区块（唯一索引）
--   2. 统计每条链处理了多少区块
--
-- 【MySQL 概念：BIGINT】
-- BIGINT 是 64 位有符号整数（最大 9223372036854775807），适合存区块高度。
-- 以太坊主网区块约 18,000,000+，还在持续增长。
CREATE TABLE IF NOT EXISTS blocks (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,       -- 自增主键（每行唯一 ID）
    chain VARCHAR(20) NOT NULL,                 -- 链名：eth / bsc / sol
    block_number BIGINT NOT NULL,               -- 区块号（Solana 是 Slot）
    block_hash VARCHAR(100) NOT NULL,           -- 区块哈希（区块的唯一指纹）
    block_time TIMESTAMP NOT NULL,              -- 区块时间戳
    tx_count INT DEFAULT 0,                     -- 区块内的交易数量
    UNIQUE KEY uk_chain_number (chain, block_number),  -- 唯一索引：同一链+同一区块号不能重复
    KEY idx_block_time (block_time)                   -- 普通索引：按时间查询
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================================================
-- events 表 — 事件表（核心数据表）
-- ============================================================================
-- 统一存储所有链的 Token 和 NFT 事件。是整个系统最重要的表。
--
-- 【设计决策：为什么 Token 和 NFT 用同一张表？】
-- Token 和 NFT 事件在结构上非常相似（都有转账、铸造、销毁），
-- 唯二的区别是 NFT 多了 token_id 且 amount 总是 1。
-- 用 category 字段（token/nft）区分，查询时 WHERE category='nft' 即可。
-- 这样避免了维护两张几乎一样的表。
--
-- 【MySQL 概念：UNIQUE KEY】
-- uk_tx_log (tx_hash, log_index) 保证同一笔交易的同一个 log 不会重复插入。
-- 当 listener 重启后重新拉取区块时，已存在的事件会被 ON DUPLICATE KEY UPDATE 更新，
-- 而不是报错。
--
-- 【MySQL 概念：VARCHAR 存储大数】
-- amount 存 token 的原始单位（如 1000000000000000000 = 1 ETH）。
-- 为什么用 VARCHAR 不用 DECIMAL？
-- 以太坊 uint256 最大可达 78 位十进制数，远超 MySQL DECIMAL 最大 65 位精度。
-- 用 VARCHAR(100) 安全存储任意大数，保留完整精度。
CREATE TABLE IF NOT EXISTS events (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,       -- 自增主键
    chain VARCHAR(20) NOT NULL,                 -- 链名
    block_number BIGINT NOT NULL,               -- 区块号
    tx_hash VARCHAR(100) NOT NULL,              -- 交易哈希
    log_index INT NOT NULL,                     -- 事件日志在交易中的索引（一笔交易可能有多条事件）
    category VARCHAR(20) NOT NULL COMMENT 'token / nft', -- 类别：token 或 nft
    event_type VARCHAR(50) NOT NULL COMMENT 'Transfer / Mint / Burn / Approval', -- 事件类型
    contract_address VARCHAR(100) NOT NULL,     -- 合约地址
    from_address VARCHAR(100),                  -- 发送方（可为 NULL，Mint 时是零地址）
    to_address VARCHAR(100),                    -- 接收方（可为 NULL，Burn 时是零地址）
    token_id VARCHAR(100) COMMENT 'NFT token ID', -- NFT 的 Token ID（非 NFT 为 NULL）
    amount VARCHAR(100) COMMENT 'token amount in raw units as string', -- 金额（字符串存大数）
    symbol VARCHAR(20) COMMENT 'token symbol if known', -- 代币符号（如 USDT）
    decimals INT DEFAULT 18,                    -- 代币精度（小数位数）
    raw_data JSON,                              -- 原始事件 JSON（保留完整数据，便于调试）
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- 记录创建时间
    UNIQUE KEY uk_tx_log (tx_hash, log_index),  -- 唯一索引：防重复
    KEY idx_chain_category (chain, category),   -- 复合索引：按链和类别查询
    KEY idx_chain_contract (chain, contract_address), -- 复合索引：按合约查询
    KEY idx_chain_from (chain, from_address),   -- 复合索引：按发送方查询
    KEY idx_chain_to (chain, to_address),       -- 复合索引：按接收方查询
    KEY idx_created (created_at)                -- 按时间排序
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================================================
-- alerts 表 — 告警表
-- ============================================================================
-- 记录系统触发的所有告警，包括大额转账和监控地址活动。
--
-- 【MySQL 概念：JSON 类型】
-- detail 字段使用 JSON 类型，存储半结构化的告警详情。
-- MySQL 的 JSON 类型可以：
--   - 自动验证 JSON 格式（非法 JSON 会报错）
--   - 支持 JSON 路径查询（如 SELECT detail->>'$.amount' FROM alerts）
--   - 创建虚拟列索引
CREATE TABLE IF NOT EXISTS alerts (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    chain VARCHAR(20) NOT NULL,                 -- 哪个链的事件
    alert_type VARCHAR(50) NOT NULL COMMENT 'large_transfer / watched_address', -- 告警类型
    tx_hash VARCHAR(100) NOT NULL,              -- 关联的交易哈希
    severity VARCHAR(20) DEFAULT 'medium' COMMENT 'low / medium / high / critical', -- 严重程度
    detail JSON,                                -- 告警详情（JSON 格式，灵活存储）
    notified BOOLEAN DEFAULT FALSE,             -- 是否已通知（后续阶段用）
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    KEY idx_chain_type (chain, alert_type),     -- 按链和告警类型查询
    KEY idx_severity (severity),                -- 按严重程度查询
    KEY idx_created (created_at)                -- 按时间排序
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================================================
-- indexer_state 表 — 索引状态表
-- ============================================================================
-- 记录每条链当前处理到哪个区块，用于：
--   1. 程序重启后知道从哪里继续（断点续传）
--   2. 监控索引进度（是否落后于链的最新高度）
--
-- 每条链只有一行记录，用 ON DUPLICATE KEY UPDATE 持续更新。
CREATE TABLE IF NOT EXISTS indexer_state (
    chain VARCHAR(20) PRIMARY KEY,              -- 链名作为主键（每条链只有一行）
    last_block BIGINT NOT NULL DEFAULT 0,       -- 最后处理的区块号
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP -- 最后更新时间（自动更新）
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================================================
-- 初始化数据：插入三条链的初始状态
-- ============================================================================
-- INSERT IGNORE：如果已存在（chain 主键冲突），不报错，静默跳过。
INSERT IGNORE INTO indexer_state (chain, last_block) VALUES
    ('eth', 0),
    ('bsc', 0),
    ('sol', 0);
