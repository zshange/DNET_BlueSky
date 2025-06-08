# PDS Checker & Followers 处理工具

这个工具集用于处理ScyllaDB数据，生成CSV文件，并为每个用户获取followers信息。

## 文件说明

- `main.go` - 主程序，从ScyllaDB读取数据并生成CSV文件
- `pds_follows.go` - Followers处理器，为CSV文件添加followers信息
- `csv_followers_main.go` - 独立的CSV处理程序

## 使用步骤

### 步骤1：生成CSV文件

运行主程序，从ScyllaDB中提取数据并生成20个CSV文件：

```bash
cd DNET_BlueSky/downloader/bsky-indexer/cmd/pds_checker
go run main.go
```

**输出**：
- 20个CSV文件：`repos_batch_01.csv` 到 `repos_batch_20.csv`
- 位置：`/mydata/csv_repos/`
- 每个文件：10,000条记录
- 格式：`did, record_count`

### 步骤2：添加Followers信息

使用独立的CSV处理程序为每个CSV文件添加followers列：

```bash
cd DNET_BlueSky/downloader/bsky-indexer/cmd/pds_checker
go run csv_followers_main.go pds_follows.go /mydata/csv_repos
```

**处理过程**：
- 自动发现所有CSV文件
- 为每个DID调用BlueSky API获取followers数量
- 更新CSV文件，添加第三列：`followers`
- 应用速率限制（每秒10个请求）
- 支持断点续传（已处理的记录会跳过）

### 最终CSV格式

处理完成后，每个CSV文件将包含三列：

```csv
did,record_count,followers
did:plc:example123,156,1234
did:plc:example456,89,567
...
```

## 配置参数

### main.go 配置
```go
const (
    TARGET_RECORD_COUNT   = 200000  // 目标记录数
    CSV_FILES_COUNT       = 20      // CSV文件数量
    RECORDS_PER_CSV       = 10000   // 每个CSV文件的记录数
    MIN_RECORDS_REQUIRED  = 5       // 最小记录数要求
    CSV_OUTPUT_DIR        = "/mydata/csv_repos"  // CSV输出目录
)
```

### pds_follows.go 配置
- **速率限制**：每秒10个API请求（3000/5分钟）
- **重试机制**：每个DID最多重试3次
- **批处理**：每100条记录显示一次进度

## 环境变量

设置BlueSky认证信息：

```bash
export BSKY_HANDLE="your.handle.bsky.social"
export BSKY_PASSWORD="your_password"
```

如果不设置，将使用默认值。

## 错误处理

- **API失败**：标记为`ERROR`或`-1`
- **速率限制**：自动等待并重试
- **网络错误**：最多重试3次
- **断点续传**：已处理的记录会跳过

## 性能优化

1. **速率限制**：遵守BlueSky API限制
2. **批处理**：每个文件间暂停5秒
3. **并发控制**：单线程处理避免超限
4. **断点续传**：支持中断后继续处理

## 监控和日志

程序会输出详细的进度信息：

```
=== 处理文件 1/20: repos_batch_01.csv ===
读取到 10000 条记录
已处理: 100/10000
已处理: 200/10000
...
✅ 完成文件: repos_batch_01.csv
暂停5秒...

🎉 所有CSV文件处理完成!
总用时: 2h30m15s
处理文件数: 20
总API请求数: 195678
成功获取followers: 189234
失败: 6444
```

## 故障排除

### 常见问题

1. **API认证失败**
   - 检查环境变量设置
   - 确认用户名密码正确

2. **速率限制**
   - 程序会自动处理，耐心等待
   - 可以调整速率限制参数

3. **CSV文件损坏**
   - 重新运行程序会自动修复
   - 支持部分处理的文件

4. **磁盘空间不足**
   - 确保 `/mydata` 有足够空间
   - 每个CSV文件约1-2MB

### 重新开始

如果需要完全重新开始：

```bash
# 清理CSV文件
rm -rf /mydata/csv_repos/*

# 重新运行主程序
go run main.go

# 重新处理followers
go run csv_followers_main.go pds_follows.go /mydata/csv_repos
```

## 数据质量

- **过滤条件**：只处理ScyllaDB中≥5条记录的repos
- **排序方式**：按record_count降序排列
- **数据完整性**：自动跳过无效的DID
- **一致性检查**：验证CSV文件格式

# PDS Checker (超大表优化版)

这是一个用于处理 ScyllaDB 中的 BlueSky records 超大表数据并在 PostgreSQL 中创建统计表的工具。

## 超大表优化特性

### 💡 内存优化
- **流式处理**: 避免将全部数据加载到内存
- **分批缓存**: 内存中只保存1000条记录的统计缓存
- **定期刷新**: 每5分钟或达到缓存限制时自动刷新数据

### 🚀 性能优化
- **Token分页**: 使用ScyllaDB的token范围进行高效分页
- **小批次处理**: ScyllaDB分页大小调整为100，避免超时
- **临时表**: 使用PostgreSQL临时表进行中间数据存储
- **批量写入**: 使用事务和批量INSERT优化写入性能

### ⏱️ 时间管理
- **48小时超时**: 为超大表处理预留充足时间
- **进度监控**: 每10秒更新处理进度
- **批次报告**: 每批次完成后显示详细统计

## 功能

1. **流式数据统计**: 从 ScyllaDB 的 `bluesky.records` 超大表中流式读取所有记录，统计每个 `repo`（DID）的记录数量
2. **临时表管理**: 创建PostgreSQL临时表 `temp_repo_stats` 进行中间数据存储
3. **表结构分析**: 分析 PostgreSQL 中现有的 `repos` 表结构
4. **优化JOIN创建**: 基于临时表和原 `repos` 表高效创建最终结果表 `repos_with_stats`
5. **数据筛选**: 只保留在 ScyllaDB 中有记录的 DIDs，删除没有记录的 DIDs

## ScyllaDB 表结构

程序处理的 `bluesky.records` 表结构：
```sql
CREATE TABLE bluesky.records (
    repo text,              -- 用户DID
    collection text,        -- 记录类型
    rkey text,             -- 记录键
    at_rev text,           -- 版本
    created_at timestamp,  -- 创建时间
    deleted boolean,       -- 删除标记
    record text,           -- 记录内容(JSON)
    PRIMARY KEY ((repo, collection), rkey, at_rev)
);
```

  ## PostgreSQL 表变化
  
  - **输入表**: `repos` (必须包含 `did` 和 `pds` 字段)
  - **临时表**: `temp_repo_stats` (用于中间数据存储，处理完成后自动删除)
  - **输出表**: `repos_with_stats` (包含关键字段：`pds`, `did`, `record_count`, `follows`)

## 环境要求

- Go 1.22+
- ScyllaDB 实例（默认端口 9042）
- PostgreSQL 实例（端口 15432）
- **充足的磁盘空间**（用于临时表存储）
- **建议至少4GB内存**（用于处理缓存）

## 配置

### 环境变量

```bash
# PostgreSQL 密码（如果不在代码中硬编码）
export POSTGRES_PASSWORD="你的密码"

# ScyllaDB 主机（可选，默认 127.0.0.1）
export SCYLLA_HOST="127.0.0.1"

# ScyllaDB 端口（可选，默认 9042）
export SCYLLA_PORT="9042"
```

### 数据库连接优化

- **ScyllaDB**: 
  - 主机: `SCYLLA_HOST` 环境变量或默认 `127.0.0.1`
  - 端口: `SCYLLA_PORT` 环境变量或默认 `9042`
  - Keyspace: `bluesky`
  - 一致性级别: `LocalQuorum` (优化性能)
  - 页面大小: 100 (避免超时)
  - 连接数: 2 (减少资源占用)
  
- **PostgreSQL**: 
  - 主机: `localhost`
  - 端口: `15432`
  - 数据库: `bluesky`
  - 用户: `postgres`
  - 连接池: 最大5个连接，2个空闲连接

## 使用方法

1. **确保数据库服务运行**:
   ```bash
   # 检查ScyllaDB和PostgreSQL是否运行
   docker ps
   
   # 确保有足够的磁盘空间（建议至少20GB可用空间）
   df -h
   ```

2. **运行程序**:
   ```bash
   cd DNET_BlueSky/downloader/bsky-indexer/cmd/pds_checker
   go run main.go
   ```

3. **或使用运行脚本**:
   ```bash
   ./run.sh
   ```

## 处理流程（超大表优化）

1. **连接数据库**: 
   - 连接到 ScyllaDB 和 PostgreSQL
   - 优化连接池和超时设置
   - 验证连接状态

2. **创建临时表**:
   - 创建 `temp_repo_stats` 临时表
   - 使用 `UNLOGGED` 类型提高写入性能
   - 创建索引优化查询

3. **流式统计 ScyllaDB 数据**:
   - 使用token范围进行分页查询
   - 每批处理100个分区，避免内存溢出
   - 流式处理每个分区内的记录
   - 在内存中缓存1000条统计数据
   - 每5分钟或达到缓存限制时刷新到临时表
   - 显示实时处理进度

4. **创建最终结果表**:
   - 基于临时表和原 `repos` 表执行高效JOIN
   - 只保留有记录的DIDs
   - 创建主键和索引优化查询
   - 清理临时表

## 输出信息

程序会显示以下信息：

- 数据库连接状态和优化设置
- 临时表创建状态
- 实时处理进度（每10秒更新）:
  - 已处理记录数
  - 发现的唯一repos数量
  - 当前批次数
- 批次完成报告（每批次）
- 缓存刷新通知（每5分钟）
- 最终处理统计:
  - 总处理记录数
  - 总处理批次数
  - 最终保留的repos数量
  - 平均处理速度
- 记录数统计示例（前10个，按记录数降序）

## 结果表结构

创建的 `repos_with_stats` 表包含：
- **pds** (来自原表) - PDS地址
- **did** (来自原表) - 用户标识符  
- **record_count** (新增字段 bigint) - 该 DID 在 ScyllaDB 中的记录数
- **follows** (预留字段 text) - 关注信息，当前为空值，保留用于后续扩展

**索引:**
- 主键：`did`
- 索引：`record_count DESC` (按记录数降序)
- 索引：`pds` (按PDS地址)
- 索引：`follows` (按关注信息)

这种精简的表结构专注于关键数据，便于统计分析和查询优化。

## 超大表处理策略

### 内存管理
- 使用流式处理，避免OOM
- 内存缓存限制在1000条记录
- 定期刷新缓存到磁盘

### 查询优化
- ScyllaDB: 使用token分页，小批次处理
- PostgreSQL: 使用临时表和高效JOIN
- 避免大量数据的内存操作

### 时间管理
- 48小时超时保护
- 分阶段处理，中间结果可恢复
- 详细的进度报告

## 错误处理

程序会自动检查并处理：

- 数据库连接状态和超时
- PostgreSQL 表结构（`repos` 表和 `did` 字段）
- 单个分区处理失败（继续处理其他分区）
- 临时表创建和清理
- 缓存刷新失败（自动重试）

## 性能优化细节

### ScyllaDB优化
- **Token分页**: 使用token范围而不是普通分页
- **小批次**: 每批100个分区，避免超时
- **LocalQuorum**: 降低一致性要求提高性能
- **连接池**: 限制连接数减少资源竞争

### PostgreSQL优化
- **临时表**: 使用UNLOGGED表提高写入性能
- **批量写入**: 使用事务和预编译语句
- **高效JOIN**: 一次性创建最终结果表
- **索引优化**: 创建必要的索引加速查询

### 内存优化
- **流式处理**: 避免加载全部数据到内存
- **缓存限制**: 内存中最多保存1000条统计
- **定期刷新**: 防止内存无限增长
- **垃圾回收**: 及时清理临时数据

## 注意事项

1. **时间预期**: 超大表处理可能需要**数小时到数十小时**
2. **磁盘空间**: 确保有足够空间存储临时表（建议20GB+）
3. **内存要求**: 建议至少4GB可用内存
4. **网络稳定**: 确保数据库连接稳定，避免长时间断线
5. **监控日志**: 密切关注处理进度和错误信息
6. **中断恢复**: 如果中断，需要重新开始（未实现断点续传）

## 故障排除

### 连接超时
- 检查网络连接稳定性
- 调整数据库超时设置
- 确认防火墙配置

### 内存不足
- 减少 `memoryBatchSize` 参数
- 增加系统内存
- 监控内存使用情况

### 磁盘空间不足
- 清理临时文件
- 扩展磁盘容量
- 监控磁盘空间

### 处理速度慢
- 检查数据库性能
- 调整批次大小
- 监控系统资源使用

### 数据不一致
- 检查ScyllaDB数据完整性
- 验证PostgreSQL表结构
- 重新运行处理流程

## 监控建议

```bash
# 监控处理进度
tail -f logs/pds_checker.log

# 监控系统资源
htop
iotop
df -h

# 监控数据库状态
docker stats
```

通过这些优化，程序可以高效处理包含数亿条记录的超大表数据。 