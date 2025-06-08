# BlueSky Record Blob Downloader

基于 `record-indexer` 架构的高性能用户内容爬取服务，用于从BlueSky网络下载用户档案和记录数据。**完全基于文件系统，无需数据库**。

## 🎯 功能特性

### 📊 数据获取
- **CSV文件处理**: 自动扫描 `/mydata/csv` 目录中的CSV文件，读取用户DID列表
- **用户档案下载**: 获取用户profile信息（followers、关注数、帖子数等）
- **记录下载**: 对followers > 20的用户，下载完整的历史记录仓库
- **智能去重**: 基于文件系统避免重复处理已下载的用户

### 🗂️ 数据存储 (文件系统)
- **分用户存储**: 每个用户一个文件夹 `/mydata/records/{did}/`
- **JSON格式**: 所有数据以结构化JSON格式存储
- **状态跟踪**: 基于文件系统的状态管理，无需数据库
- **元数据管理**: 统计信息和用户状态以JSON文件形式存储

### 🚀 性能优化
- **并发处理**: 可配置的工作池（默认10个worker）
- **智能限速**: 分层限速机制，避免触发API限制
- **错误处理**: 自动重试和错误恢复机制
- **监控指标**: 完整的Prometheus监控指标
- **零依赖**: 无需数据库，仅依赖文件系统

## 📁 文件结构

```
/mydata/
├── csv/              # CSV输入文件目录
│   ├── users_1.csv   # 包含did字段的CSV文件
│   └── users_2.csv
└── records/          # 输出目录
    ├── .status/      # 状态管理目录
    │   └── stats.json # 全局统计信息
    ├── did_com_example_user1/
    │   ├── profile.json      # 用户档案数据
    │   ├── profile_meta.json # 用户元数据和状态
    │   └── records.json      # 用户记录 (仅当followers>20)
    └── did_com_example_user2/
        ├── profile.json
        └── profile_meta.json
```

## 📊 状态管理 (无数据库)

### 用户元数据 (profile_meta.json)
```json
{
  "did": "did:plc:example",
  "handle": "user.bsky.social",
  "display_name": "User Name",
  "followers_count": 150,
  "follows_count": 89,
  "posts_count": 45,
  "records_count": 245,
  "status": "completed",  // pending, processing, completed, failed, deleted
  "last_error": "",
  "failed_attempts": 0,
  "last_processed": "2024-01-01T12:00:00Z",
  "created_at": "2024-01-01T10:00:00Z",
  "updated_at": "2024-01-01T12:00:00Z"
}
```

### 全局统计 (.status/stats.json)
```json
{
  "total_users": 1000,
  "processed_users": 850,
  "completed_users": 800,
  "failed_users": 30,
  "deleted_users": 20,
  "total_records": 125000,
  "users_with_records": 600,
  "last_processed_file": "users_batch_3.csv",
  "last_processed_did": "did:plc:example",
  "start_time": "2024-01-01T09:00:00Z",
  "last_update_time": "2024-01-01T12:00:00Z"
}
```

## 🚀 快速开始

### 1. 环境准备
```bash
# 无需数据库！只需要文件系统
# 确保/mydata/csv目录存在并包含CSV文件

# CSV文件格式示例:
# did,repos
# did:plc:example123,1500
# did:plc:example456,2300
```

### 2. 启动服务
```bash
cd DNET_BlueSky/downloader/bsky-indexer/cmd/record_blob_downloader
chmod +x run.sh
./run.sh
```

### 3. 监控服务
- **Prometheus监控**: http://localhost:8080/metrics
- **健康检查**: http://localhost:8080/health
- **实时统计**: http://localhost:8080/stats

## ⚙️ 配置选项

### 环境变量
```bash
export DOWNLOADER_WORKERS=10                   # 工作线程数
export DOWNLOADER_CSV_DIR=/mydata/csv          # CSV输入目录
export DOWNLOADER_RECORDS_DIR=/mydata/records  # 数据输出目录
export DOWNLOADER_METRICS_PORT=8080            # 监控端口
export BSKY_HANDLE="your.handle"               # BlueSky账号
export BSKY_PASSWORD="your_password"           # BlueSky密码
```

### 命令行参数
```bash
./record_blob_downloader \
    -workers=10 \
    -csv-dir=/mydata/csv \
    -records-dir=/mydata/records \
    -log-level=1
```

## 🔧 管理API

### 查看实时统计
```bash
curl http://localhost:8080/stats
```
返回完整的处理统计信息，包括用户数量、处理状态等。

### 调整工作池大小
```bash
curl "http://localhost:8080/pool/resize?size=15"
```

### 调整限速设置
```bash
# Profile请求限制 (1-100 requests/second)
curl "http://localhost:8080/rate/profile/set?limit=15"

# Repo请求限制 (1-50 requests/second)  
curl "http://localhost:8080/rate/repo/set?limit=5"

# 全局请求限制 (1-100 requests/second)
curl "http://localhost:8080/rate/global/set?limit=10"
```

### 查看限速状态
```bash
curl "http://localhost:8080/rate/status"
```

## 📊 监控指标

### 关键指标
- `downloader_queue_length`: 队列长度
- `downloader_users_processed_total`: 已处理用户数
- `downloader_profiles_downloaded_total`: 已下载档案数
- `downloader_records_downloaded_total`: 已下载记录数
- `downloader_worker_pool_size`: 工作池大小
- `downloader_errors_total`: 错误计数

## 🔄 工作流程

1. **扫描CSV**: 每2分钟扫描一次CSV目录，发现新用户
2. **状态检查**: 基于文件系统检查用户是否已处理
3. **队列管理**: 将新用户加入处理队列（最大10,000个）
4. **用户处理**: 
   - 获取用户profile
   - 如果followers > 20，下载完整记录
   - 保存到本地文件系统
   - 更新状态文件
5. **错误处理**: 不可访问的用户标记为删除状态

## ⚠️ 限制说明

### 速率限制
- **Profile请求**: 默认10 requests/second
- **Repo请求**: 默认10 requests/second
- **全局请求**: 默认10 requests/second

### 数据筛选
- 只有followers > 20的用户才会下载完整记录
- 不可访问的用户会被自动删除

## 🛠️ 故障排除

### 常见问题
1. **认证失败**: 检查BSKY_HANDLE和BSKY_PASSWORD环境变量
2. **文件权限**: 确保对/mydata目录有读写权限
3. **限速触发**: 降低请求速率或等待重置时间
4. **磁盘空间不足**: 监控/mydata/records目录大小

### 日志级别
- `-1`: TRACE - 最详细的调试信息
- `0`: DEBUG - 调试信息
- `1`: INFO - 一般信息 (默认)
- `5`: PANIC - 仅致命错误

## 📈 性能建议

1. **调整worker数量**: 根据网络和CPU性能调整
2. **监控限速**: 通过metrics观察请求成功率
3. **磁盘优化**: 使用SSD存储提高IO性能
4. **定期清理**: 清理不需要的用户数据释放空间

## 🌟 优势特性

### 无数据库设计
- **零依赖**: 无需安装配置数据库
- **简单部署**: 仅需文件系统即可运行
- **数据透明**: 所有数据以JSON格式存储，易于查看和备份
- **故障恢复**: 重启后自动从文件系统恢复状态

### 数据持久性
- **原子操作**: 每个用户的数据独立存储
- **状态一致性**: 基于文件存在性判断处理状态
- **备份友好**: 直接复制目录即可备份所有数据

### 扩展性
- **水平扩展**: 可通过分区目录支持多实例
- **渐进迁移**: 需要时可轻松迁移到数据库
- **数据分析**: JSON格式便于后续数据分析和处理 