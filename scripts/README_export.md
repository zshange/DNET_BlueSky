# ScyllaDB 数据分批导出工具

## 概述

为了解决224GB ScyllaDB数据在100GB剩余空间限制下的导出问题，提供了三个导出脚本：

1. **scylla_simple_export.sh** - 🔧 简化导出脚本（推荐）
2. **scylla_advanced_export.sh** - 🚀 高级导出脚本（支持断点续传）
3. **scylla_batch_export.sh** - 📦 基础导出脚本

## 推荐方案：使用简化导出脚本

### 快速开始

```bash
cd /users/sbwang/DNET_BlueSky/scripts

# 查看帮助
./scylla_simple_export.sh --help

# 开始第1批次导出（使用COPY方法）
./scylla_docker_export.sh 1 sstable

# 开始第2批次导出（使用SSTable方法，推荐）
./scylla_docker_export.sh 2 sstable

# 开始第3批次导出（使用Token方法）
./scylla_docker_export.sh 3 sstable
```

### 三种导出方法对比

| 方法 | 特点 | 速度 | 文件格式 | 适用场景 |
|------|------|------|----------|----------|
| **copy** | 使用COPY命令导出CSV | 中等 | CSV通用格式 | 中小数据量，跨平台兼容 |
| **sstable** | 快照+SSTable文件 | 最快 | ScyllaDB专用 | 大数据量，同版本迁移 |
| **token** | Token范围分割 | 较快 | CSV格式 | 精确控制数据范围 |

### 导出配置

默认配置参数：
- **批次大小**: 100GB per批次
- **文件大小**: 10GB per文件  
- **导出目录**: `/mydata/scy`
- **数据库**: `bluesky.records`

## 操作步骤

### 1. 环境检查

脚本会自动检查：
- ✅ ScyllaDB连接状态
- ✅ 磁盘空间（至少120GB可用）
- ✅ 必要工具（cqlsh, nodetool, tar, split）

### 2. 分批导出流程

#### 第一批次（推荐使用SSTable方法）

```bash
# 创建导出目录
sudo mkdir -p /mydata/scy
sudo chown $USER:$USER /mydata/scy

# 导出第1批次
./scylla_simple_export.sh 1 sstable
```

#### 等待第一批次完成后

```bash
# 检查导出结果
ls -lh /mydata/scy/batch_1_sstable/
du -sh /mydata/scy/batch_1_sstable/

# 可选：压缩导出文件
cd /mydata/scy
tar -czf batch_1_sstable.tar.gz batch_1_sstable/

# 传输到其他服务器后，删除本地文件
rm -rf batch_1_sstable/

# 继续第2批次
./scylla_simple_export.sh 2 sstable
```

### 3. 目录结构

导出完成后的目录结构：
```
/mydata/scy/
├── batch_1_sstable/
│   ├── BATCH_INFO.txt          # 批次信息摘要
│   ├── *.db                    # SSTable数据文件
│   ├── *.Filter.db             # 布隆过滤器文件
│   ├── *.Index.db              # 索引文件
│   └── *.Statistics.db         # 统计信息文件
├── batch_2_copy/
│   ├── BATCH_INFO.txt
│   ├── records_part_1.csv      # CSV数据文件
│   ├── records_part_2.csv
│   └── ...
└── .export_state               # 导出状态文件（高级脚本）
```

## 高级功能

### 断点续传（使用高级脚本）

```bash
# 自动从上次中断位置继续
./scylla_advanced_export.sh auto true

# 监控导出进度
./scylla_advanced_export.sh monitor
```

### 文件大小控制

如果单个文件超过10GB，脚本会自动分割：
- CSV文件：按行数分割，保持header
- SSTable文件：按文件大小分组到子目录

### 数据验证

每个批次完成后会生成`BATCH_INFO.txt`文件，包含：
- 📊 文件统计信息
- 🔍 验证命令
- 📝 恢复命令示例

## 在目标服务器上恢复数据

### 方法1：CSV文件导入

```bash
# 创建keyspace和table
cqlsh -H target_host -e "
CREATE KEYSPACE IF NOT EXISTS bluesky 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE bluesky;
CREATE TABLE IF NOT EXISTS records (
    repo text,
    collection text,
    rkey text,
    at_rev bigint,
    created_at timestamp,
    deleted boolean,
    record text,
    PRIMARY KEY ((repo, collection), rkey, at_rev)
);"

# 导入CSV数据
for csv_file in /path/to/csv/*.csv; do
    cqlsh -H target_host -e "
    COPY bluesky.records FROM '$csv_file' 
    WITH DELIMITER='|' AND HEADER=true;"
done
```

### 方法2：SSTable文件导入

```bash
# 使用sstableloader导入
sstableloader -d target_host:9042 /path/to/sstable/files/

# 或者直接复制SSTable文件到数据目录（需要停止ScyllaDB）
sudo systemctl stop scylla-server
sudo cp -r /path/to/sstable/* /var/lib/scylla/data/bluesky/records/
sudo chown -R scylla:scylla /var/lib/scylla/data/bluesky/
sudo systemctl start scylla-server
```

## 监控和故障排除

### 磁盘空间监控

```bash
# 实时监控磁盘使用情况
watch -n 10 'df -h /mydata'

# 监控导出目录大小
watch -n 30 'du -sh /mydata/scy/*'
```

### 进程监控

```bash
# 查看CQL进程
ps aux | grep cqlsh

# 查看导出脚本进程
ps aux | grep scylla_.*_export

# 查看系统负载
htop
```

### 常见问题

1. **磁盘空间不足**
   ```bash
   # 清理临时文件
   rm -f /tmp/copy_export_*.cql
   rm -f /tmp/export_*.cql
   
   # 压缩并转移已完成的批次
   tar -czf batch_N.tar.gz batch_N/
   rm -rf batch_N/
   ```

2. **连接超时**
   ```bash
   # 增加超时时间（在脚本中修改timeout值）
   timeout 7200 cqlsh ...  # 2小时超时
   ```

3. **内存不足**
   ```bash
   # 减少每次导出的行数
   MAX_ROWS_PER_FILE=2000000  # 在脚本中修改
   ```

## 性能优化建议

1. **使用SSTable方法**：对于大数据量，SSTable方法速度最快
2. **并行处理**：在不同服务器上同时运行多个批次
3. **网络传输**：使用rsync或scp的压缩选项传输文件
4. **压缩存储**：使用gzip压缩减少存储空间

## 安全注意事项

- 🔒 确保导出目录权限正确
- 🔐 传输过程中使用加密连接
- 🗑️ 及时清理临时文件和CQL脚本
- 📋 验证导出数据的完整性

## 联系信息

如有问题请联系：shange0403@gmail.com 