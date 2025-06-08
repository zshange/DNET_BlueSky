# main.go 详细处理流程说明

## 🏗️ 整体架构

main.go实现了一个**三阶段流水线处理**系统，专门用于处理超大规模BlueSky数据：

```
ScyllaDB Records → 内存缓存 → PostgreSQL临时表 → 最终统计表
     ↓              ↓           ↓              ↓
  流式读取        批量聚合    定期刷新        数据过滤
```

## 📊 第一阶段：ScyllaDB Records流式处理

### 🔄 **是否实时插入temp_repo_stats表？**

**答案：不是完全实时，采用批量缓存+定期刷新机制**

### 🚀 流式处理机制详解

#### 1. **Token分页遍历**
```go
// 使用ScyllaDB的token()函数进行分区遍历
SELECT DISTINCT repo, collection FROM bluesky.records 
WHERE token(repo, collection) > ? LIMIT 200
```

#### 2. **内存缓存策略**
```go
repoStatsCache := make(map[string]int64)  // 内存中临时缓存
memoryBatchSize: 2000                     // 缓存2000个repos后刷新
```

#### 3. **定期刷新触发条件**
```go
// 两个条件任一满足即触发刷新：
// 1. 时间条件：每3分钟
// 2. 容量条件：缓存达到2000个repos
if time.Since(lastFlushTime) > 3*time.Minute || len(repoStatsCache) >= 2000 {
    flushStatsToTemp(ctx, repoStatsCache)  // 批量写入数据库
}
```

#### 4. **批量数据库写入**
```go
// 🚀 高性能批量UPSERT
INSERT INTO temp_repo_stats (did, record_count) 
VALUES ($1, $2), ($3, $4), ..., ($n-1, $n)
ON CONFLICT (did) DO UPDATE SET 
record_count = temp_repo_stats.record_count + EXCLUDED.record_count
```

### 📈 性能优化措施

#### ScyllaDB连接优化
```go
cluster.PageSize = 500                   // 🚀 增加页面大小提高吞吐量
cluster.NumConns = 4                     // 🚀 增加连接数提高并发
cluster.MaxPreparedStmts = 1000          // 🚀 增加预处理语句缓存
cluster.Timeout = time.Second * 30       // 🚀 减少超时时间，快速失败
```

#### PostgreSQL连接优化
```go
postgresDB.SetMaxOpenConns(10)          // 🚀 增加最大连接数
postgresDB.SetMaxIdleConns(5)           // 🚀 增加空闲连接数
postgresDB.SetConnMaxIdleTime(30 * time.Minute) // 🚀 空闲连接超时
```

#### 批处理优化
```go
batchSize: 200                           // 🚀 ScyllaDB分页大小
memoryBatchSize: 2000                    // 🚀 内存批处理大小
刷新间隔: 3分钟                          // 🚀 减少刷新频率
```

## 📊 实时统计显示

### 🔍 已收集repos个数统计

#### 1. **实时进度显示**
```
🚀 实时进度: 处理记录: 1,234,567 | 发现repos: 45,678 | 已收集repos: 43,210 | 批次: 123
```

#### 2. **批次完成统计**
```
✅ 完成批次 123 | 处理记录: 1,234,567 | 发现repos: 45,678 | 已收集repos: 43,210 | 缓存repos: 1,500
```

#### 3. **刷新操作统计**
```
📊 已刷新统计数据到临时表 | 批次: 123 | 已收集repos: 43,210 | 缓存repos: 0
⚡ 批量插入 2000 repos，耗时: 1.23s
```

#### 4. **最终统计报告**
```
📊 临时表中已收集repos个数: 1,234,567

📈 Record Count分布统计:
记录数范围    Repos数量
-------------------------
= 1        456,789
2-10       234,567
11-100     345,678
101-1000   123,456
> 1000     74,077

✅ 完成ScyllaDB数据处理!
总用时: 2h34m56s
处理记录数: 12,345,678
发现唯一repos: 1,234,567
已收集repos总数: 1,234,567
将被过滤的repos(record_count=1): 456,789
处理批次数: 6,172
平均处理速度: 1,345.67 记录/秒
```

## 🔄 第二阶段：PDS和Followers信息补充

### 📋 处理流程
1. **表结构检查和更新**：自动添加`pds`和`followers`字段
2. **PDS信息查询**：从`repos`表查询对应的PDS服务器
3. **Followers API调用**：通过BlueSky API获取实时followers数量
4. **速率限制控制**：严格按照3000/5分钟限制

## 🎯 第三阶段：最终表创建和数据过滤

### 🔥 关键过滤策略
```sql
-- 🔥 重要：过滤掉record_count=1的记录，只保留活跃用户数据
CREATE TABLE repos_with_stats AS 
SELECT 
    COALESCE(t.pds, r.pds) as pds,
    t.did,
    COALESCE(t.record_count, 0) as record_count,
    t.followers
FROM temp_repo_stats t
LEFT JOIN repos r ON t.did = r.did
WHERE t.record_count > 1;  -- 🔥 关键修改：过滤掉record_count=1的记录
```

### 📊 数据过滤统计
```
📊 数据过滤统计:
  临时表总repos数: 1,234,567
  过滤掉的repos数(record_count=1): 456,789 (37.02%)
  最终保留repos数: 777,778 (62.98%)
```

## 🚀 性能特性总结

### ✅ 优化亮点
1. **流式处理**：避免内存溢出，支持无限大表
2. **批量操作**：减少数据库I/O，提高吞吐量
3. **智能缓存**：平衡内存使用和写入频率
4. **并发优化**：多连接并行处理
5. **实时监控**：详细的进度和性能统计

### 📈 性能指标
- **处理速度**：1,000+ 记录/秒
- **内存使用**：固定上限（2000 repos缓存）
- **数据库写入**：批量UPSERT，单次2000条记录
- **网络优化**：连接池复用，预处理语句缓存

### 🔧 可调参数
```go
batchSize: 200           // ScyllaDB分页大小
memoryBatchSize: 2000    // 内存缓存大小
刷新间隔: 3分钟          // 数据库写入频率
连接数: 4               // ScyllaDB连接数
超时时间: 30秒          // 查询超时
```

这个设计确保了在处理TB级别数据时的高性能和稳定性，同时提供了详细的进度监控和统计信息。 