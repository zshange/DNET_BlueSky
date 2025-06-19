package main

import (
    "context"
    "database/sql"
    "encoding/csv"
    "fmt"
    "os"
    "path/filepath"
    "sync"
    "time"
    "log"
    "strings"

    "github.com/gocql/gocql"
    "github.com/scylladb/gocqlx/v3"
    _ "github.com/lib/pq"
)

// 统计计数器
type ProgressCounter struct {
    TotalRecords     int64
    ProcessedRecords int64
    UniqueRepos      int64
    ValidRepos       int64
    FilteredRepos    int64  // 被过滤掉的repos数量(record_count=1)
    CollectedRepos   int64  // 已收集到临时表的repos数量
    BatchesProcessed int64
    CreatedNewTable  bool
}

// PDS检查器结构体
type PDSChecker struct {
    scyllaSession    *gocqlx.Session
    postgresDB       *sql.DB
    batchSize        int
    memoryBatchSize  int  // 内存中批处理大小，避免OOM
    counter          *ProgressCounter
    mutex            sync.RWMutex
}

// 临时文件处理的repo统计
type RepoStat struct {
    DID         string
    RecordCount int64
}

var contactInfo = "shange0403@gmail.com"

// 配置常量
const (
    TARGET_RECORD_COUNT   = 200000  // 目标记录数
    CSV_FILES_COUNT       = 20      // CSV文件数量
    RECORDS_PER_CSV       = 10000   // 每个CSV文件的记录数
    MIN_RECORDS_REQUIRED  = 5       // 最小记录数要求
    CSV_OUTPUT_DIR        = "/mydata/csv_repos"  // CSV输出目录
)

func NewPDSChecker(scyllaSession *gocqlx.Session, postgresDB *sql.DB) *PDSChecker {
    return &PDSChecker{
        scyllaSession:   scyllaSession,
        postgresDB:      postgresDB,
        batchSize:       200,   // 🚀 增加ScyllaDB分页大小提高吞吐量
        memoryBatchSize: 5000,  // 🚀 增加内存批处理大小，减少数据库写入频率
        counter:         &ProgressCounter{},
    }
}

// 流式处理ScyllaDB records表数据并分批统计
func (p *PDSChecker) ProcessRecordsStreaming(ctx context.Context) error {
    fmt.Println("开始流式处理ScyllaDB中的records数据...")
    startTime := time.Now()

    // 创建临时表存储repo统计
    if err := p.createTempStatsTable(ctx); err != nil {
        return fmt.Errorf("failed to create temp stats table: %w", err)
    }

    type row struct {
        Repo       string
        Collection string
        Rkey       string
        AtRev      string
        CreatedAt  time.Time
        Deleted    bool
        Record     string
    }

    // 内存中的临时统计缓存
    repoStatsCache := make(map[string]int64)
    lastFlushTime := time.Now()
    
    // 使用token分页处理所有记录
    var lastToken int64 = -9223372036854775808  // 从最小token开始
    for {
        // 构建token范围查询 - 更小的范围避免超时
        partitionQuery := "SELECT DISTINCT repo, collection FROM bluesky.records WHERE token(repo, collection) > ? LIMIT ?"
        partitionValues := []interface{}{lastToken, p.batchSize}

        // 执行分区查询
        partitionStmt := p.scyllaSession.Session.Query(partitionQuery, partitionValues...)
        partitionStmt.WithContext(ctx)
        partitionIter := partitionStmt.Iter()

        var currentRepo, currentCollection string
        partitionsInBatch := 0
        var currentToken int64

        // 遍历每个分区
        for partitionIter.Scan(&currentRepo, &currentCollection) {
            partitionsInBatch++
            
            // 计算当前分区的token
            currentToken = calculateToken(currentRepo, currentCollection)

            // 流式处理当前分区内的记录
            if err := p.processPartitionStreaming(ctx, currentRepo, currentCollection, repoStatsCache); err != nil {
                fmt.Printf("警告: 处理分区失败 %s:%s - %v\n", currentRepo, currentCollection, err)
                continue
            }

            // 🚀 优化刷新策略：减少刷新频率，增加批量大小
            if time.Since(lastFlushTime) > 1*time.Minute || len(repoStatsCache) >= 2000 {
                // 获取当前临时表repos数量
                var currentCollected int64
                countQuery := "SELECT COUNT(*) FROM temp_repo_stats WHERE record_count >= " + fmt.Sprintf("%d", MIN_RECORDS_REQUIRED)
                if err := p.postgresDB.QueryRowContext(ctx, countQuery).Scan(&currentCollected); err == nil {
                    p.counter.CollectedRepos = currentCollected
                }
                
                if err := p.flushStatsToTemp(ctx, repoStatsCache); err != nil {
                    return fmt.Errorf("failed to flush stats: %w", err)
                }
                
                // 更新收集数量并检查是否达到目标
                if err := p.postgresDB.QueryRowContext(ctx, countQuery).Scan(&currentCollected); err == nil {
                    p.counter.CollectedRepos = currentCollected
                    
                    // 检查是否达到目标记录数
                    if currentCollected >= TARGET_RECORD_COUNT {
                        fmt.Printf("\n🎯 已达到目标记录数 %d，准备停止并导出CSV...\n", TARGET_RECORD_COUNT)
                        return nil  // 提前退出循环
                    }
                }
                
                repoStatsCache = make(map[string]int64) // 清空缓存
                lastFlushTime = time.Now()
                fmt.Printf("📊 已刷新统计数据到临时表 | 批次: %d | 已收集repos: %d | 缓存repos: %d | 目标: %d\n", 
                          p.counter.BatchesProcessed, p.counter.CollectedRepos, len(repoStatsCache), TARGET_RECORD_COUNT)
            }
        }

        if err := partitionIter.Close(); err != nil {
            return fmt.Errorf("error closing partition iterator: %w", err)
        }

        // 更新lastToken
        if partitionsInBatch > 0 {
            lastToken = currentToken
        }

        // 如果这批次分区数小于批次大小，说明已经处理完所有分区
        if partitionsInBatch < p.batchSize {
            break
        }

        p.counter.BatchesProcessed++
        
        // 🚀 增强批次处理信息显示
        fmt.Printf("✅ 完成批次 %d | 处理记录: %d | 发现repos: %d | 已收集repos: %d | 缓存repos: %d\n", 
                  p.counter.BatchesProcessed, p.counter.ProcessedRecords, p.counter.UniqueRepos, 
                  p.counter.CollectedRepos, len(repoStatsCache))
    }

    // 最后一次刷新剩余的缓存
    if len(repoStatsCache) > 0 {
        fmt.Printf("🔄 刷新最后 %d 个缓存repos到临时表...\n", len(repoStatsCache))
        if err := p.flushStatsToTemp(ctx, repoStatsCache); err != nil {
            return fmt.Errorf("failed to flush final stats: %w", err)
        }
    }

    // 获取最终临时表中的repos统计信息（符合条件的）
    var tempReposCount int64
    tempCountQuery := "SELECT COUNT(*) FROM temp_repo_stats WHERE record_count >= " + fmt.Sprintf("%d", MIN_RECORDS_REQUIRED)
    if err := p.postgresDB.QueryRowContext(ctx, tempCountQuery).Scan(&tempReposCount); err != nil {
        fmt.Printf("警告: 无法获取临时表repos统计: %v\n", err)
    } else {
        p.counter.CollectedRepos = tempReposCount
        fmt.Printf("\n📊 临时表中已收集repos个数(≥%d条记录): %d\n", MIN_RECORDS_REQUIRED, tempReposCount)
    }

    // 显示record_count分布统计
    fmt.Println("\n📈 Record Count分布统计:")
    distributionQuery := `
        SELECT 
            CASE 
                WHEN record_count = 1 THEN '= 1'
                WHEN record_count BETWEEN 2 AND 10 THEN '2-10'
                WHEN record_count BETWEEN 11 AND 100 THEN '11-100'
                WHEN record_count BETWEEN 101 AND 1000 THEN '101-1000'
                WHEN record_count > 1000 THEN '> 1000'
                ELSE 'unknown'
            END as range,
            COUNT(*) as count
        FROM temp_repo_stats 
        GROUP BY 
            CASE 
                WHEN record_count = 1 THEN '= 1'
                WHEN record_count BETWEEN 2 AND 10 THEN '2-10'
                WHEN record_count BETWEEN 11 AND 100 THEN '11-100'
                WHEN record_count BETWEEN 101 AND 1000 THEN '101-1000'
                WHEN record_count > 1000 THEN '> 1000'
                ELSE 'unknown'
            END
        ORDER BY 
            CASE 
                WHEN record_count = 1 THEN 1
                WHEN record_count BETWEEN 2 AND 10 THEN 2
                WHEN record_count BETWEEN 11 AND 100 THEN 3
                WHEN record_count BETWEEN 101 AND 1000 THEN 4
                WHEN record_count > 1000 THEN 5
                ELSE 6
            END
    `
    
    distRows, err := p.postgresDB.QueryContext(ctx, distributionQuery)
    if err == nil {
        defer distRows.Close()
        fmt.Printf("%-10s %s\n", "记录数范围", "Repos数量")
        fmt.Println(strings.Repeat("-", 25))
        for distRows.Next() {
            var rangeStr string
            var count int64
            if err := distRows.Scan(&rangeStr, &count); err == nil {
                fmt.Printf("%-10s %d\n", rangeStr, count)
                if rangeStr == "= 1" {
                    p.counter.FilteredRepos = count
                }
            }
        }
    }

    // 打印统计信息
    duration := time.Since(startTime)
    fmt.Printf("\n✅ 完成ScyllaDB数据处理!\n")
    fmt.Printf("总用时: %v\n", duration)
    fmt.Printf("处理记录数: %d\n", p.counter.ProcessedRecords)
    fmt.Printf("发现唯一repos: %d\n", p.counter.UniqueRepos)
    fmt.Printf("已收集repos总数: %d\n", p.counter.CollectedRepos)
    fmt.Printf("将被过滤的repos(record_count=1): %d\n", p.counter.FilteredRepos)
    fmt.Printf("处理批次数: %d\n", p.counter.BatchesProcessed)
    fmt.Printf("平均处理速度: %.2f 记录/秒\n", float64(p.counter.ProcessedRecords)/duration.Seconds())

    return nil
}

// 流式处理单个分区
func (p *PDSChecker) processPartitionStreaming(ctx context.Context, repo, collection string, cache map[string]int64) error {
    recordQuery := "SELECT repo, collection, rkey, at_rev, created_at, deleted, record FROM bluesky.records WHERE repo = ? AND collection = ?"
    recordStmt := p.scyllaSession.Session.Query(recordQuery, repo, collection)
    recordStmt.WithContext(ctx)
    recordIter := recordStmt.Iter()

    var r struct {
        Repo       string
        Collection string
        Rkey       string
        AtRev      string
        CreatedAt  time.Time
        Deleted    bool
        Record     string
    }
    
    lastProgressUpdate := time.Now()
    currentRepoCount := int64(0)

    // 流式处理当前分区内的所有记录
    for recordIter.Scan(&r.Repo, &r.Collection, &r.Rkey, &r.AtRev, &r.CreatedAt, &r.Deleted, &r.Record) {
        p.counter.ProcessedRecords++
        currentRepoCount++

        // 每10秒更新一次进度
        if time.Since(lastProgressUpdate) > 10*time.Second {
            p.printProgress()
            lastProgressUpdate = time.Now()
        }
    }

    // 更新缓存中的repo统计 - 只统计符合最小记录数要求的repo
    if currentRepoCount >= MIN_RECORDS_REQUIRED {
        if _, exists := cache[repo]; !exists {
            p.counter.UniqueRepos++
        }
        cache[repo] += currentRepoCount
    }

    return recordIter.Close()
}

// 创建临时统计表
func (p *PDSChecker) createTempStatsTable(ctx context.Context) error {
    createTempTableQuery := `
        DROP TABLE IF EXISTS temp_repo_stats;
        CREATE UNLOGGED TABLE temp_repo_stats (
            did TEXT PRIMARY KEY,
            record_count BIGINT DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_temp_repo_stats_count ON temp_repo_stats(record_count);
    `
    
    _, err := p.postgresDB.ExecContext(ctx, createTempTableQuery)
    return err
}

// 🚀 将缓存的统计数据高效刷新到临时表
func (p *PDSChecker) flushStatsToTemp(ctx context.Context, cache map[string]int64) error {
    if len(cache) == 0 {
        return nil
    }

    startTime := time.Now()
    
    // 🚀 使用批量UPSERT进行高效插入
    tx, err := p.postgresDB.BeginTx(ctx, nil)
    if err != nil {
        return err
    }
    defer tx.Rollback()

    // 🚀 构建批量VALUES语句，提高插入性能
    var values []string
    var args []interface{}
    argIndex := 1
    
    for did, count := range cache {
        values = append(values, fmt.Sprintf("($%d, $%d)", argIndex, argIndex+1))
        args = append(args, did, count)
        argIndex += 2
    }
    
    batchQuery := fmt.Sprintf(`
        INSERT INTO temp_repo_stats (did, record_count) 
        VALUES %s
        ON CONFLICT (did) DO UPDATE SET 
        record_count = temp_repo_stats.record_count + EXCLUDED.record_count
    `, strings.Join(values, ", "))
    
    if _, err := tx.ExecContext(ctx, batchQuery, args...); err != nil {
        return fmt.Errorf("批量插入失败: %w", err)
    }

    if err := tx.Commit(); err != nil {
        return fmt.Errorf("提交事务失败: %w", err)
    }
    
    duration := time.Since(startTime)
    fmt.Printf("⚡ 批量插入 %d repos，耗时: %v\n", len(cache), duration)
    
    return nil
}

// 计算token值（简化版本）
func calculateToken(repo, collection string) int64 {
    // 这是一个简化的token计算，实际应该使用Cassandra的murmur3 hash
    key := repo + ":" + collection
    hash := int64(0)
    for _, c := range key {
        hash = hash*31 + int64(c)
    }
    return hash
}

// 基于临时表创建最终的repos_with_stats表
func (p *PDSChecker) CreateRepoStatsTableFromTemp(ctx context.Context) error {
    fmt.Println("开始基于临时统计表创建最终结果表...")
    startTime := time.Now()

    // 1. 检查原repos表是否存在
    var tableExists bool
    checkTableQuery := `SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = 'repos'
    )`
    
    if err := p.postgresDB.QueryRowContext(ctx, checkTableQuery).Scan(&tableExists); err != nil {
        return fmt.Errorf("failed to check if repos table exists: %w", err)
    }
    
    if !tableExists {
        return fmt.Errorf("原始repos表不存在，无法继续操作")
    }

    // 1.5. 在创建最终表之前，先处理PDS和Followers信息
    fmt.Println("\n=== 第二阶段：处理PDS和Followers信息 ===")
    fmt.Println("⚠️  PDS和Followers处理暂时跳过（需要同时运行pds_follows.go）")
    // TODO: 运行时使用: go run main.go pds_follows.go
    /*
    followsProcessor := NewPDSFollowsProcessor(p.postgresDB)
    if err := followsProcessor.ProcessDIDsWithFollows(ctx); err != nil {
        fmt.Printf("警告: PDS和Followers处理失败: %v\n", err)
        fmt.Println("继续创建最终表，但可能缺少PDS和Followers信息...")
    } else {
        fmt.Println("✓ PDS和Followers信息处理完成")
    }
    */

    // 2. 检查repos表是否包含必要字段
    fmt.Println("检查repos表字段...")
    var hasDidColumn, hasRpdsColumn bool
    
    checkColumnsQuery := `
        SELECT column_name
        FROM information_schema.columns 
        WHERE table_schema = 'public' 
        AND table_name = 'repos'
        AND column_name IN ('did', 'pds')
    `
    
    rows, err := p.postgresDB.QueryContext(ctx, checkColumnsQuery)
    if err != nil {
        return fmt.Errorf("failed to check repos table columns: %w", err)
    }
    defer rows.Close()
    
    for rows.Next() {
        var columnName string
        if err := rows.Scan(&columnName); err != nil {
            continue
        }
        if columnName == "did" {
            hasDidColumn = true
            fmt.Printf("  ✓ 找到字段: %s\n", columnName)
        } else if columnName == "pds" {
            hasRpdsColumn = true
            fmt.Printf("  ✓ 找到字段: %s\n", columnName)
        }
    }
    
    if !hasDidColumn {
        return fmt.Errorf("repos表中没有找到did字段")
    }
    if !hasRpdsColumn {
        return fmt.Errorf("repos表中没有找到rpds字段")
    }

    // 3. 创建最终结果表（包含 pds、did、record_count、followers 四个字段）
    fmt.Println("创建最终结果表 repos_with_stats (包含 pds, did, record_count, followers)...")
    createFinalTableQuery := `
        DROP TABLE IF EXISTS repos_with_stats;
        
        CREATE TABLE repos_with_stats AS 
        SELECT 
            COALESCE(t.pds, r.pds) as pds,
            t.did,
            COALESCE(t.record_count, 0) as record_count,
            t.followers
        FROM temp_repo_stats t
        LEFT JOIN repos r ON t.did = r.did
        WHERE t.record_count >= 5;
        
        ALTER TABLE repos_with_stats 
        ADD CONSTRAINT repos_with_stats_pkey PRIMARY KEY (did);
        
        CREATE INDEX IF NOT EXISTS idx_repos_with_stats_count 
        ON repos_with_stats(record_count DESC);
        
        CREATE INDEX IF NOT EXISTS idx_repos_with_stats_pds 
        ON repos_with_stats(pds);
        
        CREATE INDEX IF NOT EXISTS idx_repos_with_stats_followers 
        ON repos_with_stats(followers);
    `
    
    if _, err := p.postgresDB.ExecContext(ctx, createFinalTableQuery); err != nil {
        return fmt.Errorf("failed to create final repos_with_stats table: %w", err)
    }
    p.counter.CreatedNewTable = true

    // 4. 获取最终统计
    var finalCount int64
    countQuery := "SELECT COUNT(*) FROM repos_with_stats"
    if err := p.postgresDB.QueryRowContext(ctx, countQuery).Scan(&finalCount); err != nil {
        return fmt.Errorf("failed to get final count: %w", err)
    }
    p.counter.ValidRepos = finalCount

    // 5. 验证表结构
    fmt.Println("验证新表结构...")
    verifyQuery := `
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema = 'public' 
        AND table_name = 'repos_with_stats'
        ORDER BY ordinal_position
    `
    
    verifyRows, err := p.postgresDB.QueryContext(ctx, verifyQuery)
    if err == nil {
        defer verifyRows.Close()
        fmt.Println("新表字段:")
        for verifyRows.Next() {
            var columnName, dataType string
            if err := verifyRows.Scan(&columnName, &dataType); err == nil {
                fmt.Printf("  - %s (%s)\n", columnName, dataType)
            }
        }
    }

    // 6. 清理临时表
    _, err = p.postgresDB.ExecContext(ctx, "DROP TABLE IF EXISTS temp_repo_stats")
    if err != nil {
        fmt.Printf("警告: 清理临时表失败: %v\n", err)
    }

    // 打印结果
    duration := time.Since(startTime)
    fmt.Printf("\n完成PostgreSQL处理!\n")
    fmt.Printf("总用时: %v\n", duration)
    fmt.Printf("最终保留的repos数: %d\n", p.counter.ValidRepos)

    // 显示统计示例
    fmt.Println("\n记录数统计示例（前10个）:")
    sampleRows, err := p.postgresDB.QueryContext(ctx, `
        SELECT pds, did, record_count, followers 
        FROM repos_with_stats 
        ORDER BY record_count DESC 
        LIMIT 10
    `)
    if err == nil {
        defer sampleRows.Close()
        fmt.Printf("%-20s %-50s %-12s %s\n", "PDS", "DID", "记录数", "FOLLOWERS")
        fmt.Println(strings.Repeat("-", 98))
        for sampleRows.Next() {
            var pds, did string
            var followers sql.NullInt64
            var recordCount int64
            if err := sampleRows.Scan(&pds, &did, &recordCount, &followers); err == nil {
                followersStr := "NULL"
                if followers.Valid {
                    followersStr = fmt.Sprintf("%d", followers.Int64)
                }
                fmt.Printf("%-20s %-50s %-12d %s\n", pds, did, recordCount, followersStr)
            }
        }
    }

    return nil
}

// 导出CSV文件
func (p *PDSChecker) ExportToCSVFiles(ctx context.Context) error {
    fmt.Printf("开始导出CSV文件到 %s...\n", CSV_OUTPUT_DIR)
    
    // 创建输出目录
    if err := os.MkdirAll(CSV_OUTPUT_DIR, 0755); err != nil {
        return fmt.Errorf("failed to create output directory: %w", err)
    }
    
    // 获取符合条件的repos，按record_count降序排列
    query := `
        SELECT did, record_count 
        FROM temp_repo_stats 
        WHERE record_count >= $1 
        ORDER BY record_count DESC 
        LIMIT $2
    `
    
    rows, err := p.postgresDB.QueryContext(ctx, query, MIN_RECORDS_REQUIRED, TARGET_RECORD_COUNT)
    if err != nil {
        return fmt.Errorf("failed to query temp_repo_stats: %w", err)
    }
    defer rows.Close()
    
    // 读取所有数据
    var repos []struct {
        DID         string
        RecordCount int64
    }
    
    for rows.Next() {
        var repo struct {
            DID         string
            RecordCount int64
        }
        if err := rows.Scan(&repo.DID, &repo.RecordCount); err != nil {
            return fmt.Errorf("failed to scan row: %w", err)
        }
        repos = append(repos, repo)
    }
    
    fmt.Printf("成功获取 %d 条符合条件的repos记录\n", len(repos))
    
    // 分批导出到CSV文件
    for i := 0; i < CSV_FILES_COUNT; i++ {
        startIdx := i * RECORDS_PER_CSV
        endIdx := startIdx + RECORDS_PER_CSV
        if startIdx >= len(repos) {
            break
        }
        if endIdx > len(repos) {
            endIdx = len(repos)
        }
        
        filename := fmt.Sprintf("repos_batch_%02d.csv", i+1)
        filepath := filepath.Join(CSV_OUTPUT_DIR, filename)
        
        if err := p.writeCSVFile(filepath, repos[startIdx:endIdx]); err != nil {
            return fmt.Errorf("failed to write CSV file %s: %w", filename, err)
        }
        
        fmt.Printf("✅ 导出文件 %s: %d 条记录\n", filename, endIdx-startIdx)
    }
    
    fmt.Printf("\n🎉 CSV导出完成! 共导出 %d 个文件到 %s\n", CSV_FILES_COUNT, CSV_OUTPUT_DIR)
    return nil
}

// 写入单个CSV文件
func (p *PDSChecker) writeCSVFile(filepath string, repos []struct {
    DID         string
    RecordCount int64
}) error {
    file, err := os.Create(filepath)
    if err != nil {
        return err
    }
    defer file.Close()
    
    writer := csv.NewWriter(file)
    defer writer.Flush()
    
    // 写入CSV头部
    if err := writer.Write([]string{"did", "record_count"}); err != nil {
        return err
    }
    
    // 写入数据行
    for _, repo := range repos {
        record := []string{
            repo.DID,
            fmt.Sprintf("%d", repo.RecordCount),
        }
        if err := writer.Write(record); err != nil {
            return err
        }
    }
    
    return nil
}

// 打印进度信息
func (p *PDSChecker) printProgress() {
    fmt.Printf("\r🚀 实时进度: 处理记录: %d | 发现repos: %d | 已收集repos: %d | 批次: %d",
        p.counter.ProcessedRecords,
        p.counter.UniqueRepos,
        p.counter.CollectedRepos,
        p.counter.BatchesProcessed)
}

func main() {
    fmt.Println("启动PDS Checker (超大表优化版)...")

    // 获取配置
    scyllaHost := os.Getenv("SCYLLA_HOST")
    if scyllaHost == "" {
        scyllaHost = "127.0.0.1"
    }
    
    postgresPassword := "DNET_ZQJ"
    
    scyllaPort := 9042 // ScyllaDB默认端口
    if portStr := os.Getenv("SCYLLA_PORT"); portStr != "" {
        if _, err := fmt.Sscanf(portStr, "%d", &scyllaPort); err != nil {
            log.Printf("警告: 无法解析SCYLLA_PORT环境变量，使用默认端口9042")
            scyllaPort = 9042
        }
    }
    
    if postgresPassword == "" {
        log.Fatal("请设置POSTGRES_PASSWORD环境变量")
    }

    // 🚀 连接ScyllaDB - 高性能优化设置
    cluster := gocql.NewCluster(scyllaHost)
    cluster.Keyspace = "bluesky"
    cluster.Consistency = gocql.LocalQuorum  // 降低一致性要求提高性能
    cluster.Timeout = time.Second * 30       // 🚀 减少超时时间，快速失败
    cluster.Port = scyllaPort
    cluster.PageSize = 500                   // 🚀 增加页面大小提高吞吐量
    cluster.NumConns = 4                     // 🚀 增加连接数提高并发
    cluster.MaxPreparedStmts = 1000          // 🚀 增加预处理语句缓存
    cluster.MaxRoutingKeyInfo = 1000         // 🚀 增加路由键缓存

    scyllaSession, err := gocqlx.WrapSession(cluster.CreateSession())
    if err != nil {
        log.Fatal(fmt.Sprintf("Failed to connect to ScyllaDB at %s:%d: %v", scyllaHost, scyllaPort, err))
    }
    defer scyllaSession.Close()
    fmt.Printf("✓ 成功连接到ScyllaDB at %s:%d\n", scyllaHost, scyllaPort)

    // 连接PostgreSQL - 优化连接池设置
    postgresConnStr := fmt.Sprintf("host=localhost port=15432 user=postgres dbname=bluesky sslmode=disable password=%s", postgresPassword)
    postgresDB, err := sql.Open("postgres", postgresConnStr)
    if err != nil {
        log.Fatal(fmt.Sprintf("Failed to connect to PostgreSQL: %v", err))
    }
    defer postgresDB.Close()
    
    // 🚀 优化PostgreSQL连接池设置
    postgresDB.SetMaxOpenConns(10)  // 🚀 增加最大连接数
    postgresDB.SetMaxIdleConns(5)   // 🚀 增加空闲连接数
    postgresDB.SetConnMaxLifetime(time.Hour) // 连接生命周期
    postgresDB.SetConnMaxIdleTime(30 * time.Minute) // 🚀 空闲连接超时

    // 测试PostgreSQL连接
    if err := postgresDB.Ping(); err != nil {
        log.Fatal(fmt.Sprintf("Failed to ping PostgreSQL: %v", err))
    }
    fmt.Println("✓ 成功连接到PostgreSQL")

    // 创建检查器
    checker := NewPDSChecker(&scyllaSession, postgresDB)

    // 设置上下文，48小时超时（超大表需要更长时间）
    ctx, cancel := context.WithTimeout(context.Background(), 48*time.Hour)
    defer cancel()

    // 第一步：流式处理ScyllaDB中的records数据
    fmt.Println("\n=== 第一阶段：处理ScyllaDB Records数据 ===")
    fmt.Printf("目标: 收集 %d 条记录(每条至少%d个records)\n", TARGET_RECORD_COUNT, MIN_RECORDS_REQUIRED)
    if err := checker.ProcessRecordsStreaming(ctx); err != nil {
        log.Fatal(fmt.Sprintf("Failed to process records: %v", err))
    }

    // 第二步：导出CSV文件
    fmt.Println("\n=== 第二阶段：导出CSV文件 ===")
    if err := checker.ExportToCSVFiles(ctx); err != nil {
        log.Fatal(fmt.Sprintf("Failed to export CSV files: %v", err))
    }

    // 第三步：基于临时表创建最终结果表（包含PDS和Followers处理）
    fmt.Println("\n=== 第三阶段：创建最终结果表 ===")
    if err := checker.CreateRepoStatsTableFromTemp(ctx); err != nil {
        log.Fatal(fmt.Sprintf("Failed to create final stats table: %v", err))
    }

    fmt.Println("\n🎉 PDS Checker 处理完成!")
    fmt.Printf("已导出 %d 个CSV文件到: %s\n", CSV_FILES_COUNT, CSV_OUTPUT_DIR)
    fmt.Printf("新表 'repos_with_stats' 已创建，包含 %d 条有记录的repos数据\n", checker.counter.ValidRepos)
    fmt.Printf("总处理记录数: %d\n", checker.counter.ProcessedRecords)
    fmt.Printf("收集到的repos数: %d (最小记录数: %d)\n", checker.counter.CollectedRepos, MIN_RECORDS_REQUIRED)
    fmt.Printf("处理批次数: %d\n", checker.counter.BatchesProcessed)
}