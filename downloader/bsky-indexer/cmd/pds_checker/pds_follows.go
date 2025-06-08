package main

import (
    "context"
    "database/sql"
    "encoding/csv"
    "fmt"
    "os"
    "path/filepath"
    "strconv"
    "sync"
    "time"
    "log"
    "strings"

    "github.com/bluesky-social/indigo/atproto/identity"
    "github.com/bluesky-social/indigo/atproto/syntax"
    "github.com/bluesky-social/indigo/xrpc"
    comatproto "github.com/bluesky-social/indigo/api/atproto"
    "github.com/bluesky-social/indigo/api/bsky"
    _ "github.com/lib/pq"
)

// PDS Follows处理器结构体
type PDSFollowsProcessor struct {
    postgresDB      *sql.DB
    bskyClient      *xrpc.Client
    rateLimiter     *RateLimiter
    batchSize       int
    counter         *FollowsCounter
    mutex           sync.RWMutex
}

// 处理计数器
type FollowsCounter struct {
    TotalDIDs        int64
    ProcessedDIDs    int64
    SuccessfulDIDs   int64
    FailedDIDs       int64
    BatchesProcessed int64
    APIRequests      int64
    UpdatedRecords   int64
}

// 速率限制器 - 3000/5min = 10 requests/second
type RateLimiter struct {
    tokens    int
    maxTokens int
    lastRefill time.Time
    mutex     sync.Mutex
}

// DID记录结构
type DIDRecord struct {
    DID         string
    RecordCount int64
    PDS         sql.NullString
    Followers   sql.NullInt64
}

// CSV记录结构
type CSVRecord struct {
    DID         string
    RecordCount int64
    Followers   int64
    HasFollowers bool
}

var contactInfo = "shange0403@gmail.com"

// 创建新的RateLimiter，每秒允许10个请求
func NewRateLimiter() *RateLimiter {
    return &RateLimiter{
        tokens:    10,
        maxTokens: 10,
        lastRefill: time.Now(),
    }
}

// 等待获取token
func (r *RateLimiter) Wait() {
    r.mutex.Lock()
    defer r.mutex.Unlock()
    
    // 补充token（每秒补充10个）
    now := time.Now()
    elapsed := now.Sub(r.lastRefill)
    tokensToAdd := int(elapsed.Seconds() * 10) // 每秒10个token
    
    if tokensToAdd > 0 {
        r.tokens += tokensToAdd
        if r.tokens > r.maxTokens {
            r.tokens = r.maxTokens
        }
        r.lastRefill = now
    }
    
    // 如果没有token，等待
    for r.tokens <= 0 {
        r.mutex.Unlock()
        time.Sleep(100 * time.Millisecond) // 等待100ms后重试
        r.mutex.Lock()
        
        // 重新计算token
        now = time.Now()
        elapsed = now.Sub(r.lastRefill)
        tokensToAdd = int(elapsed.Seconds() * 10)
        
        if tokensToAdd > 0 {
            r.tokens += tokensToAdd
            if r.tokens > r.maxTokens {
                r.tokens = r.maxTokens
            }
            r.lastRefill = now
        }
    }
    
    // 消耗一个token
    r.tokens--
}

// 创建新的处理器
func NewPDSFollowsProcessor(postgresDB *sql.DB) *PDSFollowsProcessor {
    return &PDSFollowsProcessor{
        postgresDB:  postgresDB,
        rateLimiter: NewRateLimiter(),
        batchSize:   100, // 每批处理100个DID
        counter:     &FollowsCounter{},
    }
}

// 获取认证的BlueSky客户端
func (p *PDSFollowsProcessor) getAuthenticatedClient(ctx context.Context) (*xrpc.Client, error) {
    // 从环境变量获取认证信息
    handle := os.Getenv("BSKY_HANDLE")
    password := os.Getenv("BSKY_PASSWORD")
    
    // 如果环境变量为空，使用默认值
    if handle == "" {
        handle = "shange.bsky.social"
    }
    if password == "" {
        password = "zqj20030403"
    }
    
    // 创建临时客户端进行认证
    tempClient := &xrpc.Client{
        Host: "https://bsky.social",
    }
    
    // 获取会话
    session, err := comatproto.ServerCreateSession(ctx, tempClient, &comatproto.ServerCreateSession_Input{
        Identifier: handle,
        Password:   password,
    })
    if err != nil {
        return nil, fmt.Errorf("认证失败: %w", err)
    }
    
    // 创建认证客户端
    return &xrpc.Client{
        Host: "https://bsky.social",
        Auth: &xrpc.AuthInfo{
            AccessJwt: session.AccessJwt,
            Handle:    session.Handle,
            Did:       session.Did,
        },
    }, nil
}

// 修改temp_repo_stats表结构，添加pds和followers字段
func (p *PDSFollowsProcessor) updateTempTableSchema(ctx context.Context) error {
    fmt.Println("检查并更新temp_repo_stats表结构...")
    
    // 检查字段是否存在
    checkQuery := `
        SELECT column_name
        FROM information_schema.columns 
        WHERE table_schema = 'public' 
        AND table_name = 'temp_repo_stats'
        AND column_name IN ('pds', 'followers')
    `
    
    rows, err := p.postgresDB.QueryContext(ctx, checkQuery)
    if err != nil {
        return fmt.Errorf("failed to check table schema: %w", err)
    }
    defer rows.Close()
    
    existingColumns := make(map[string]bool)
    for rows.Next() {
        var columnName string
        if err := rows.Scan(&columnName); err != nil {
            continue
        }
        existingColumns[columnName] = true
    }
    
    // 添加缺失的字段
    var alterQueries []string
    
    if !existingColumns["pds"] {
        alterQueries = append(alterQueries, "ALTER TABLE temp_repo_stats ADD COLUMN pds TEXT")
        fmt.Println("  添加pds字段...")
    } else {
        fmt.Println("  ✓ pds字段已存在")
    }
    
    if !existingColumns["followers"] {
        alterQueries = append(alterQueries, "ALTER TABLE temp_repo_stats ADD COLUMN followers BIGINT")
        fmt.Println("  添加followers字段...")
    } else {
        fmt.Println("  ✓ followers字段已存在")
    }
    
    // 执行ALTER语句
    for _, query := range alterQueries {
        if _, err := p.postgresDB.ExecContext(ctx, query); err != nil {
            return fmt.Errorf("failed to alter table: %w", err)
        }
    }
    
    // 添加索引
    indexQueries := []string{
        "CREATE INDEX IF NOT EXISTS idx_temp_repo_stats_pds ON temp_repo_stats(pds)",
        "CREATE INDEX IF NOT EXISTS idx_temp_repo_stats_followers ON temp_repo_stats(followers)",
    }
    
    for _, query := range indexQueries {
        if _, err := p.postgresDB.ExecContext(ctx, query); err != nil {
            fmt.Printf("警告: 创建索引失败: %v\n", err)
        }
    }
    
    fmt.Println("✓ 表结构更新完成")
    return nil
}

// 获取总DID数量
func (p *PDSFollowsProcessor) getTotalDIDCount(ctx context.Context) (int64, error) {
    var count int64
    query := "SELECT COUNT(*) FROM temp_repo_stats WHERE pds IS NULL OR followers IS NULL"
    err := p.postgresDB.QueryRowContext(ctx, query).Scan(&count)
    return count, err
}

// 批量处理DID数据
func (p *PDSFollowsProcessor) ProcessDIDsWithFollows(ctx context.Context) error {
    fmt.Println("开始处理DID数据，添加PDS和Followers信息...")
    startTime := time.Now()
    
    // 首先更新表结构
    if err := p.updateTempTableSchema(ctx); err != nil {
        return fmt.Errorf("failed to update table schema: %w", err)
    }
    
    // 获取总数量
    totalCount, err := p.getTotalDIDCount(ctx)
    if err != nil {
        return fmt.Errorf("failed to get total count: %w", err)
    }
    p.counter.TotalDIDs = totalCount
    fmt.Printf("需要处理的DID总数: %d\n", totalCount)
    
    // 获得认证客户端
    client, err := p.getAuthenticatedClient(ctx)
    if err != nil {
        return fmt.Errorf("failed to get authenticated client: %w", err)
    }
    p.bskyClient = client
    fmt.Println("✓ BlueSky API客户端认证成功")
    
    // 分批处理
    offset := int64(0)
    for offset < totalCount {
        if err := p.processBatch(ctx, offset); err != nil {
            fmt.Printf("警告: 批次处理失败 (offset=%d): %v\n", offset, err)
        }
        
        offset += int64(p.batchSize)
        p.counter.BatchesProcessed++
        
        // 打印进度
        p.printProgress()
        
        // 每10批次暂停一下，避免过度负载
        if p.counter.BatchesProcessed%10 == 0 {
            time.Sleep(5 * time.Second)
        }
    }
    
    // 打印最终统计
    duration := time.Since(startTime)
    fmt.Printf("\n🎉 处理完成!\n")
    fmt.Printf("总用时: %v\n", duration)
    fmt.Printf("处理DID数: %d/%d\n", p.counter.ProcessedDIDs, p.counter.TotalDIDs)
    fmt.Printf("成功: %d, 失败: %d\n", p.counter.SuccessfulDIDs, p.counter.FailedDIDs)
    fmt.Printf("API请求数: %d\n", p.counter.APIRequests)
    fmt.Printf("更新记录数: %d\n", p.counter.UpdatedRecords)
    fmt.Printf("平均处理速度: %.2f DID/秒\n", float64(p.counter.ProcessedDIDs)/duration.Seconds())
    
    return nil
}

// 处理单个批次
func (p *PDSFollowsProcessor) processBatch(ctx context.Context, offset int64) error {
    // 查询当前批次的DID数据
    query := `
        SELECT did, record_count, pds, followers 
        FROM temp_repo_stats 
        WHERE pds IS NULL OR followers IS NULL
        ORDER BY did
        LIMIT $1 OFFSET $2
    `
    
    rows, err := p.postgresDB.QueryContext(ctx, query, p.batchSize, offset)
    if err != nil {
        return fmt.Errorf("failed to query batch: %w", err)
    }
    defer rows.Close()
    
    var records []DIDRecord
    for rows.Next() {
        var record DIDRecord
        if err := rows.Scan(&record.DID, &record.RecordCount, &record.PDS, &record.Followers); err != nil {
            fmt.Printf("警告: 扫描记录失败: %v\n", err)
            continue
        }
        records = append(records, record)
    }
    
    if len(records) == 0 {
        return nil
    }
    
    // 处理每个记录
    for i := range records {
        p.counter.ProcessedDIDs++
        
        // 填充PDS信息（如果缺失）
        if !records[i].PDS.Valid {
            if pds, err := p.getPDSForDID(ctx, records[i].DID); err != nil {
                fmt.Printf("警告: 获取PDS失败 %s: %v\n", records[i].DID, err)
            } else {
                records[i].PDS = sql.NullString{String: pds, Valid: true}
            }
        }
        
        // 填充Followers信息（如果缺失）
        if !records[i].Followers.Valid {
            // 应用速率限制
            p.rateLimiter.Wait()
            p.counter.APIRequests++
            
            if followers, err := p.getFollowersCount(ctx, records[i].DID); err != nil {
                fmt.Printf("警告: 获取Followers失败 %s: %v\n", records[i].DID, err)
                p.counter.FailedDIDs++
            } else {
                records[i].Followers = sql.NullInt64{Int64: followers, Valid: true}
                p.counter.SuccessfulDIDs++
            }
        }
    }
    
    // 批量更新数据库
    return p.updateBatch(ctx, records)
}

// 通过repos表获取DID对应的PDS
func (p *PDSFollowsProcessor) getPDSForDID(ctx context.Context, did string) (string, error) {
    var pds string
    query := "SELECT pds FROM repos WHERE did = $1"
    err := p.postgresDB.QueryRowContext(ctx, query, did).Scan(&pds)
    if err == sql.ErrNoRows {
        return "", fmt.Errorf("DID not found in repos table")
    }
    return pds, err
}

// 获取用户的followers数量 - 优化版本，参考fetch_repos.go
func (p *PDSFollowsProcessor) getFollowersCount(ctx context.Context, did string) (int64, error) {
    // 优先使用DID作为actor参数
    actor := did
    
    // 如果DID不是标准格式，尝试作为handle使用
    if !strings.HasPrefix(did, "did:") {
        actor = did
    }
    
    // 调用BlueSky API获取profile
    profile, err := bsky.ActorGetProfile(ctx, p.bskyClient, actor)
    if err != nil {
        // 如果使用DID失败，且看起来像handle，尝试直接使用
        if strings.Contains(did, ".") && !strings.HasPrefix(did, "did:") {
            profile, err = bsky.ActorGetProfile(ctx, p.bskyClient, did)
            if err != nil {
                return 0, fmt.Errorf("failed to get profile for %s: %w", did, err)
            }
        } else {
            return 0, fmt.Errorf("failed to get profile for %s: %w", actor, err)
        }
    }
    
    // 检查followers数量
    if profile.FollowersCount == nil {
        return 0, fmt.Errorf("no followersCount in profile for %s", actor)
    }
    
    return *profile.FollowersCount, nil
}

// 带重试机制的followers获取
func (p *PDSFollowsProcessor) getFollowersCountWithRetry(ctx context.Context, did string, maxRetries int) (int64, error) {
    var lastErr error
    
    for attempt := 0; attempt < maxRetries; attempt++ {
        if attempt > 0 {
            // 重试前等待递增的时间
            waitTime := time.Duration(attempt) * 2 * time.Second
            fmt.Printf("重试获取followers %s (尝试 %d/%d)，等待 %v...\n", did, attempt+1, maxRetries, waitTime)
            time.Sleep(waitTime)
        }
        
        followers, err := p.getFollowersCount(ctx, did)
        if err == nil {
            if attempt > 0 {
                fmt.Printf("✓ 重试成功: %s\n", did)
            }
            return followers, nil
        }
        
        lastErr = err
        
        // 检查是否是速率限制错误
        if strings.Contains(err.Error(), "rate") || strings.Contains(err.Error(), "429") {
            fmt.Printf("遇到速率限制，等待更长时间...\n")
            time.Sleep(30 * time.Second)
        }
    }
    
    return 0, fmt.Errorf("重试 %d 次后仍失败: %w", maxRetries, lastErr)
}

// 获取完整的用户profile信息（可选，用于调试）
func (p *PDSFollowsProcessor) getFullProfile(ctx context.Context, did string) (*bsky.ActorDefs_ProfileViewDetailed, error) {
    actor := did
    if !strings.HasPrefix(did, "did:") {
        actor = did
    }
    
    profile, err := bsky.ActorGetProfile(ctx, p.bskyClient, actor)
    if err != nil {
        if strings.Contains(did, ".") && !strings.HasPrefix(did, "did:") {
            return bsky.ActorGetProfile(ctx, p.bskyClient, did)
        }
        return nil, err
    }
    
    return profile, nil
}

// 从DID获取handle（可选的辅助功能）
func (p *PDSFollowsProcessor) getHandleFromDID(ctx context.Context, did string) (string, error) {
    // 尝试从identity目录解析
    atid, err := syntax.ParseAtIdentifier(did)
    if err != nil {
        return "", err
    }
    
    dir := identity.DefaultDirectory()
    ident, err := dir.Lookup(ctx, *atid)
    if err != nil {
        return "", err
    }
    
    if ident.Handle.String() != "" {
        return ident.Handle.String(), nil
    }
    
    return "", fmt.Errorf("no handle found for DID %s", did)
}

// 批量更新数据库
func (p *PDSFollowsProcessor) updateBatch(ctx context.Context, records []DIDRecord) error {
    if len(records) == 0 {
        return nil
    }
    
    tx, err := p.postgresDB.BeginTx(ctx, nil)
    if err != nil {
        return fmt.Errorf("failed to begin transaction: %w", err)
    }
    defer tx.Rollback()
    
    stmt, err := tx.PrepareContext(ctx, `
        UPDATE temp_repo_stats 
        SET pds = $2, followers = $3 
        WHERE did = $1
    `)
    if err != nil {
        return fmt.Errorf("failed to prepare statement: %w", err)
    }
    defer stmt.Close()
    
    updatedCount := 0
    for _, record := range records {
        result, err := stmt.ExecContext(ctx, record.DID, record.PDS, record.Followers)
        if err != nil {
            fmt.Printf("警告: 更新记录失败 %s: %v\n", record.DID, err)
            continue
        }
        
        if rowsAffected, _ := result.RowsAffected(); rowsAffected > 0 {
            updatedCount++
        }
    }
    
    if err := tx.Commit(); err != nil {
        return fmt.Errorf("failed to commit transaction: %w", err)
    }
    
    p.counter.UpdatedRecords += int64(updatedCount)
    return nil
}

// 打印进度信息
func (p *PDSFollowsProcessor) printProgress() {
    fmt.Printf("\r进度: 处理DID: %d/%d | 成功: %d | 失败: %d | API请求: %d | 批次: %d",
        p.counter.ProcessedDIDs,
        p.counter.TotalDIDs,
        p.counter.SuccessfulDIDs,
        p.counter.FailedDIDs,
        p.counter.APIRequests,
        p.counter.BatchesProcessed)
}

// 主函数用于独立运行此处理器
func runPDSFollowsProcessor() error {
    fmt.Println("启动PDS Follows处理器...")
    
    // 获取PostgreSQL连接配置
    postgresPassword := os.Getenv("POSTGRES_PASSWORD")
    if postgresPassword == "" {
        postgresPassword = "DNET_ZQJ"
    }
    
    // 连接PostgreSQL
    postgresConnStr := fmt.Sprintf("host=localhost port=15432 user=postgres dbname=bluesky sslmode=disable password=%s", postgresPassword)
    postgresDB, err := sql.Open("postgres", postgresConnStr)
    if err != nil {
        return fmt.Errorf("failed to connect to PostgreSQL: %w", err)
    }
    defer postgresDB.Close()
    
    // 优化连接池设置
    postgresDB.SetMaxOpenConns(5)
    postgresDB.SetMaxIdleConns(2)
    postgresDB.SetConnMaxLifetime(time.Hour)
    
    // 测试连接
    if err := postgresDB.Ping(); err != nil {
        return fmt.Errorf("failed to ping PostgreSQL: %w", err)
    }
    fmt.Println("✓ 成功连接到PostgreSQL")
    
    // 创建处理器
    processor := NewPDSFollowsProcessor(postgresDB)
    
    // 设置上下文，12小时超时
    ctx, cancel := context.WithTimeout(context.Background(), 12*time.Hour)
    defer cancel()
    
    // 处理DID数据
    return processor.ProcessDIDsWithFollows(ctx)
}

// 处理CSV文件，添加followers信息
func (p *PDSFollowsProcessor) ProcessCSVFiles(ctx context.Context, csvDir string) error {
    fmt.Printf("开始处理CSV文件目录: %s\n", csvDir)
    startTime := time.Now()
    
    // 获得认证客户端
    client, err := p.getAuthenticatedClient(ctx)
    if err != nil {
        return fmt.Errorf("failed to get authenticated client: %w", err)
    }
    p.bskyClient = client
    fmt.Println("✓ BlueSky API客户端认证成功")
    
    // 查找所有CSV文件
    csvFiles, err := p.findCSVFiles(csvDir)
    if err != nil {
        return fmt.Errorf("failed to find CSV files: %w", err)
    }
    
    if len(csvFiles) == 0 {
        return fmt.Errorf("no CSV files found in directory: %s", csvDir)
    }
    
    fmt.Printf("找到 %d 个CSV文件\n", len(csvFiles))
    
    // 处理每个CSV文件
    for i, csvFile := range csvFiles {
        fmt.Printf("\n=== 处理文件 %d/%d: %s ===\n", i+1, len(csvFiles), filepath.Base(csvFile))
        
        if err := p.processCSVFile(ctx, csvFile); err != nil {
            fmt.Printf("警告: 处理文件失败 %s: %v\n", csvFile, err)
            continue
        }
        
        fmt.Printf("✅ 完成文件: %s\n", filepath.Base(csvFile))
        
        // 每个文件之间暂停一下
        if i < len(csvFiles)-1 {
            fmt.Println("暂停5秒...")
            time.Sleep(5 * time.Second)
        }
    }
    
    // 打印总体统计
    duration := time.Since(startTime)
    fmt.Printf("\n🎉 所有CSV文件处理完成!\n")
    fmt.Printf("总用时: %v\n", duration)
    fmt.Printf("处理文件数: %d\n", len(csvFiles))
    fmt.Printf("总API请求数: %d\n", p.counter.APIRequests)
    fmt.Printf("成功获取followers: %d\n", p.counter.SuccessfulDIDs)
    fmt.Printf("失败: %d\n", p.counter.FailedDIDs)
    
    return nil
}

// 查找目录中的所有CSV文件
func (p *PDSFollowsProcessor) findCSVFiles(csvDir string) ([]string, error) {
    var csvFiles []string
    
    err := filepath.Walk(csvDir, func(path string, info os.FileInfo, err error) error {
        if err != nil {
            return err
        }
        
        if !info.IsDir() && strings.HasSuffix(strings.ToLower(info.Name()), ".csv") {
            csvFiles = append(csvFiles, path)
        }
        
        return nil
    })
    
    return csvFiles, err
}

// 处理单个CSV文件
func (p *PDSFollowsProcessor) processCSVFile(ctx context.Context, csvFilePath string) error {
    // 读取CSV文件
    records, err := p.readCSVFile(csvFilePath)
    if err != nil {
        return fmt.Errorf("failed to read CSV file: %w", err)
    }
    
    fmt.Printf("读取到 %d 条记录\n", len(records))
    
    if len(records) == 0 {
        return fmt.Errorf("CSV文件为空")
    }
    
    // 处理每条记录，获取followers
    processedCount := 0
    for i := range records {
        if records[i].HasFollowers {
            continue // 已经有followers数据，跳过
        }
        
        // 应用速率限制
        p.rateLimiter.Wait()
        p.counter.APIRequests++
        
        // 获取followers数量（带重试机制）
        followers, err := p.getFollowersCountWithRetry(ctx, records[i].DID, 3)
        if err != nil {
            fmt.Printf("警告: 获取followers失败 %s: %v\n", records[i].DID, err)
            p.counter.FailedDIDs++
            records[i].Followers = -1 // 标记为失败
            records[i].HasFollowers = true // 标记已处理，避免重复尝试
        } else {
            records[i].Followers = followers
            records[i].HasFollowers = true
            p.counter.SuccessfulDIDs++
        }
        
        processedCount++
        
        // 每处理100条记录显示一次进度
        if processedCount%100 == 0 {
            fmt.Printf("已处理: %d/%d\n", processedCount, len(records))
        }
    }
    
    // 写回CSV文件
    if err := p.writeCSVFile(csvFilePath, records); err != nil {
        return fmt.Errorf("failed to write CSV file: %w", err)
    }
    
    fmt.Printf("已处理 %d 条记录并更新文件\n", processedCount)
    return nil
}

// 读取CSV文件
func (p *PDSFollowsProcessor) readCSVFile(csvFilePath string) ([]CSVRecord, error) {
    file, err := os.Open(csvFilePath)
    if err != nil {
        return nil, err
    }
    defer file.Close()
    
    reader := csv.NewReader(file)
    rows, err := reader.ReadAll()
    if err != nil {
        return nil, err
    }
    
    if len(rows) == 0 {
        return nil, fmt.Errorf("CSV文件为空")
    }
    
    var records []CSVRecord
    
    // 检查第一行是否为标题行
    startRow := 0
    if len(rows[0]) >= 2 && rows[0][0] == "did" && rows[0][1] == "record_count" {
        startRow = 1 // 跳过标题行
    }
    
    for i := startRow; i < len(rows); i++ {
        row := rows[i]
        if len(row) < 2 {
            continue // 跳过格式错误的行
        }
        
        record := CSVRecord{
            DID: strings.TrimSpace(row[0]),
        }
        
        // 解析record_count
        if recordCount, err := strconv.ParseInt(strings.TrimSpace(row[1]), 10, 64); err == nil {
            record.RecordCount = recordCount
        }
        
        // 如果已有followers列（第3列）
        if len(row) >= 3 && strings.TrimSpace(row[2]) != "" {
            if followers, err := strconv.ParseInt(strings.TrimSpace(row[2]), 10, 64); err == nil && followers >= 0 {
                record.Followers = followers
                record.HasFollowers = true
            }
        }
        
        records = append(records, record)
    }
    
    return records, nil
}

// 写入CSV文件
func (p *PDSFollowsProcessor) writeCSVFile(csvFilePath string, records []CSVRecord) error {
    file, err := os.Create(csvFilePath)
    if err != nil {
        return err
    }
    defer file.Close()
    
    writer := csv.NewWriter(file)
    defer writer.Flush()
    
    // 写入标题行
    if err := writer.Write([]string{"did", "record_count", "followers"}); err != nil {
        return err
    }
    
    // 写入数据行
    for _, record := range records {
        followersStr := ""
        if record.HasFollowers {
            if record.Followers >= 0 {
                followersStr = strconv.FormatInt(record.Followers, 10)
            } else {
                followersStr = "ERROR" // 标记获取失败
            }
        }
        
        row := []string{
            record.DID,
            strconv.FormatInt(record.RecordCount, 10),
            followersStr,
        }
        
        if err := writer.Write(row); err != nil {
            return err
        }
    }
    
    return nil
}

// 主函数用于直接处理CSV文件
func ProcessCSVFilesStandalone(csvDir string) error {
    fmt.Println("启动CSV Followers处理器...")
    
    // 创建处理器（不需要数据库连接）
    processor := &PDSFollowsProcessor{
        rateLimiter: NewRateLimiter(),
        batchSize:   100,
        counter:     &FollowsCounter{},
    }
    
    // 设置上下文，12小时超时
    ctx, cancel := context.WithTimeout(context.Background(), 12*time.Hour)
    defer cancel()
    
    // 处理CSV文件
    return processor.ProcessCSVFiles(ctx, csvDir)
}

// 如果直接运行此文件则执行处理器
func init() {
    // 这个函数可以从main.go中调用，也可以独立运行
}
