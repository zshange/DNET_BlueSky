package main

import (
    "context"
    "database/sql"
    "encoding/csv"
    "fmt"
    "os"
    "path/filepath"
    "time"
    "log"

    _ "github.com/lib/pq"
)

// 配置常量
const (
    RECORDS_PER_FILE = 100000 // 每个CSV文件100,000条记录
    CSV_OUTPUT_DIR   = "/mydata/csv" // CSV输出目录
)

// ReposExporter 结构体
type ReposExporter struct {
    postgresDB *sql.DB
}

// ReposWithPDS 结构体表示导出的记录
type ReposWithPDS struct {
    DID       string
    Head      string
    Rev       string
    IndexedAt time.Time
    PDS       string
    PDSHost   string
}

func NewReposExporter(postgresDB *sql.DB) *ReposExporter {
    return &ReposExporter{
        postgresDB: postgresDB,
    }
}

// ExportReposToCSV 导出repos表数据到CSV文件
func (e *ReposExporter) ExportReposToCSV(ctx context.Context) error {
    fmt.Printf("开始导出repos表数据到CSV文件...\n")
    fmt.Printf("输出目录: %s\n", CSV_OUTPUT_DIR)
    
    startTime := time.Now()
    
    // 创建输出目录
    if err := os.MkdirAll(CSV_OUTPUT_DIR, 0755); err != nil {
        return fmt.Errorf("failed to create output directory: %w", err)
    }
    
    // 检查已存在的CSV文件，实现断点续传
    existingFiles, startOffset, startFileIndex := e.checkExistingFiles()
    
    if existingFiles > 0 {
        fmt.Printf("📁 发现已存在 %d 个CSV文件\n", existingFiles)
        fmt.Printf("📍 将从第 %d 个文件开始继续导出 (跳过前 %d 条记录)\n", startFileIndex, startOffset)
    } else {
        fmt.Printf("📁 输出目录为空，从头开始导出\n")
    }
    
    // 首先获取总记录数
    var totalCount int64
    countQuery := `
        SELECT COUNT(*) 
        FROM repos r
        LEFT JOIN pds p ON r.pds = p.id
    `
    if err := e.postgresDB.QueryRowContext(ctx, countQuery).Scan(&totalCount); err != nil {
        return fmt.Errorf("failed to get total count: %w", err)
    }
    
    fmt.Printf("总记录数: %d\n", totalCount)
    fmt.Printf("剩余需要处理: %d 条记录\n", totalCount-startOffset)
    
    // 计算需要创建的文件数
    remainingRecords := totalCount - startOffset
    remainingFileCount := (remainingRecords + RECORDS_PER_FILE - 1) / RECORDS_PER_FILE
    totalFileCount := (totalCount + RECORDS_PER_FILE - 1) / RECORDS_PER_FILE
    fmt.Printf("预计总文件数: %d，还需创建: %d 个文件\n", totalFileCount, remainingFileCount)
    
    // 从计算出的offset开始导出
    var offset int64 = startOffset
    fileIndex := startFileIndex
    
    for offset < totalCount {
        // 构建查询语句，包含JOIN和分页
        query := `
            SELECT 
                r.did,
                COALESCE(r.last_indexed_rev, '') as head,
                COALESCE(r.last_firehose_rev, '') as rev,
                r.updated_at as indexed_at,
                COALESCE(r.pds::text, '') as pds,
                COALESCE(p.host, '') as pds_host
            FROM repos r
            LEFT JOIN pds p ON r.pds = p.id
            ORDER BY r.did
            LIMIT $1 OFFSET $2
        `
        
        fmt.Printf("正在查询第 %d 批数据 (offset: %d, limit: %d)...\n", fileIndex, offset, RECORDS_PER_FILE)
        
        rows, err := e.postgresDB.QueryContext(ctx, query, RECORDS_PER_FILE, offset)
        if err != nil {
            return fmt.Errorf("failed to query repos data: %w", err)
        }
        
        // 读取数据
        var records []ReposWithPDS
        for rows.Next() {
            var record ReposWithPDS
            if err := rows.Scan(
                &record.DID,
                &record.Head,
                &record.Rev,
                &record.IndexedAt,
                &record.PDS,
                &record.PDSHost,
            ); err != nil {
                rows.Close()
                return fmt.Errorf("failed to scan row: %w", err)
            }
            records = append(records, record)
        }
        rows.Close()
        
        if len(records) == 0 {
            break
        }
        
        // 生成文件名
        filename := fmt.Sprintf("repos_export_%03d.csv", fileIndex)
        filepath := filepath.Join(CSV_OUTPUT_DIR, filename)
        
        // 写入CSV文件
        if err := e.writeCSVFile(filepath, records); err != nil {
            return fmt.Errorf("failed to write CSV file %s: %w", filename, err)
        }
        
        fmt.Printf("✅ 已导出文件 %s: %d 条记录\n", filename, len(records))
        
        offset += int64(len(records))
        fileIndex++
        
        // 如果返回的记录数少于请求的数量，说明已经到达末尾
        if len(records) < RECORDS_PER_FILE {
            break
        }
    }
    
    duration := time.Since(startTime)
    fmt.Printf("\n🎉 CSV导出完成!\n")
    fmt.Printf("总用时: %v\n", duration)
    fmt.Printf("总记录数: %d\n", totalCount)
    fmt.Printf("本次导出文件数: %d\n", fileIndex-startFileIndex)
    fmt.Printf("累计文件数: %d\n", fileIndex-1)
    fmt.Printf("输出目录: %s\n", CSV_OUTPUT_DIR)
    
    return nil
}

// writeCSVFile 写入单个CSV文件
func (e *ReposExporter) writeCSVFile(filepath string, records []ReposWithPDS) error {
    file, err := os.Create(filepath)
    if err != nil {
        return fmt.Errorf("failed to create file: %w", err)
    }
    defer file.Close()
    
    writer := csv.NewWriter(file)
    defer writer.Flush()
    
    // 写入CSV头部
    headers := []string{
        "did",
        "last_indexed_rev", 
        "last_firehose_rev",
        "updated_at",
        "pds_id",
        "pds_host",
    }
    if err := writer.Write(headers); err != nil {
        return fmt.Errorf("failed to write headers: %w", err)
    }
    
    // 写入数据行
    for _, record := range records {
        row := []string{
            record.DID,
            record.Head,
            record.Rev,
            record.IndexedAt.Format(time.RFC3339),
            record.PDS,
            record.PDSHost,
        }
        if err := writer.Write(row); err != nil {
            return fmt.Errorf("failed to write row: %w", err)
        }
    }
    
    return nil
}

// TestConnection 测试数据库连接并显示表结构信息
func (e *ReposExporter) TestConnection(ctx context.Context) error {
    fmt.Println("测试数据库连接...")
    
    // 测试连接
    if err := e.postgresDB.PingContext(ctx); err != nil {
        return fmt.Errorf("failed to ping database: %w", err)
    }
    fmt.Println("✓ 数据库连接正常")
    
    // 检查repos表结构
    fmt.Println("\n检查repos表结构...")
    reposStructQuery := `
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns 
        WHERE table_schema = 'public' 
        AND table_name = 'repos'
        ORDER BY ordinal_position
    `
    
    rows, err := e.postgresDB.QueryContext(ctx, reposStructQuery)
    if err != nil {
        return fmt.Errorf("failed to query repos table structure: %w", err)
    }
    defer rows.Close()
    
    fmt.Printf("%-15s %-20s %s\n", "Column", "Type", "Nullable")
    fmt.Println(fmt.Sprintf("%s", string(make([]byte, 50))))
    for rows.Next() {
        var columnName, dataType, nullable string
        if err := rows.Scan(&columnName, &dataType, &nullable); err != nil {
            continue
        }
        fmt.Printf("%-15s %-20s %s\n", columnName, dataType, nullable)
    }
    
    // 检查pds表结构
    fmt.Println("\n检查pds表结构...")
    pdsStructQuery := `
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns 
        WHERE table_schema = 'public' 
        AND table_name = 'pds'
        ORDER BY ordinal_position
    `
    
    rows2, err := e.postgresDB.QueryContext(ctx, pdsStructQuery)
    if err != nil {
        return fmt.Errorf("failed to query pds table structure: %w", err)
    }
    defer rows2.Close()
    
    fmt.Printf("%-20s %-20s %s\n", "Column", "Type", "Nullable")
    fmt.Println(fmt.Sprintf("%s", string(make([]byte, 60))))
    for rows2.Next() {
        var columnName, dataType, nullable string
        if err := rows2.Scan(&columnName, &dataType, &nullable); err != nil {
            continue
        }
        fmt.Printf("%-20s %-20s %s\n", columnName, dataType, nullable)
    }
    
    // 获取repos表记录数
    var reposCount int64
    if err := e.postgresDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM repos").Scan(&reposCount); err != nil {
        return fmt.Errorf("failed to count repos: %w", err)
    }
    fmt.Printf("\nrepos表记录数: %d\n", reposCount)
    
    // 获取pds表记录数
    var pdsCount int64
    if err := e.postgresDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM pds").Scan(&pdsCount); err != nil {
        return fmt.Errorf("failed to count pds: %w", err)
    }
    fmt.Printf("pds表记录数: %d\n", pdsCount)
    
    // 显示一些示例数据
    fmt.Println("\n显示前5条关联数据示例:")
    sampleQuery := `
        SELECT 
            r.did,
            COALESCE(r.last_indexed_rev, 'NULL') as head,
            COALESCE(r.pds::text, 'NULL') as pds,
            COALESCE(p.host, 'NULL') as pds_host
        FROM repos r
        LEFT JOIN pds p ON r.pds = p.id
        ORDER BY r.did
        LIMIT 5
    `
    
    sampleRows, err := e.postgresDB.QueryContext(ctx, sampleQuery)
    if err != nil {
        fmt.Printf("警告: 无法获取示例数据: %v\n", err)
        return nil
    }
    defer sampleRows.Close()
    
    fmt.Printf("%-50s %-20s %-30s %s\n", "DID", "HEAD", "PDS", "PDS_HOST")
    fmt.Println(fmt.Sprintf("%s", string(make([]byte, 120))))
    for sampleRows.Next() {
        var did, head, pds, pdsHost string
        if err := sampleRows.Scan(&did, &head, &pds, &pdsHost); err != nil {
            continue
        }
        fmt.Printf("%-50s %-20s %-30s %s\n", did, head, pds, pdsHost)
    }
    
    return nil
}

// checkExistingFiles 检查已存在的CSV文件，返回文件数量、起始偏移量和起始文件索引
func (e *ReposExporter) checkExistingFiles() (int, int64, int) {
	files, err := filepath.Glob(filepath.Join(CSV_OUTPUT_DIR, "repos_export_*.csv"))
	if err != nil {
		fmt.Printf("⚠️  扫描现有文件时出错: %v\n", err)
		return 0, 0, 1
	}
	
	if len(files) == 0 {
		return 0, 0, 1
	}
	
	// 验证现有文件的完整性
	validFiles := 0
	maxValidIndex := 0
	
	for _, file := range files {
		// 从文件名提取索引号
		var fileIndex int
		filename := filepath.Base(file)
		if n, err := fmt.Sscanf(filename, "repos_export_%03d.csv", &fileIndex); n == 1 && err == nil {
			// 检查文件是否有效（大小大于header行）
			if info, err := os.Stat(file); err == nil && info.Size() > 200 { // 至少200字节（包含header）
				validFiles++
				if fileIndex > maxValidIndex {
					maxValidIndex = fileIndex
				}
			} else {
				fmt.Printf("⚠️  发现损坏的文件: %s (大小: %d)\n", filename, info.Size())
			}
		}
	}
	
	if validFiles == 0 {
		return 0, 0, 1
	}
	
	// 计算起始偏移量和下一个文件索引
	startOffset := int64(validFiles) * RECORDS_PER_FILE
	nextFileIndex := maxValidIndex + 1
	
	fmt.Printf("📊 文件扫描结果:\n")
	fmt.Printf("   - 总文件数: %d\n", len(files))
	fmt.Printf("   - 有效文件数: %d\n", validFiles)
	fmt.Printf("   - 最大文件索引: %d\n", maxValidIndex)
	fmt.Printf("   - 已处理记录数: %d\n", startOffset)
	
	return validFiles, startOffset, nextFileIndex
}

func main() {
    fmt.Println("启动Repos CSV导出工具...")
    
    // 从环境变量或使用默认值获取PostgreSQL密码
    postgresPassword := os.Getenv("POSTGRES_PASSWORD")
    if postgresPassword == "" {
        postgresPassword = "DNET_ZQJ" // 使用main.go中的默认密码
    }
    
    // 连接PostgreSQL - 使用与main.go相同的连接参数
    postgresConnStr := fmt.Sprintf("host=localhost port=15432 user=postgres dbname=bluesky sslmode=disable password=%s", postgresPassword)
    postgresDB, err := sql.Open("postgres", postgresConnStr)
    if err != nil {
        log.Fatal(fmt.Sprintf("Failed to connect to PostgreSQL: %v", err))
    }
    defer postgresDB.Close()
    
    // 优化PostgreSQL连接池设置
    postgresDB.SetMaxOpenConns(10)
    postgresDB.SetMaxIdleConns(5)
    postgresDB.SetConnMaxLifetime(24*time.Hour)
    postgresDB.SetConnMaxIdleTime(30 * time.Minute)
    
    // 创建导出器
    exporter := NewReposExporter(postgresDB)
    
    // 设置上下文，2小时超时
    ctx, cancel := context.WithTimeout(context.Background(), 24*time.Hour)
    defer cancel()
    
    // 测试连接并显示表信息
    fmt.Println("=== 数据库连接测试 ===")
    if err := exporter.TestConnection(ctx); err != nil {
        log.Fatal(fmt.Sprintf("Database connection test failed: %v", err))
    }
    
    // 开始导出
    fmt.Println("\n=== 开始导出CSV ===")
    if err := exporter.ExportReposToCSV(ctx); err != nil {
        log.Fatal(fmt.Sprintf("Failed to export CSV: %v", err))
    }
    
    fmt.Println("\n🎉 程序执行完成!")
} 