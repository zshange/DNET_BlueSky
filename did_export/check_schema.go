package main

import (
    "context"
    "database/sql"
    "fmt"
    "log"
    "time"

    _ "github.com/lib/pq"
)

func main() {
    fmt.Println("检查数据库表结构...")
    
    // 连接PostgreSQL
    postgresPassword := "DNET_ZQJ"
    postgresConnStr := fmt.Sprintf("host=localhost port=15432 user=postgres dbname=bluesky sslmode=disable password=%s", postgresPassword)
    postgresDB, err := sql.Open("postgres", postgresConnStr)
    if err != nil {
        log.Fatal(fmt.Sprintf("Failed to connect to PostgreSQL: %v", err))
    }
    defer postgresDB.Close()

    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    // 检查repos表结构
    fmt.Println("\n=== REPOS表结构 ===")
    reposQuery := `
        SELECT column_name, data_type, character_maximum_length, is_nullable
        FROM information_schema.columns 
        WHERE table_schema = 'public' 
        AND table_name = 'repos'
        ORDER BY ordinal_position
    `
    
    rows, err := postgresDB.QueryContext(ctx, reposQuery)
    if err != nil {
        log.Fatal(fmt.Sprintf("Failed to query repos structure: %v", err))
    }
    defer rows.Close()
    
    fmt.Printf("%-15s %-20s %-10s %s\n", "Column", "Type", "Length", "Nullable")
    fmt.Println(string(make([]byte, 60)))
    for rows.Next() {
        var columnName, dataType, nullable string
        var maxLength sql.NullInt64
        if err := rows.Scan(&columnName, &dataType, &maxLength, &nullable); err != nil {
            continue
        }
        lengthStr := "N/A"
        if maxLength.Valid {
            lengthStr = fmt.Sprintf("%d", maxLength.Int64)
        }
        fmt.Printf("%-15s %-20s %-10s %s\n", columnName, dataType, lengthStr, nullable)
    }

    // 检查pds表结构
    fmt.Println("\n=== PDS表结构 ===")
    pdsQuery := `
        SELECT column_name, data_type, character_maximum_length, is_nullable
        FROM information_schema.columns 
        WHERE table_schema = 'public' 
        AND table_name = 'pds'
        ORDER BY ordinal_position
    `
    
    rows2, err := postgresDB.QueryContext(ctx, pdsQuery)
    if err != nil {
        log.Fatal(fmt.Sprintf("Failed to query pds structure: %v", err))
    }
    defer rows2.Close()
    
    fmt.Printf("%-20s %-20s %-10s %s\n", "Column", "Type", "Length", "Nullable")
    fmt.Println(string(make([]byte, 70)))
    for rows2.Next() {
        var columnName, dataType, nullable string
        var maxLength sql.NullInt64
        if err := rows2.Scan(&columnName, &dataType, &maxLength, &nullable); err != nil {
            continue
        }
        lengthStr := "N/A"
        if maxLength.Valid {
            lengthStr = fmt.Sprintf("%d", maxLength.Int64)
        }
        fmt.Printf("%-20s %-20s %-10s %s\n", columnName, dataType, lengthStr, nullable)
    }

    // 检查repos表中pds字段的实际数据类型和示例值
    fmt.Println("\n=== REPOS表PDS字段示例 ===")
    sampleQuery := `
        SELECT DISTINCT r.pds, pg_typeof(r.pds) as pds_type
        FROM repos r 
        WHERE r.pds IS NOT NULL 
        LIMIT 5
    `
    
    rows3, err := postgresDB.QueryContext(ctx, sampleQuery)
    if err != nil {
        fmt.Printf("检查repos.pds字段失败: %v\n", err)
    } else {
        defer rows3.Close()
        fmt.Printf("%-30s %s\n", "PDS Value", "Data Type")
        fmt.Println(string(make([]byte, 50)))
        for rows3.Next() {
            var pdsValue, pdsType string
            if err := rows3.Scan(&pdsValue, &pdsType); err != nil {
                continue
            }
            fmt.Printf("%-30s %s\n", pdsValue, pdsType)
        }
    }

    // 检查pds表中host字段的示例值
    fmt.Println("\n=== PDS表HOST字段示例 ===")
    hostQuery := `
        SELECT DISTINCT p.host, pg_typeof(p.host) as host_type
        FROM pds p 
        WHERE p.host IS NOT NULL 
        LIMIT 5
    `
    
    rows4, err := postgresDB.QueryContext(ctx, hostQuery)
    if err != nil {
        fmt.Printf("检查pds.host字段失败: %v\n", err)
    } else {
        defer rows4.Close()
        fmt.Printf("%-40s %s\n", "Host Value", "Data Type")
        fmt.Println(string(make([]byte, 60)))
        for rows4.Next() {
            var hostValue, hostType string
            if err := rows4.Scan(&hostValue, &hostType); err != nil {
                continue
            }
            fmt.Printf("%-40s %s\n", hostValue, hostType)
        }
    }

    // 尝试检查是否有pds表的id字段与repos表的某个字段关联
    fmt.Println("\n=== 尝试查找关联关系 ===")
    
    // 检查是否repos.pds存储的是pds.id
    testQuery1 := `
        SELECT COUNT(*) as count
        FROM repos r 
        JOIN pds p ON r.pds::text = p.id::text
        LIMIT 1
    `
    
    var count1 int64
    if err := postgresDB.QueryRowContext(ctx, testQuery1).Scan(&count1); err != nil {
        fmt.Printf("测试 repos.pds = pds.id 失败: %v\n", err)
    } else {
        fmt.Printf("repos.pds = pds.id 匹配数量: %d\n", count1)
    }

    // 检查是否repos.pds存储的是pds.host
    testQuery2 := `
        SELECT COUNT(*) as count
        FROM repos r 
        JOIN pds p ON r.pds = p.host
        LIMIT 1
    `
    
    var count2 int64
    if err := postgresDB.QueryRowContext(ctx, testQuery2).Scan(&count2); err != nil {
        fmt.Printf("测试 repos.pds = pds.host 失败: %v\n", err)
    } else {
        fmt.Printf("repos.pds = pds.host 匹配数量: %d\n", count2)
    }
} 