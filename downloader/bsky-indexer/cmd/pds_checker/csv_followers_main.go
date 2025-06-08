package main

import (
    "fmt"
    "os"
)

func main() {
    // 检查命令行参数
    if len(os.Args) < 2 {
        fmt.Println("用法: go run csv_followers_main.go pds_follows.go <CSV目录路径>")
        fmt.Println("例如: go run csv_followers_main.go pds_follows.go /mydata/csv_repos")
        os.Exit(1)
    }
    
    csvDir := os.Args[1]
    
    // 检查目录是否存在
    if _, err := os.Stat(csvDir); os.IsNotExist(err) {
        fmt.Printf("错误: 目录不存在: %s\n", csvDir)
        os.Exit(1)
    }
    
    fmt.Printf("开始处理CSV目录: %s\n", csvDir)
    
    // 调用处理函数
    if err := ProcessCSVFilesStandalone(csvDir); err != nil {
        fmt.Printf("处理失败: %v\n", err)
        os.Exit(1)
    }
    
    fmt.Println("�� 所有CSV文件处理完成!")
} 