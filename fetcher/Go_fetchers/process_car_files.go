package main

import (
    "fmt"
    "os"
    "path/filepath"
    "strings"
)

func main() {
    // 设置 repo_files 目录的相对路径
    repoFilesDir := "../../repo_files"
    
    // 获取绝对路径以便打印
    absPath, err := filepath.Abs(repoFilesDir)
    if err != nil {
        fmt.Printf("Error getting absolute path: %v\n", err)
        return
    }
    
    fmt.Printf("Scanning directory: %s\n", absPath)
    
    // 遍历目录
    carFiles, err := scanCarFiles(repoFilesDir)
    if err != nil {
        fmt.Printf("Error scanning directory: %v\n", err)
        return
    }
    
    // 处理找到的 .car 文件
    processCarFiles(carFiles)
}

// 扫描目录及其子目录，找出所有 .car 文件
func scanCarFiles(dirPath string) ([]string, error) {
    var carFiles []string
    
    // 检查目录是否存在
    _, err := os.Stat(dirPath)
    if os.IsNotExist(err) {
        return nil, fmt.Errorf("directory does not exist: %s", dirPath)
    }
    
    // 遍历目录中的所有文件
    err = filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
        if err != nil {
            return err
        }
        
        // 跳过目录，只处理文件
        if info.IsDir() {
            return nil
        }
        
        // 检查文件是否以 .car 结尾
        if strings.HasSuffix(info.Name(), ".car") {
            carFiles = append(carFiles, path)
        }
        
        return nil
    })
    
    if err != nil {
        return nil, err
    }
    
    return carFiles, nil
}

// 处理找到的 .car 文件
func processCarFiles(carFiles []string) {
    if len(carFiles) == 0 {
        fmt.Println("No .car files found")
        return
    }
    
    fmt.Printf("Found %d .car files:\n", len(carFiles))
    
    // 遍历并处理每个 .car 文件
    for i, filePath := range carFiles {
        fmt.Printf("%d. Processing file: %s\n", i+1, filePath)
        
        // 这里可以添加你的处理逻辑
        // 例如，读取文件内容、解析 CAR 数据等
        
        // 示例：获取文件名（不含路径）
        fileName := filepath.Base(filePath)
        
        // 示例：赋值操作 - 创建一个新的变量来存储文件路径
        carPath := filePath
        fmt.Printf("   Assigned carPath = %s\n", carPath)
        
        // 这里可以添加更多的处理代码
        // 例如：
        // - 读取文件内容
        // - 解析 CAR 文件
        // - 提取元数据
        // - 等等
    }
} 