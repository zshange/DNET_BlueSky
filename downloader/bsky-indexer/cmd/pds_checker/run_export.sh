#!/bin/bash

echo "Repos CSV导出工具启动脚本"
echo "=========================="

# 确保输出目录存在
mkdir -p /mydata/csv

# 切换到程序目录
cd "$(dirname "$0")"

# 编译并运行程序
echo "正在编译程序..."
go build -o export_repos_csv export_repos_csv.go

if [ $? -eq 0 ]; then
    echo "编译成功，开始运行..."
    echo ""
    ./export_repos_csv
else
    echo "编译失败，请检查Go环境和代码"
    exit 1
fi

echo ""
echo "运行完成。查看输出文件："
ls -la /mydata/csv/

echo ""
echo "CSV文件统计："
echo "文件数量: $(ls -1 /mydata/csv/*.csv 2>/dev/null | wc -l)"
echo "总大小: $(du -sh /mydata/csv/ 2>/dev/null | cut -f1)" 