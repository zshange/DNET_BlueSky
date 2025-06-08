#!/bin/bash

echo "🚀 PDS Checker (超大表优化版) 启动脚本"
echo "=========================================="

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 检查函数
check_requirement() {
    local name=$1
    local command=$2
    local min_value=$3
    local current_value=$4
    
    if [ -n "$min_value" ] && [ -n "$current_value" ]; then
        if [ "$current_value" -lt "$min_value" ]; then
            echo -e "${RED}❌ $name: $current_value (需要至少 $min_value)${NC}"
            return 1
        else
            echo -e "${GREEN}✅ $name: $current_value${NC}"
        fi
    elif command -v $command >/dev/null 2>&1; then
        echo -e "${GREEN}✅ $name: 已安装${NC}"
    else
        echo -e "${RED}❌ $name: 未找到${NC}"
        return 1
    fi
    return 0
}

# 系统检查
echo -e "${BLUE}📋 系统环境检查${NC}"
echo "----------------------------------------"

# 检查Go版本
if command -v go >/dev/null 2>&1; then
    GO_VERSION=$(go version | grep -oE 'go[0-9]+\.[0-9]+' | sed 's/go//')
    GO_MAJOR=$(echo $GO_VERSION | cut -d. -f1)
    GO_MINOR=$(echo $GO_VERSION | cut -d. -f2)
    if [ "$GO_MAJOR" -gt 1 ] || ([ "$GO_MAJOR" -eq 1 ] && [ "$GO_MINOR" -ge 22 ]); then
        echo -e "${GREEN}✅ Go版本: $GO_VERSION${NC}"
    else
        echo -e "${RED}❌ Go版本: $GO_VERSION (需要1.22+)${NC}"
        exit 1
    fi
else
    echo -e "${RED}❌ Go: 未安装${NC}"
    exit 1
fi

# 检查内存
TOTAL_MEM=$(free -g | awk '/^Mem:/{print $2}')
AVAIL_MEM=$(free -g | awk '/^Mem:/{print $7}')
check_requirement "总内存" "" 4 $TOTAL_MEM
check_requirement "可用内存" "" 2 $AVAIL_MEM

# 检查磁盘空间
DISK_AVAIL=$(df -BG . | awk 'NR==2{gsub(/G/,"",$4); print $4}')
check_requirement "可用磁盘空间" "" 20 $DISK_AVAIL

echo ""
echo -e "${BLUE}🔌 数据库连接检查${NC}"
echo "----------------------------------------"

# 检查Docker
if command -v docker >/dev/null 2>&1; then
    echo -e "${GREEN}✅ Docker: 已安装${NC}"
    
    # 检查ScyllaDB容器
    if docker ps | grep -q scylla; then
        SCYLLA_CONTAINER=$(docker ps --format "table {{.Names}}\t{{.Status}}" | grep scylla | head -1)
        echo -e "${GREEN}✅ ScyllaDB容器: $SCYLLA_CONTAINER${NC}"
    else
        echo -e "${RED}❌ ScyllaDB容器: 未运行${NC}"
        echo -e "${YELLOW}💡 请先启动ScyllaDB容器${NC}"
        exit 1
    fi
    
    # 检查PostgreSQL容器
    if docker ps | grep -q postgres; then
        POSTGRES_CONTAINER=$(docker ps --format "table {{.Names}}\t{{.Status}}" | grep postgres | head -1)
        echo -e "${GREEN}✅ PostgreSQL容器: $POSTGRES_CONTAINER${NC}"
    else
        echo -e "${RED}❌ PostgreSQL容器: 未运行${NC}"
        echo -e "${YELLOW}💡 请先启动PostgreSQL容器${NC}"
        exit 1
    fi
else
    echo -e "${RED}❌ Docker: 未安装${NC}"
    exit 1
fi

# 创建日志目录
mkdir -p logs

echo ""
echo -e "${BLUE}⚡ 性能预估${NC}"
echo "----------------------------------------"
echo -e "${YELLOW}⏱️  预计处理时间: 2-24小时（取决于数据量）${NC}"
echo -e "${YELLOW}💾 预计内存使用: 1-4GB${NC}"
echo -e "${YELLOW}💿 预计临时存储: 5-20GB${NC}"
echo -e "${YELLOW}🔄 进度更新频率: 每10秒${NC}"

echo ""
echo -e "${BLUE}🏃 开始执行${NC}"
echo "----------------------------------------"

# 设置环境变量
export SCYLLA_HOST=${SCYLLA_HOST:-"127.0.0.1"}
export SCYLLA_PORT=${SCYLLA_PORT:-"9042"}

echo "ScyllaDB连接: $SCYLLA_HOST:$SCYLLA_PORT"
echo "PostgreSQL连接: localhost:15432"
echo ""

# 询问是否继续
echo -e "${YELLOW}⚠️  注意: 这是一个长时间运行的进程，可能需要数小时完成${NC}"
echo -e "${YELLOW}   处理过程中请保持网络连接稳定，避免中断程序${NC}"
echo ""
read -p "是否继续执行? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "已取消执行"
    exit 0
fi

echo ""
echo -e "${GREEN}🎯 开始处理超大表数据...${NC}"
echo "日志文件: logs/pds_checker_$(date +%Y%m%d_%H%M%S).log"

# 创建日志文件名
LOG_FILE="logs/pds_checker_$(date +%Y%m%d_%H%M%S).log"

# 运行程序并记录日志
if go run main.go 2>&1 | tee "$LOG_FILE"; then
    echo ""
    echo -e "${GREEN}🎉 处理完成!${NC}"
    echo "日志文件已保存: $LOG_FILE"
    
    # 显示结果摘要
    if [ -f "$LOG_FILE" ]; then
        echo ""
        echo -e "${BLUE}📊 处理结果摘要${NC}"
        echo "----------------------------------------"
        grep -E "(处理记录数|发现唯一repos|最终保留的repos数|总用时)" "$LOG_FILE" | tail -10
    fi
else
    echo ""
    echo -e "${RED}❌ 处理失败，请检查日志文件: $LOG_FILE${NC}"
    exit 1
fi

echo ""
echo -e "${BLUE}🔍 下一步建议${NC}"
echo "----------------------------------------"
echo "1. 检查新创建的表: repos_with_stats"
echo "2. 验证数据完整性"
echo "3. 根据需要创建额外的索引"
echo "4. 考虑备份结果表"

echo ""
echo -e "${GREEN}✨ 脚本执行完成!${NC}" 