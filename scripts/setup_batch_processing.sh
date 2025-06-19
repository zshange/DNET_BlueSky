#!/bin/bash

# 脚本配置
CSV_SOURCE_DIR="/mydata/csv"
SOURCE_PROGRAM_DIR="record_downloader"
WORK_BASE_DIR="/mydata/batch_processing"
FILES_PER_GROUP=16

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

# 检查必要的目录和文件
check_prerequisites() {
    log_step "检查先决条件..."
    
    if [ ! -d "$CSV_SOURCE_DIR" ]; then
        log_error "CSV源目录不存在: $CSV_SOURCE_DIR"
        exit 1
    fi
    
    if [ ! -d "$SOURCE_PROGRAM_DIR" ]; then
        log_error "程序源目录不存在: $SOURCE_PROGRAM_DIR"
        exit 1
    fi
    
    # 统计CSV文件数量
    csv_count=$(find "$CSV_SOURCE_DIR" -name "*.csv" -type f | wc -l)
    if [ "$csv_count" -eq 0 ]; then
        log_error "CSV源目录中没有找到CSV文件"
        exit 1
    fi
    
    log_info "找到 $csv_count 个CSV文件"
    log_info "每组 $FILES_PER_GROUP 个文件，预计创建 $(( (csv_count + FILES_PER_GROUP - 1) / FILES_PER_GROUP )) 个处理组"
}

# 创建工作目录结构
create_work_structure() {
    log_step "创建工作目录结构..."
    
    # 创建基础工作目录
    mkdir -p "$WORK_BASE_DIR"
    sudo rm -rf "$WORK_BASE_DIR"
    sudo chmod -R 777 "$WORK_BASE_DIR"
    # 备份现有的处理组（如果存在）
    if [ -d "$WORK_BASE_DIR" ] && [ "$(ls -A $WORK_BASE_DIR 2>/dev/null | grep -E '^group_[0-9]+$')" ]; then
        backup_dir="$WORK_BASE_DIR/backup_$(date +%Y%m%d_%H%M%S)"
        log_warn "发现现有处理组，备份到: $backup_dir"
        mkdir -p "$backup_dir"
        mv "$WORK_BASE_DIR"/group_* "$backup_dir/" 2>/dev/null || true
    fi
    
    log_info "工作目录准备完成: $WORK_BASE_DIR"
}

# 分组并设置处理实例
setup_processing_groups() {
    log_step "开始分组和设置处理实例..."
    
    # 获取所有CSV文件并排序
    mapfile -t csv_files < <(find "$CSV_SOURCE_DIR" -name "*.csv" -type f | sort)
    total_files=${#csv_files[@]}
    
    if [ "$total_files" -eq 0 ]; then
        log_error "没有找到CSV文件"
        exit 1
    fi
    
    # 计算组数
    total_groups=$(( (total_files + FILES_PER_GROUP - 1) / FILES_PER_GROUP ))
    
    log_info "开始处理 $total_files 个文件，分为 $total_groups 组"
    
    # 为每组创建处理实例
    for ((group=1; group<=total_groups; group++)); do
        group_dir="$WORK_BASE_DIR/group_$group"
        
        log_info "正在设置第 $group 组 (目录: $group_dir)..."
        
        # 创建组目录结构
        mkdir -p "$group_dir"
        mkdir -p "$group_dir/csv"
        mkdir -p "$group_dir/records"
        mkdir -p "$group_dir/records_follow"
        mkdir -p "$group_dir/logs"
        
        # 复制程序文件
        log_info "  复制程序文件..."
        cp -r "$SOURCE_PROGRAM_DIR" "$group_dir/downloader"
        
        # 计算当前组的文件范围
        start_idx=$(( (group - 1) * FILES_PER_GROUP ))
        end_idx=$(( start_idx + FILES_PER_GROUP - 1 ))
        if [ "$end_idx" -ge "$total_files" ]; then
            end_idx=$(( total_files - 1 ))
        fi
        
        # 复制CSV文件到组目录
        files_in_group=0
        for ((i=start_idx; i<=end_idx; i++)); do
            if [ "$i" -lt "$total_files" ]; then
                csv_file="${csv_files[$i]}"
                filename=$(basename "$csv_file")
                cp "$csv_file" "$group_dir/csv/"
                files_in_group=$((files_in_group + 1))
            fi
        done
        
        # 创建组特定的启动脚本
        cat > "$group_dir/run_group.sh" << EOF
#!/bin/bash

# 第 $group 组处理脚本
# 包含 $files_in_group 个CSV文件

GROUP_DIR="\$(cd "\$(dirname "\${BASH_SOURCE[0]}")" && pwd)"
DOWNLOADER_DIR="\$GROUP_DIR/downloader"

echo "🚀 启动第 $group 组处理器..."
echo "📁 工作目录: \$GROUP_DIR"
echo "📊 CSV文件数: $files_in_group"
echo "================================"

cd "\$DOWNLOADER_DIR"

# 设置环境变量
export DOWNLOADER_CONTACT_INFO="bsky-batch-processor-group-$group"

# 启动处理器
nohup ./record_blob_downloader \\
    --csv-dir="\$GROUP_DIR/csv" \\
    --records-dir="\$GROUP_DIR/records" \\
    --follow-records-dir="\$GROUP_DIR/records_follow" \\
    --workers=2 \\
    --log-level=1 \\
    --metrics-port=808$group \\
    2>&1 | tee "/mydata/downloader.log"

echo "✅ 第 $group 组处理完成"
EOF
        
        chmod +x "$group_dir/run_group.sh"
        
        # 创建状态查看脚本
        cat > "$group_dir/status.sh" << EOF
#!/bin/bash

GROUP_DIR="\$(cd "\$(dirname "\${BASH_SOURCE[0]}")" && pwd)"

echo "📊 第 $group 组状态报告"
echo "======================="
echo "📁 CSV文件数: \$(find "\$GROUP_DIR/csv" -name "*.csv" | wc -l)"
echo "👥 已处理用户: \$(find "\$GROUP_DIR/records" -name "profile.json" | wc -l)"
echo "⭐ 高followers用户: \$(find "\$GROUP_DIR/records_follow" -name "profile.json" | wc -l)"
echo "📈 指标端点: http://localhost:808$group/metrics"
echo ""

# 检查进程状态
if pgrep -f "metrics-port=808$group" > /dev/null; then
    echo "🟢 处理器状态: 运行中"
else
    echo "🔴 处理器状态: 未运行"
fi
EOF
        
        chmod +x "$group_dir/status.sh"
        
        log_info "  第 $group 组设置完成 ($files_in_group 个文件)"
    done
}

# 创建全局管理脚本
create_management_scripts() {
    log_step "创建全局管理脚本..."
    
    # 创建启动所有组的脚本
    cat > "$WORK_BASE_DIR/start_all_groups.sh" << 'EOF'
#!/bin/bash

WORK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "🚀 启动所有处理组..."

for group_dir in "$WORK_DIR"/group_*; do
    if [ -d "$group_dir" ] && [ -f "$group_dir/run_group.sh" ]; then
        group_name=$(basename "$group_dir")
        echo "启动 $group_name..."
        
        # 在后台运行每个组
        (
            cd "$group_dir"
            nohup ./run_group.sh > /dev/null 2>&1 &
            echo $! > "$group_dir/process.pid"
        )
        
        sleep 2  # 给每个组一些启动时间
    fi
done

echo "✅ 所有组已启动"
echo "📊 使用 ./status_all.sh 查看状态"
EOF
    
    # 创建停止所有组的脚本
    cat > "$WORK_BASE_DIR/stop_all_groups.sh" << 'EOF'
#!/bin/bash

WORK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "🛑 停止所有处理组..."

for group_dir in "$WORK_DIR"/group_*; do
    if [ -d "$group_dir" ] && [ -f "$group_dir/process.pid" ]; then
        group_name=$(basename "$group_dir")
        pid=$(cat "$group_dir/process.pid" 2>/dev/null)
        
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            echo "停止 $group_name (PID: $pid)..."
            kill "$pid"
            rm -f "$group_dir/process.pid"
        else
            echo "$group_name 未运行"
        fi
    fi
done

echo "✅ 所有组已停止"
EOF
    
    # 创建状态查看脚本
    cat > "$WORK_BASE_DIR/status_all.sh" << 'EOF'
#!/bin/bash

WORK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "📊 所有处理组状态报告"
echo "=============================="

total_csv=0
total_processed=0
total_follow=0
running_groups=0

for group_dir in "$WORK_DIR"/group_*; do
    if [ -d "$group_dir" ]; then
        group_name=$(basename "$group_dir")
        
        csv_count=$(find "$group_dir/csv" -name "*.csv" 2>/dev/null | wc -l)
        processed_count=$(find "$group_dir/records" -name "profile.json" 2>/dev/null | wc -l)
        follow_count=$(find "$group_dir/records_follow" -name "profile.json" 2>/dev/null | wc -l)
        
        total_csv=$((total_csv + csv_count))
        total_processed=$((total_processed + processed_count))
        total_follow=$((total_follow + follow_count))
        
        # 检查运行状态
        status="🔴 停止"
        if [ -f "$group_dir/process.pid" ]; then
            pid=$(cat "$group_dir/process.pid" 2>/dev/null)
            if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
                status="🟢 运行"
                running_groups=$((running_groups + 1))
            fi
        fi
        
        echo "$group_name: $status | CSV:$csv_count | 已处理:$processed_count | 高followers:$follow_count"
    fi
done

echo "=============================="
echo "📈 总计: CSV文件:$total_csv | 已处理:$total_processed | 高followers:$total_follow"
echo "🏃 运行中的组: $running_groups"
EOF
    
    # 设置执行权限
    chmod +x "$WORK_BASE_DIR"/*.sh
    
    log_info "管理脚本创建完成"
}

# 显示使用说明
show_usage_instructions() {
    log_step "设置完成！使用说明："
    echo ""
    echo "📁 工作目录: $WORK_BASE_DIR"
    echo ""
    echo "🔧 管理命令:"
    echo "  启动所有组: cd $WORK_BASE_DIR && ./start_all_groups.sh"
    echo "  停止所有组: cd $WORK_BASE_DIR && ./stop_all_groups.sh"
    echo "  查看状态:   cd $WORK_BASE_DIR && ./status_all.sh"
    echo ""
    echo "📊 单个组操作:"
    echo "  启动单组:   cd $WORK_BASE_DIR/group_N && ./run_group.sh"
    echo "  查看状态:   cd $WORK_BASE_DIR/group_N && ./status.sh"
    echo ""
    echo "📈 监控端点:"
    for ((i=1; i<=total_groups; i++)); do
        echo "  第${i}组: http://localhost:808${i}/metrics"
    done
    echo ""
    
    # 显示目录结构示例
    if [ -d "$WORK_BASE_DIR/group_1" ]; then
        echo "📁 目录结构示例:"
        tree "$WORK_BASE_DIR/group_1" -L 2 2>/dev/null || {
            find "$WORK_BASE_DIR/group_1" -type d | head -10 | sed 's|^|  |'
        }
    fi
}

# 主执行流程
main() {
    echo "🚀 BlueSky 批处理设置脚本"
    echo "=========================="
    
    check_prerequisites
    create_work_structure
    setup_processing_groups
    create_management_scripts
    show_usage_instructions
    
    log_info "批处理环境设置完成！"
}

# 执行主函数
main "$@" 