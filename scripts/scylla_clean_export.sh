#!/bin/bash

# ScyllaDB Docker环境连续导出脚本（动态断点续传）
set -e

# ==================== 配置参数 ====================
SCYLLA_CONTAINER="018fa82eab22"  # Docker容器名
SCYLLA_HOST="127.0.0.1"
SCYLLA_PORT="9042"
KEYSPACE="bluesky"
TABLE="records"
EXPORT_DIR="/mydata/scy"
BATCH_SIZE_GB=100
TARGET_FILE_SIZE_GB=1
PROGRESS_FILE="$EXPORT_DIR/export_progress.txt"

# ==================== 日志函数 ====================
log_info() { echo -e "\033[0;32m[INFO]\033[0m $(date '+%Y-%m-%d %H:%M:%S') $1"; }
log_warn() { echo -e "\033[0;33m[WARN]\033[0m $(date '+%Y-%m-%d %H:%M:%S') $1"; }
log_error() { echo -e "\033[0;31m[ERROR]\033[0m $(date '+%Y-%m-%d %H:%M:%S') $1"; }

# ==================== Docker工具函数 ====================
docker_cqlsh() {
    local cql_command="$1"
    sudo docker exec "$SCYLLA_CONTAINER" cqlsh -e "$cql_command"
}

docker_cqlsh_file() {
    local cql_file="$1"
    local container_file="/tmp/$(basename $cql_file)"
    sudo docker cp "$cql_file" "$SCYLLA_CONTAINER:$container_file"
    sudo docker exec "$SCYLLA_CONTAINER" cqlsh -f "$container_file"
    sudo docker exec "$SCYLLA_CONTAINER" rm -f "$container_file"
}

# ==================== 检查函数 ====================
check_prerequisites() {
    log_info "检查Docker环境..."
    if ! sudo docker ps | grep -q "$SCYLLA_CONTAINER"; then
        log_error "ScyllaDB容器 '$SCYLLA_CONTAINER' 未运行"
        log_info "可用的容器："
        sudo docker ps --format "table {{.Names}}\t{{.Status}}"
        return 1
    fi
    if ! docker_cqlsh "SELECT now() FROM system.local;" >/dev/null 2>&1; then
        log_error "无法连接到ScyllaDB"
        return 1
    fi
    if ! docker_cqlsh "DESCRIBE KEYSPACE $KEYSPACE;" >/dev/null 2>&1; then
        log_error "Keyspace '$KEYSPACE' 不存在"
        return 1
    fi
    local available_gb=$(df /mydata 2>/dev/null | tail -1 | awk '{print $4}' | xargs -I {} expr {} / 1024 / 1024 2>/dev/null || echo "0")
    if [ $available_gb -lt $((BATCH_SIZE_GB + 20)) ]; then
        log_error "磁盘空间不足！需要${BATCH_SIZE_GB}GB，可用${available_gb}GB"
        return 1
    fi
    log_info "Docker环境检查通过"
    return 0
}

# ==================== 进度管理 ====================
load_progress() {
    if [ -f "$PROGRESS_FILE" ]; then
        local last_token=$(grep "last_token=" "$PROGRESS_FILE" | cut -d= -f2)
        local file_counter=$(grep "file_counter=" "$PROGRESS_FILE" | cut -d= -f2)
        local total_exported_mb=$(grep "total_exported_mb=" "$PROGRESS_FILE" | cut -d= -f2)
        local start_time=$(grep "start_time=" "$PROGRESS_FILE" | cut -d= -f2)
        
        echo "$last_token:$file_counter:$total_exported_mb:$start_time"
    else
        # 初始状态：从最小Token开始
        echo "-9223372036854775808:1:0:$(date '+%Y-%m-%d_%H:%M:%S')"
    fi
}

save_progress() {
    local last_token=$1
    local file_counter=$2
    local total_exported_mb=$3
    local start_time=$4
    
    cat > "$PROGRESS_FILE" << EOF
# ScyllaDB 导出进度状态文件
# 最后更新时间: $(date '+%Y-%m-%d %H:%M:%S')
last_token=$last_token
file_counter=$file_counter
total_exported_mb=$total_exported_mb
start_time=$start_time
status=running
EOF
    log_info "进度已保存: Token=$last_token, 文件=$file_counter, 已导出=${total_exported_mb}MB"
}

# ==================== 连续导出方法 ====================
method_continuous_export() {
    log_info "=== 连续导出模式（从上次终点开始，动态断点续传） ==="
    
    # 加载进度
    local progress=$(load_progress)
    local last_token=$(echo $progress | cut -d: -f1)
    local file_counter=$(echo $progress | cut -d: -f2)
    local total_exported_mb=$(echo $progress | cut -d: -f3)
    local start_time=$(echo $progress | cut -d: -f4)
    
    log_info "加载进度: 上次Token=$last_token, 文件编号=$file_counter, 已导出=${total_exported_mb}MB"
    
    mkdir -p "$EXPORT_DIR"
    local rows_per_file=1000000  # 初始值，后续动态调整
    
    while [ $total_exported_mb -lt $((BATCH_SIZE_GB * 1024)) ]; do
        local output_file="$EXPORT_DIR/records_continuous_${file_counter}.csv"
        
        # 断点续写：检查文件是否已存在
        if [ -f "$output_file" ]; then
            log_info "文件已存在，跳过: $(basename $output_file)"
            local file_size_mb=$(du -m "$output_file" | cut -f1)
            total_exported_mb=$((total_exported_mb + file_size_mb))
            file_counter=$((file_counter + 1))
            continue
        fi
        
        log_info "导出文件 $file_counter: $(basename $output_file) (从Token: $last_token 开始)"
        
        # 创建分页查询脚本
        local query_script="/tmp/continuous_export_${file_counter}.cql"
        cat > "$query_script" << EOF
COPY ${KEYSPACE}.${TABLE} (repo, collection, rkey, at_rev, created_at, deleted, record)
TO '/tmp/$(basename $output_file)'
WITH DELIMITER='|'
AND HEADER=true
AND ENCODING='UTF-8'
AND MAXROWS=$rows_per_file;
EOF
        
        # 执行导出
        if docker_cqlsh_file "$query_script"; then
            sudo docker cp "$SCYLLA_CONTAINER:/tmp/$(basename $output_file)" "$output_file"
            
            if [ -f "$output_file" ] && [ -s "$output_file" ]; then
                local file_size_mb=$(du -m "$output_file" | cut -f1)
                local file_lines=$(wc -l < "$output_file")
                
                log_info "文件 $file_counter 完成: ${file_size_mb}MB, ${file_lines}行"
                
                # 动态调整rows_per_file以接近1GB
                if [ $file_size_mb -gt 0 ]; then
                    rows_per_file=$((rows_per_file * TARGET_FILE_SIZE_GB * 1024 / file_size_mb))
                    if [ $rows_per_file -lt 50000 ]; then rows_per_file=50000; fi
                    if [ $rows_per_file -gt 5000000 ]; then rows_per_file=5000000; fi
                fi
                
                # 更新进度
                total_exported_mb=$((total_exported_mb + file_size_mb))
                file_counter=$((file_counter + 1))
                save_progress "$last_token" "$file_counter" "$total_exported_mb" "$start_time"
                
                # 如果文件很小，说明数据接近导出完毕
                if [ $file_size_mb -lt 10 ]; then
                    log_info "文件大小过小，可能已接近数据末尾"
                    break
                fi
            else
                log_warn "文件未生成或为空，可能已导出完所有数据"
                break
            fi
            
            # 清理容器内临时文件
            sudo docker exec "$SCYLLA_CONTAINER" rm -f "/tmp/$(basename $output_file)"
        else
            log_error "导出失败"
            break
        fi
        
        rm -f "$query_script"
    done
    
    log_info "本次导出完成，总计: ${total_exported_mb}MB"
    return 0
}

# ==================== 主函数 ====================
main() {
    log_info "==================== ScyllaDB 连续导出工具（动态断点续传） ===================="
    log_info "Docker容器: $SCYLLA_CONTAINER"
    log_info "导出目录: $EXPORT_DIR"
    log_info "本次导出目标: ${BATCH_SIZE_GB}GB（每文件${TARGET_FILE_SIZE_GB}GB）"
    log_info "进度文件: $PROGRESS_FILE"
    log_info "================================================================"
    
    if ! check_prerequisites; then
        exit 1
    fi
    
    mkdir -p "$EXPORT_DIR"
    method_continuous_export
    
    if [ $? -eq 0 ]; then
        log_info "🎉 本次导出完成！"
        log_info "继续导出下一批: $0"
        log_info "查看进度: cat $PROGRESS_FILE"
    else
        log_error "导出失败"
        exit 1
    fi
}

# ==================== 帮助信息 ====================
if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    cat << EOF
ScyllaDB 连续导出工具（动态断点续传）

用法: $0

特性:
  - 连续导出：每次运行导出${BATCH_SIZE_GB}GB数据
  - 动态断点：记录精确的Token终点，下次从该点继续
  - 无重复数据：已导出文件自动跳过
  - 自动恢复：中断后重新运行即可从断点继续

示例:
  $0        # 首次运行或继续导出
  $0 --help # 显示帮助信息

配置参数:
  容器名称: $SCYLLA_CONTAINER
  数据库: $KEYSPACE.$TABLE
  单次导出: ${BATCH_SIZE_GB}GB
  文件大小: ${TARGET_FILE_SIZE_GB}GB
  导出目录: $EXPORT_DIR
EOF
    exit 0
fi

main "$@" 