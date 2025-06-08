#!/bin/bash
# Description:
# This script runs the bsky record downloader.
#
# Usage:
# export DOWNLOADER_WORKERS=
# export DOWNLOADER_CSV_DIR=
# export DOWNLOADER_RECORDS_DIR=
# ./run.sh
#

# Stop the script if any command fails
#set -e

echo "BlueSky Record Blob Downloader"
echo "=============================="

# 设置环境变量


export DOWNLOADER_METRICS_PORT=${DOWNLOADER_METRICS_PORT:-8080}
export DOWNLOADER_WORKERS=${DOWNLOADER_WORKERS:-2}
export DOWNLOADER_CSV_DIR=${DOWNLOADER_CSV_DIR:-/mydata/csv}
export DOWNLOADER_RECORDS_DIR=${DOWNLOADER_RECORDS_DIR:-/mydata/records}
export DOWNLOADER_FOLLOW_RECORDS_DIR=${DOWNLOADER_FOLLOW_RECORDS_DIR:-/mydata/records_follow}

# BlueSky认证信息
export BSKY_HANDLE=${BSKY_HANDLE:-"shange.bsky.social"}
export BSKY_PASSWORD=${BSKY_PASSWORD:-"zqj20030403"}

# 联系信息
export DOWNLOADER_CONTACT_INFO=${DOWNLOADER_CONTACT_INFO:-"shange0403@gmail.com"}

# 创建必要的目录
mkdir -p ${DOWNLOADER_CSV_DIR}
mkdir -p ${DOWNLOADER_RECORDS_DIR}

echo "Configuration:"
echo "  Storage Backend: FileSystem (No Database Required)"
echo "  CSV Input Directory: ${DOWNLOADER_CSV_DIR}"
echo "  Records Output Directory: ${DOWNLOADER_RECORDS_DIR}"
echo "  Workers: ${DOWNLOADER_WORKERS}"
echo "  Metrics Port: ${DOWNLOADER_METRICS_PORT}"
echo "  BlueSky Handle: ${BSKY_HANDLE}"
echo ""


# 编译程序
go mod init downloader
go mod tidy
echo "Building application..."
go build -o record_blob_downloader .

if [ $? -ne 0 ]; then
    echo "Build failed!"
    exit 1
fi

echo "Starting Record Blob Downloader..."
echo "Monitor: http://localhost:${DOWNLOADER_METRICS_PORT}/metrics"
echo "Admin API: http://localhost:${DOWNLOADER_METRICS_PORT}/"
echo "Stats: http://localhost:${DOWNLOADER_METRICS_PORT}/stats"
echo ""

# Run the program in the background with nohup,
# and use tee to both display the output and append it to a log file.
# Note that this will create a log file in the current directory.
# To specify an absolute path, change the log file name below.
nohup ./record_blob_downloader \
    -workers=${DOWNLOADER_WORKERS} \
    -csv-dir=${DOWNLOADER_CSV_DIR} \
    -records-dir=${DOWNLOADER_RECORDS_DIR} \
    -log-level=0 2>&1 | tee -a /mydata/downloader.log &