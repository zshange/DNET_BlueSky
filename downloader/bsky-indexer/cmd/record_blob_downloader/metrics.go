package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// 队列长度指标
	queueLength = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "downloader_queue_length",
		Help: "Number of items in different queues",
	}, []string{"queue_type"})

	// 用户处理指标
	usersProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "downloader_users_processed_total",
		Help: "Total number of users processed",
	}, []string{"success"})

	// 用户排队指标
	usersQueued = promauto.NewCounter(prometheus.CounterOpts{
		Name: "downloader_users_queued_total",
		Help: "Total number of users queued for processing",
	})

	// Profile下载指标
	profilesDownloaded = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "downloader_profiles_downloaded_total",
		Help: "Total number of profiles downloaded",
	}, []string{"success"})

	// Records下载指标
	recordsDownloaded = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "downloader_records_downloaded_total",
		Help: "Total number of user records downloaded",
	}, []string{"success"})

	// 工作池大小
	workerPoolSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "downloader_worker_pool_size",
		Help: "Current size of the worker pool",
	})

	// 限速等待时间
	rateLimitWaitTime = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "downloader_rate_limit_wait_seconds",
		Help: "Time spent waiting for rate limits",
		Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60, 120},
	})

	// 请求持续时间
	requestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "downloader_request_duration_seconds",
		Help: "Duration of different types of requests",
		Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60},
	}, []string{"request_type"})

	// CSV文件处理指标
	csvFilesProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "downloader_csv_files_processed_total",
		Help: "Total number of CSV files processed",
	})

	// 数据库操作指标
	databaseOperations = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "downloader_database_operations_total",
		Help: "Total number of database operations",
	}, []string{"operation", "success"})

	// 存储大小指标
	storageSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "downloader_storage_bytes",
		Help: "Size of stored data in bytes",
	}, []string{"type"})


) 