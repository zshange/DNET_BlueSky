package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	_ "github.com/joho/godotenv/autoload"
	"github.com/kelseyhightower/envconfig"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
)

type Config struct {
	LogFile          string
	LogFormat        string `default:"text"`
	LogLevel         int64  `default:"1"`
	MetricsPort      string `split_words:"true"`
	Workers          int    `default:"1"`  // 固定默认worker数量
	ContactInfo      string `split_words:"true"`
	CSVInputDir      string `default:"/mydata/csv"`
	RecordsBaseDir   string `default:"/mydata/records_1"`
	FollowRecordsDir string `default:"/mydata/records_follow"` // 符合followers标准的用户额外存储目录
}

var config Config

func runMain(ctx context.Context) error {
	ctx = setupLogging(ctx)
	log := zerolog.Ctx(ctx)
	
	log.Info().Msg("🚀 启动 BlueSky Record Blob Downloader")
	log.Info().Msg("======================================")
	
	// 打印所有配置信息
	log.Info().Msg("📋 系统配置:")
	log.Info().Str("csv_input_dir", config.CSVInputDir).Msg("  📁 CSV输入目录")
	log.Info().Str("records_base_dir", config.RecordsBaseDir).Msg("  📁 记录存储目录")
	log.Info().Str("follow_records_dir", config.FollowRecordsDir).Msg("  📁 高followers用户存储目录")
	log.Info().Int("workers", config.Workers).Msg("  👷 工作线程数")
	log.Info().Str("metrics_port", config.MetricsPort).Msg("  📊 指标端口")

	log.Info().Str("contact_info", config.ContactInfo).Msg("  📧 联系信息")
	log.Info().Str("log_level", zerolog.Level(config.LogLevel).String()).Msg("  📝 日志级别")

	if config.ContactInfo == "" {
		config.ContactInfo = "<contact info unspecified>"
		log.Warn().Msg("⚠️ 未指定联系信息，建议设置DOWNLOADER_CONTACT_INFO环境变量")
	}

	// 验证和创建目录
	log.Info().Msg("🔧 初始化存储目录...")
	if err := os.MkdirAll(config.RecordsBaseDir, 0755); err != nil {
		return fmt.Errorf("failed to create records directory: %w", err)
	}
	log.Info().Str("path", config.RecordsBaseDir).Msg("✅ 记录存储目录创建成功")

	if err := os.MkdirAll(config.FollowRecordsDir, 0755); err != nil {
		return fmt.Errorf("failed to create follow records directory: %w", err)
	}
	log.Info().Str("path", config.FollowRecordsDir).Msg("✅ 高followers用户存储目录创建成功")

	// 创建文件系统管理器
	log.Info().Msg("💾 初始化文件系统管理器...")
	fsManager := NewFileSystemManager(config.RecordsBaseDir)
	log.Info().Msg("✅ 文件系统管理器初始化完成")

	// Create rate limiter
	log.Info().Msg("Creating rate limiter...")
	limiter := NewLimiterWithConfig()
	log.Info().Msg("Rate limiter initialized")
	
	// Configuration summary
	log.Info().Msg("System configuration confirmed:")
	log.Info().Int("workers", config.Workers).Msg("Using fixed configuration")

	// 创建工作通道和线程池
	log.Info().Msg("👷 初始化工作线程池...")
	ch := make(chan WorkItem, 2000) // 缓冲区
	pool := NewWorkerPool(ch, fsManager, config.Workers, limiter, config.ContactInfo, config.RecordsBaseDir, config.FollowRecordsDir)
	if err := pool.Start(ctx); err != nil {
		return fmt.Errorf("failed to start worker pool: %w", err)
	}
	log.Info().Int("workers", config.Workers).Msg("✅ 工作线程池启动成功")

	// 创建调度器
	log.Info().Msg("📅 初始化任务调度器...")
	scheduler := NewScheduler(ch, fsManager, config.CSVInputDir)
	if err := scheduler.Start(ctx); err != nil {
		return fmt.Errorf("failed to start scheduler: %w", err)
	}
	log.Info().Str("csv_dir", config.CSVInputDir).Msg("✅ 任务调度器启动成功")

	// 启动HTTP服务器（只用于metrics和基本状态）
	log.Info().Str("port", config.MetricsPort).Msg("🌐 启动HTTP服务器...")
	AddAdminHandlers(fsManager)
	http.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Addr: fmt.Sprintf(":%s", config.MetricsPort)}
	
	log.Info().Msg("🚀 系统启动完成，开始处理任务...")
	log.Info().Msg("======================================")
	
	errCh := make(chan error)
	go func() {
		errCh <- srv.ListenAndServe()
	}()
	select {
	case <-ctx.Done():
		log.Info().Msg("🛑 接收到关闭信号，正在优雅关闭...")
		
		// 确保异步统计更新系统正确关闭
		fsManager.Close()
		
		if err := srv.Shutdown(context.Background()); err != nil {
			return fmt.Errorf("HTTP server shutdown failed: %w", err)
		}
		log.Info().Msg("✅ 系统已安全关闭")
	}
	return <-errCh
}

func main() {
	flag.StringVar(&config.LogFile, "log", "", "Path to the log file. If empty, will log to stderr")
	flag.StringVar(&config.LogFormat, "log-format", "text", "Logging format. 'text' or 'json'")
	flag.Int64Var(&config.LogLevel, "log-level", 1, "Log level. -1 - trace, 0 - debug, 1 - info, 5 - panic")
	flag.IntVar(&config.Workers, "workers", 1, "Number of workers (fixed)")
	flag.StringVar(&config.CSVInputDir, "csv-dir", "/mydata/csv", "Directory containing CSV files")
	flag.StringVar(&config.RecordsBaseDir, "records-dir", "/mydata/records", "Directory to store downloaded records")
	flag.StringVar(&config.FollowRecordsDir, "follow-records-dir", "/mydata/records_follow", "Directory to store records for users meeting followers threshold")

	if err := envconfig.Process("downloader", &config); err != nil {
		log.Fatalf("envconfig.Process: %s", err)
	}

	flag.Parse()

	ctx, _ := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	if err := runMain(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func setupLogging(ctx context.Context) context.Context {
	logFile := os.Stderr

	if config.LogFile != "" {
		f, err := os.OpenFile(config.LogFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalf("Failed to open the specified log file %q: %s", config.LogFile, err)
		}
		logFile = f
	}

	var output io.Writer

	switch config.LogFormat {
	case "json":
		output = logFile
	case "text":
		prefixList := []string{}
		info, ok := debug.ReadBuildInfo()
		if ok {
			prefixList = append(prefixList, info.Path+"/")
		}

		basedir := ""
		_, sourceFile, _, ok := runtime.Caller(0)
		if ok {
			basedir = filepath.Dir(sourceFile)
		}

		if basedir != "" && strings.HasPrefix(basedir, "/") {
			prefixList = append(prefixList, basedir+"/")
			head, _ := filepath.Split(basedir)
			for head != "/" {
				prefixList = append(prefixList, head)
				head, _ = filepath.Split(strings.TrimSuffix(head, "/"))
			}
		}

		output = zerolog.ConsoleWriter{
			Out:        logFile,
			NoColor:    true,
			TimeFormat: time.RFC3339,
			PartsOrder: []string{
				zerolog.LevelFieldName,
				zerolog.TimestampFieldName,
				zerolog.CallerFieldName,
				zerolog.MessageFieldName,
			},
			FormatFieldName:  func(i interface{}) string { return fmt.Sprintf("%s:", i) },
			FormatFieldValue: func(i interface{}) string { return fmt.Sprintf("%s", i) },
			FormatCaller: func(i interface{}) string {
				s := i.(string)
				for _, p := range prefixList {
					s = strings.TrimPrefix(s, p)
				}
				return s
			},
		}
	default:
		log.Fatalf("Invalid log format specified: %q", config.LogFormat)
	}

	logger := zerolog.New(output).Level(zerolog.Level(config.LogLevel)).With().Caller().Timestamp().Logger()

	ctx = logger.WithContext(ctx)

	zerolog.DefaultContextLogger = &logger
	log.SetOutput(logger)

	return ctx
}
