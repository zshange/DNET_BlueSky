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

	// comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	_ "github.com/joho/godotenv/autoload"
	"github.com/kelseyhightower/envconfig"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	// "github.com/bluesky-social/indigo/xrpc"
	// "github.com/uabluerail/bsky-tools/xrpcauth"
	// "github.com/uabluerail/indexer/models"
	// "github.com/uabluerail/indexer/pds"
	"github.com/uabluerail/indexer/repo"
	"github.com/uabluerail/indexer/util/gormzerolog"
	// "github.com/uabluerail/indexer/util/resolver"
)

type Config struct {
	LogFile      string
	LogFormat    string `default:"text"`
	LogLevel     int64  `default:"1"`
	MetricsPort  string `split_words:"true"`
	DBUrl        string `default:"postgres://postgres:DNET_ZQJ@localhost:15432/bluesky?sslmode=disable"`
	Workers      int    `default:"10"`
	DIDFilter    string `split_words:"true" default:""`
	ContactInfo  string `split_words:"true" default:"shange040@gmail.com"`
	BatchSize    int    `default:"100" split_words:"true"`
}

// FilteredRepo 是筛选后的仓库数据
type FilteredRepo struct {
	ID         int64     `gorm:"primarykey;column:id"`
	CreatedAt  time.Time	 `gorm:"column:created_at"`
	UpdatedAt  time.Time	 `gorm:"column:updated_at"`
	FlDid        string    `gorm:"column:did"`
	FetchedAt  time.Time	 `gorm:"column:fetched_at"`
	PDS        int64   `gorm:"column:pds"`
}
func (FilteredRepo) TableName() string {
    return "filtered_repos"
}


var config Config

func runMain(ctx context.Context) error {
	ctx = setupLogging(ctx)
	log := zerolog.Ctx(ctx)
	log.Debug().Msgf("Starting up...")

	// 连接数据库
	dbCfg, err := pgxpool.ParseConfig(config.DBUrl)
	if err != nil {
		return fmt.Errorf("parsing DB URL: %w", err)
	}
	dbCfg.MaxConns = 1024
	dbCfg.MinConns = 24
	dbCfg.MaxConnLifetime = 6 * time.Hour
	conn, err := pgxpool.NewWithConfig(ctx, dbCfg)
	if err != nil {
		return fmt.Errorf("connecting to postgres: %w", err)
	}
	sqldb := stdlib.OpenDBFromPool(conn)

	db, err := gorm.Open(postgres.New(postgres.Config{
		Conn: sqldb,
	}), &gorm.Config{
		SkipDefaultTransaction: true,
		PrepareStmt:            true,
		Logger: gormzerolog.New(&logger.Config{
			SlowThreshold:             3 * time.Second,
			IgnoreRecordNotFoundError: true,
		}, nil),
	})
	if err != nil {
		return fmt.Errorf("connecting to the database: %w", err)
	}
	log.Debug().Msgf("DB connection established")

	// 检查 filtered_repos 表中已有的记录数
	var existingCount int64
	db.Model(&FilteredRepo{}).Count(&existingCount)
	if existingCount >= 10000 {
		log.Info().Msgf("filtered_repos already has %d records, exiting.", existingCount)
		return nil
	}

	// 启动指标服务器
	log.Info().Msgf("Starting HTTP listener on %q...", config.MetricsPort)
	http.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Addr: fmt.Sprintf(":%s", config.MetricsPort)}
	errCh := make(chan error)
	go func() {
		errCh <- srv.ListenAndServe()
	}()

	// 启动工作协程
	workCh := make(chan string, config.Workers*2)
	doneCh := make(chan struct{})

	// 启动工作协程
	for i := 0; i < config.Workers; i++ {
		go worker(ctx, db, workCh, i)
	}

	// 启动查询协程
	go func() {
		defer close(workCh)
		queryAndEnqueue(ctx, db, workCh)
		doneCh <- struct{}{}
	}()

	// 等待信号或完成
	select {
	case <-ctx.Done():
		if err := srv.Shutdown(context.Background()); err != nil {
			return fmt.Errorf("HTTP server shutdown failed: %w", err)
		}
	case <-doneCh:
		log.Info().Msg("All DIDs processed")
		if err := srv.Shutdown(context.Background()); err != nil {
			return fmt.Errorf("HTTP server shutdown failed: %w", err)
		}
	case err := <-errCh:
		return err
	}
	
	return nil
}

// 查询 DID 并加入工作队列
func queryAndEnqueue(ctx context.Context, db *gorm.DB, workCh chan<- string) {
	log := zerolog.Ctx(ctx)

	// 先查 filtered_repos 表已有多少条
	var existingCount int64
	db.Model(&FilteredRepo{}).Count(&existingCount)
	if existingCount >= 10000 {
		log.Info().Msgf("filtered_repos already has %d records, skipping enqueue.", existingCount)
		return
	}

	var repos []repo.Repo
	query := db.Model(&repo.Repo{})
	// 随机抽取 10,000 个 DID
	err := query.Order("RANDOM()").Limit(10000-int(existingCount)).Find(&repos).Error
	if err != nil {
		log.Error().Err(err).Msg("Error querying repos")
		return
	}

	log.Info().Msgf("Randomly selected %d DIDs", len(repos))

	for _, r := range repos {
		select {
		case <-ctx.Done():
			return
		case workCh <- r.DID:
		}
	}
}

// 工作协程处理单个 DID
func worker(ctx context.Context, db *gorm.DB, workCh <-chan string, workerID int) {
	log := zerolog.Ctx(ctx).With().Int("worker", workerID).Logger()
	ctx = log.WithContext(ctx)
	
	for {
		select {
		case <-ctx.Done():
			return
		case did, ok := <-workCh:
			if !ok {
				return
			}

			// 检查 filtered_repos 表中记录数，超过10000则直接退出
			var currentCount int64
			db.Model(&FilteredRepo{}).Count(&currentCount)
			if currentCount >= 10000 {
				log.Info().Msgf("filtered_repos reached %d records, worker exiting.", currentCount)
				os.Exit(0)
			}

			// 检查是否已经处理过
			var count int64
			if err := db.Model(&FilteredRepo{}).Where("did = ?", did).Count(&count).Error; err != nil {
				log.Error().Err(err).Str("did", did).Msg("Error checking if DID already processed")
				continue
			}
			if count > 0 {
				log.Debug().Str("did", did).Msg("DID already processed, skipping")
				continue
			}

			// 处理 DID
			if err := processDID(ctx, db, did); err != nil {
				log.Error().Err(err).Str("did", did).Msg("Error processing DID")
			}
		}
	}
}

// 处理单个 DID
func processDID(ctx context.Context, db *gorm.DB, did string) error {
	log := zerolog.Ctx(ctx)
	log.Info().Str("did", did).Msg("Processing DID")
	var pdsValue int64
	if err := db.Model(&repo.Repo{}).Where("did = ?", did).Select("pds").Scan(&pdsValue).Error; err != nil {
		return fmt.Errorf("failed to get PDS value for did %q: %w", did, err)
	}
	filteredRepo := FilteredRepo{
		FlDid:       did,
		FetchedAt: time.Now(),
		PDS:       pdsValue,
	}
	
	err := db.Create(&filteredRepo).Error
	if err != nil {
		return fmt.Errorf("failed to insert filtered repo: %w", err)
	}
	
	// log.Info().Str("did", did).Int("bytes", len(repoData)).Msg("Successfully filtered repo")
	return nil
}

func main() {
	flag.StringVar(&config.LogFile, "log", "", "Path to the log file. If empty, will log to stderr")
	flag.StringVar(&config.LogFormat, "log-format", "text", "Logging format. 'text' or 'json'")
	flag.Int64Var(&config.LogLevel, "log-level", 1, "Log level. -1 - trace, 0 - debug, 1 - info, 5 - panic")
	flag.IntVar(&config.Workers, "workers", 5, "Number of worker goroutines")
	flag.StringVar(&config.DIDFilter, "did-filter", "", "Filter DIDs (SQL LIKE pattern)")
	flag.IntVar(&config.BatchSize, "batch-size", 100, "Batch size for querying repos")

	if err := envconfig.Process("pds-filter", &config); err != nil {
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
