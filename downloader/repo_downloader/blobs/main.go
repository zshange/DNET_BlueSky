package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	_ "github.com/joho/godotenv/autoload"
	"github.com/kelseyhightower/envconfig"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"net/http"

	"DNET_BlueSky/downloader/repo_downloader/blobs/repo"
	"github.com/uabluerail/indexer/util/gormzerolog"
)

// Config holds application configuration
type Config struct {
	LogFile     string
	LogFormat   string `default:"text"`
	LogLevel    int64  `default:"1"`
	MetricsPort string `split_words:"true"`
	DBUrl       string   `default:"postgresql://postgres:DNET_ZQJ@postgres:5432/bluesky?sslmode=disable"`
	Workers     int    `default:"3"`
	ContactInfo string `split_words:"true"`
}

var config Config

func main() {
	flag.StringVar(&config.LogFile, "log", "", "Path to the log file. If empty, will log to stderr")
	flag.StringVar(&config.LogFormat, "log-format", "text", "Logging format. 'text' or 'json'")
	flag.Int64Var(&config.LogLevel, "log-level", 1, "Log level. -1 - trace, 0 - debug, 1 - info, 5 - panic")
	flag.IntVar(&config.Workers, "workers", 3, "Number of blob download workers")

	if err := envconfig.Process("indexer", &config); err != nil {
		log.Fatalf("envconfig.Process: %s", err)
	}

	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := runMain(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runMain(ctx context.Context) error {
	ctx = setupLogging(ctx)
	log := zerolog.Ctx(ctx)
	log.Debug().Msgf("Starting blob indexer...")

	if config.ContactInfo == "" {
		config.ContactInfo = "<contact info unspecified>"
	}

	// Connect to database
	dbCfg, err := pgxpool.ParseConfig(config.DBUrl)
	if err != nil {
		return fmt.Errorf("parsing DB URL: %w", err)
	}
	dbCfg.MaxConns = 100
	dbCfg.MinConns = 5
	dbCfg.MaxConnLifetime = 6 * time.Hour

	conn, err := pgxpool.NewWithConfig(ctx, dbCfg)
	if err != nil {
		return fmt.Errorf("connecting to postgres: %w", err)
	}

	sqldb := stdlib.OpenDBFromPool(conn)
	db, err := gorm.Open(postgres.New(postgres.Config{
		Conn: sqldb,
	}), &gorm.Config{
		Logger: gormzerolog.New(&logger.Config{
			SlowThreshold:             3 * time.Second,
			IgnoreRecordNotFoundError: true,
		}, nil),
	})
	if err != nil {
		return fmt.Errorf("connecting to the database: %w", err)
	}
	log.Debug().Msgf("DB connection established")

	// Initialize database tables
	if err := InitBlobIndexer(db); err != nil {
		return fmt.Errorf("failed to initialize blob indexer: %w", err)
	}

	// Create rate limiter
	limiter, err := NewLimiter(db)
	if err != nil {
		return fmt.Errorf("failed to create limiter: %w", err)
	}

	// Create and start blob worker pool
	blobPool := NewBlobWorkerPool(db, config.Workers, limiter, config.ContactInfo)
	if err := blobPool.Start(ctx); err != nil {
		return fmt.Errorf("failed to start blob worker pool: %w", err)
	}
	defer blobPool.Stop()

	// Setup HTTP server for metrics
	log.Info().Msgf("Starting HTTP listener on %q...", config.MetricsPort)
	http.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Addr: fmt.Sprintf(":%s", config.MetricsPort)}
	
	// Start HTTP server in a goroutine
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe()
	}()

	// Process repos in a loop
	go processRepos(ctx, db, blobPool)

	// Wait for shutdown signal
	select {
	case <-ctx.Done():
		log.Info().Msg("Received shutdown signal")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("HTTP server shutdown failed: %w", err)
		}
		return nil
	case err := <-errCh:
		return fmt.Errorf("HTTP server error: %w", err)
	}
}

// processRepos continuously processes repositories for blob indexing
func processRepos(ctx context.Context, db *gorm.DB, blobPool *BlobWorkerPool) {
	log := zerolog.Ctx(ctx)
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Find repositories to process
			var repos []repo.Repo
			
			// Get repos that have been successfully indexed (records) but not yet processed for blobs
			result := db.Where("last_indexed_rev != '' AND last_blob_index_attempt < last_index_attempt").
				Or("last_blob_index_attempt IS NULL AND last_indexed_rev != ''").
				Limit(100).
				Find(&repos)
			
			if result.Error != nil {
				log.Error().Err(result.Error).Msg("Failed to query repos for blob indexing")
				continue
			}

			log.Info().Int("count", len(repos)).Msg("Processing repos for blob indexing")
			
			for _, r := range repos {
				repoCtx := log.With().Str("did", r.DID).Logger().WithContext(ctx)
				
				// Create a copy to avoid issues with loop variable capture
				repo1 := r
				
				// Process each repo for blobs
				if err := FetchRepoBlobs(repoCtx, blobPool, &repo1); err != nil {
					log.Error().Err(err).Str("did", repo1.DID).Msg("Failed to fetch blobs for repo")
					
					// Update last attempt and error
					db.Model(&repo.Repo{}).
						Where("id = ?", repo1.ID).
						Updates(map[string]interface{}{
							"last_blob_index_attempt": time.Now(),
							"last_blob_error": err.Error(),
						})
				} else {
					// Update last attempt time
					db.Model(&repo.Repo{}).
						Where("id = ?", repo1.ID).
						Updates(map[string]interface{}{
							"last_blob_index_attempt": time.Now(),
							"last_blob_error": nil,
						})
				}
			}
		}
	}
}

func setupLogging(ctx context.Context) context.Context {
	// Simplified logging setup
	logLevel := zerolog.Level(config.LogLevel)
	zerolog.SetGlobalLevel(logLevel)
	
	var output zerolog.ConsoleWriter
	
	if config.LogFormat == "json" {
		logger := zerolog.New(os.Stderr).Level(logLevel).With().Timestamp().Logger()
		return logger.WithContext(ctx)
	}
	
	output = zerolog.ConsoleWriter{
		Out:        os.Stderr,
		TimeFormat: time.RFC3339,
	}
	
	logger := zerolog.New(output).Level(logLevel).With().Timestamp().Logger()
	return logger.WithContext(ctx)
} 