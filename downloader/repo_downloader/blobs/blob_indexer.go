package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/uabluerail/bsky-tools/xrpcauth"
	// "github.com/uabluerail/indexer/models"
	"DNET_BlueSky/downloader/repo_downloader/blobs/repo"
	"DNET_BlueSky/downloader/repo_downloader/blobs/resolver"
)

const (
	blobStoragePath = "/mydata/blob"
	maxBlobWorkers  = 5
	blobBatchSize   = 500
)

// BlobRecord represents a blob in the database
type BlobRecord struct {
	UpdatedAt time.Time `gorm:"column:updated_at"`
	DID       string    `gorm:"column:did;not null"`
	BlobCID   string    `gorm:"column:blob_cid;primaryKey"`
	Size      int64     `gorm:"column:size"`
	MimeType  string    `gorm:"column:mime_type"`
	CreatedAt time.Time `gorm:"column:created_at"`
}


// TableName sets the table name for BlobRecord
func (BlobRecord) TableName() string {
	return "media_blobs"
}

// Metrics for blob operations
var (
	blobsQueued = promauto.NewCounter(prometheus.CounterOpts{
		Name: "indexer_blobs_queued_count",
		Help: "Number of blobs added to the queue",
	})

	blobQueueLength = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "indexer_blob_queue_length",
		Help: "Current length of blob indexing queue",
	})

	blobsFetched = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "indexer_blobs_fetched_count",
		Help: "Number of blobs fetched",
	}, []string{"remote", "success"})

	blobsIndexed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "indexer_blobs_indexed_count",
		Help: "Number of blobs indexed in DB",
	})

	blobSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "indexer_blob_size_bytes",
		Help: "Size of the fetched blob in bytes",
		Buckets: prometheus.ExponentialBucketsRange(1024, 10*1024*1024, 20),
	})

	blobDownloadTime = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "indexer_blob_download_duration",
		Help: "Time taken to download a blob",
		Buckets: prometheus.ExponentialBucketsRange(0.01, 30, 20),
	})
)

// BlobWorkItem represents a blob to be processed
type BlobWorkItem struct {
	Repo     *repo.Repo
	CID      string
	signal   chan struct{}
}

// BlobWorkerPool manages workers for downloading blobs
type BlobWorkerPool struct {
	db                  *gorm.DB
	input               chan BlobWorkItem
	limiter             *Limiter
	contactInfo         string
	workerSignals       []chan struct{}
	waitGroup           sync.WaitGroup
}

// NewBlobWorkerPool creates a new worker pool for blob processing
func NewBlobWorkerPool(db *gorm.DB, size int, limiter *Limiter, contactInfo string) *BlobWorkerPool {
	if size <= 0 {
		size = maxBlobWorkers
	}
	
	p := &BlobWorkerPool{
		db:                  db,
		input:               make(chan BlobWorkItem, 1000),
		limiter:             limiter,
		contactInfo:         contactInfo,
	}
	
	p.workerSignals = make([]chan struct{}, size)
	for i := range p.workerSignals {
		p.workerSignals[i] = make(chan struct{})
	}
	
	// Ensure blob storage directory exists
	if err := os.MkdirAll(blobStoragePath, 0755); err != nil {
		panic(fmt.Sprintf("Failed to create blob storage directory: %v", err))
	}
	
	return p
}

// Start starts the blob worker pool
func (p *BlobWorkerPool) Start(ctx context.Context) error {
	for i, ch := range p.workerSignals {
		go p.worker(ctx, i, ch)
	}
	return nil
}

// Stop stops the blob worker pool
func (p *BlobWorkerPool) Stop() {
	for _, ch := range p.workerSignals {
		close(ch)
	}
	p.waitGroup.Wait()
}

// QueueBlob adds a blob to the processing queue
func (p *BlobWorkerPool) QueueBlob(repo *repo.Repo, cid string) {
	signal := make(chan struct{})
	item := BlobWorkItem{
		Repo:     repo,
		CID:      cid,
		signal:   signal,
	}
	
	select {
	case p.input <- item:
		blobsQueued.Inc()
		blobQueueLength.Inc()
	default:
		// Queue is full, drop the item or handle differently
		close(signal)
	}
}

// worker processes blob downloads
func (p *BlobWorkerPool) worker(ctx context.Context, id int, signal chan struct{}) {
	p.waitGroup.Add(1)
	defer p.waitGroup.Done()
	
	log := zerolog.Ctx(ctx).With().Int("worker_id", id).Logger()
	log.Debug().Msg("Blob worker started")
	
	for {
		select {
		case <-ctx.Done():
			log.Debug().Msg("Blob worker stopping due to context cancellation")
			return
		case <-signal:
			log.Debug().Msg("Blob worker stopping due to signal")
			return
		case work, ok := <-p.input:
			if !ok {
				log.Debug().Msg("Blob worker stopping due to closed channel")
				return
			}
			
			blobQueueLength.Dec()
			if err := p.processBlob(ctx, work); err != nil {
				log.Error().Err(err).Str("did", work.Repo.DID).Str("cid", work.CID).Msg("Failed to process blob")
			}
			close(work.signal)
		}
	}
}

// processBlob downloads and indexes a single blob
func (p *BlobWorkerPool) processBlob(ctx context.Context, work BlobWorkItem) error {
	log := zerolog.Ctx(ctx).With().Str("did", work.Repo.DID).Str("cid", work.CID).Logger()
	ctx = log.WithContext(ctx)
	
	// Check if blob already exists in DB
	var count int64
	if err := p.db.Model(&BlobRecord{}).Where("blob_cid = ?", work.CID).Count(&count).Error; err != nil {
		return fmt.Errorf("error checking blob existence: %w", err)
	}
	
	if count > 0 {
		log.Debug().Msg("Blob already exists in database, skipping")
		return nil
	}
	
	// Get PDS endpoint
	u, _, err := resolver.GetPDSEndpointAndPublicKey(ctx, work.Repo.DID)
	if err != nil {
		return fmt.Errorf("failed to resolve PDS endpoint: %w", err)
	}
	
	// Rate limit if needed
	if p.limiter != nil {
		if err := p.limiter.Wait(ctx, u.String()); err != nil {
			return fmt.Errorf("failed to wait on rate limiter: %w", err)
		}
	}
	
	// Create API client
	client := xrpcauth.NewAnonymousClient(ctx)
	client.Host = u.String()
	userAgent := fmt.Sprintf("Go-http-client/1.1 indexerbot/0.1 (based on github.com/uabluerail/indexer; %s)", p.contactInfo)
	client.UserAgent = &userAgent
	
	// TODO 修改下载Blob的方法
	startTime := time.Now()
	blobBytes, err := comatproto.SyncGetBlob(ctx, client, work.CID, work.Repo.DID)
	downloadDuration := time.Since(startTime)
	
	if err != nil {
		blobsFetched.WithLabelValues(u.String(), "false").Inc()
		return fmt.Errorf("failed to fetch blob: %w", err)
	}
	
	blobsFetched.WithLabelValues(u.String(), "true").Inc()
	blobDownloadTime.Observe(downloadDuration.Seconds())
	blobSize.Observe(float64(len(blobBytes)))
	
	// TODO 设计CID的存储路径
	blobDir := filepath.Join(blobStoragePath, work.CID[:2], work.CID[2:4])
	if err := os.MkdirAll(blobDir, 0755); err != nil {
		return fmt.Errorf("failed to create blob directory: %w", err)
	}
	
	// Save blob to disk
	blobPath := filepath.Join(blobDir, work.CID)
	if err := ioutil.WriteFile(blobPath, blobBytes, 0644); err != nil {
		return fmt.Errorf("failed to write blob to disk: %w", err)
	}
	
	// TODO 需要根据blob的类型，确定mime_type
	mimeType := "application/octet-stream"
	if len(blobBytes) > 512 {
		mimeType = http.DetectContentType(blobBytes[:512])
	} else {
		mimeType = http.DetectContentType(blobBytes)
	}
	
	// Save to database

	blobRecord := BlobRecord{
		DID:      work.Repo.DID,
		BlobCID:       work.CID,
		Size:      int64(len(blobBytes)),
		MimeType:  mimeType,
		CreatedAt: time.Now(),
	}
	// TODO 此处有报错，DID存储失败
	// fmt.Println(blobRecord.DID)
	result := p.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "blob_cid"}},
		DoNothing: true,
	}).Create(&blobRecord)
	
	if result.Error != nil {
		return fmt.Errorf("failed to save blob record to database: %w", result.Error)
	}
	
	if result.RowsAffected > 0 {
		blobsIndexed.Inc()
	}
	
	log.Debug().
		Int64("size", int64(len(blobBytes))).
		Str("path", blobPath).
		Str("mime", mimeType).
		Msg("Blob downloaded and indexed successfully")
	
	return nil
}

// FetchRepoBlobs fetches all blobs for a specific repo
func FetchRepoBlobs(ctx context.Context, workerPool *BlobWorkerPool, repo *repo.Repo) error {
	log := zerolog.Ctx(ctx).With().Str("did", repo.DID).Logger()
	ctx = log.WithContext(ctx)
	
	// Get PDS endpoint
	u, _, err := resolver.GetPDSEndpointAndPublicKey(ctx, repo.DID)

	if err != nil {
		return fmt.Errorf("failed to resolve PDS endpoint: %w", err)
	}
	
	// Create API client
	client := xrpcauth.NewAnonymousClient(ctx)
	client.Host = u.String()
	userAgent := fmt.Sprintf("Go-http-client/1.1 indexerbot/0.1 (based on github.com/uabluerail/indexer; %s)", workerPool.contactInfo)
	client.UserAgent = &userAgent
	
	// Parse DID for use with API
	did, err := syntax.ParseDID(repo.DID)
	if err != nil {
		return fmt.Errorf("invalid DID format: %w", err)
	}
	
	// Fetch blobs with pagination
	var cursor string
	for {
		resp, err := comatproto.SyncListBlobs(ctx, client, cursor, did.String(), blobBatchSize, "")
		if err != nil {
			return fmt.Errorf("failed to list blobs: %w", err)
		}
		
		log.Info().Int("count", len(resp.Cids)).Msg("Received blob list batch")
		
		// Queue each blob for processing
		for _, cidStr := range resp.Cids {
			fmt.Println(cidStr)
			workerPool.QueueBlob(repo, cidStr)
		}
		
		// Handle pagination
		if resp.Cursor != nil && *resp.Cursor != "" {
			cursor = *resp.Cursor
		} else {
			break
		}
	}
	
	log.Info().Msg("Finished queueing all blobs for repository")
	return nil
}

// InitBlobIndexer creates tables and prepares for blob indexing
func InitBlobIndexer(db *gorm.DB) error {
	// Create media_blobs table if it doesn't exist
	if err := db.AutoMigrate(&BlobRecord{}); err != nil {
		return fmt.Errorf("failed to create media_blobs table: %w", err)
	}
	return nil
} 