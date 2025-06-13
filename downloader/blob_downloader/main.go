package main

import (
	// "bufio"
    "context"
    "encoding/json"
    "fmt"
	"io"
	"io/fs"
	"log"
    "os"
    "path/filepath"
    "strings"
	"sync"
    "time"
	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/xrpc"
	"golang.org/x/time/rate"
)

// BlobInfo stores basic information about a blob.
type BlobInfo struct {
	Ref      string `json:"ref"`
	MimeType string `json:"mimeType"`
	Size     int    `json:"size"`
}

// ProgressCounter holds statistics about the download process.
type ProgressCounter struct {
	mutex            sync.Mutex
	Users            int64
	ProcessedUsers   int64
	Records          int64
	FoundImages      int64
	DownloadedImages int64
	FailedDownloads  int64
}

// BlobDownloader handles the blob downloading process.
type BlobDownloader struct {
	baseDir     string
	counter     *ProgressCounter
	rateLimiter *rate.Limiter
}

type Job struct {
	DID  string
	Path string
}

var contactInfo = ""

// NewBlobDownloader creates a new BlobDownloader.
func NewBlobDownloader(baseDir string) *BlobDownloader {
	// Rate limiter: 50 requests per minute = 1 every 1.2 seconds.
    limiter := rate.NewLimiter(rate.Every(1200*time.Millisecond), 1)
    
	return &BlobDownloader{
		baseDir:     baseDir,
		counter:     &ProgressCounter{},
        rateLimiter: limiter,
    }
}

func (d *BlobDownloader) ProcessUserRecords(ctx context.Context, job Job) {
	defer func() {
		d.counter.mutex.Lock()
		d.counter.ProcessedUsers++
		d.counter.mutex.Unlock()
	}()

	file, err := os.Open(job.Path)
	if err != nil {
		log.Printf("ERROR: Failed to open records file %s: %v", job.Path, err)
		return
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		log.Printf("ERROR: Failed to read content of %s: %v", job.Path, err)
		return
	}

	var allRecords map[string]interface{}
	if err := json.Unmarshal(content, &allRecords); err != nil {
		// This can happen for empty or malformed files.
		if len(content) > 0 { // Don't log error for empty files.
			log.Printf("ERROR: Failed to parse records file %s: %v", job.Path, err)
		}
		return
	}

	for _, recordDataIfc := range allRecords {
		recordData, ok := recordDataIfc.(map[string]interface{})
		if !ok {
			log.Printf("WARN: Record in %s is not a valid object, skipping.", job.Path)
			continue
		}

		d.counter.mutex.Lock()
		d.counter.Records++
		d.counter.mutex.Unlock()

		images, ok := d.extractImages(recordData)
		if !ok {
			continue
		}

		d.counter.mutex.Lock()
		d.counter.FoundImages += int64(len(images))
		d.counter.mutex.Unlock()

		userBlobDir := filepath.Join(d.baseDir, job.DID, "blob_records")
		if err := os.MkdirAll(userBlobDir, 0755); err != nil {
			log.Printf("ERROR: Failed to create directory %s: %v", userBlobDir, err)
			continue
		}

		for _, img := range images {
			// Per user request, save as {blob_cid}.json
			savePath := filepath.Join(userBlobDir, fmt.Sprintf("%s.%s", img.Ref, strings.Split(img.MimeType, "/")[1]))

			if _, err := os.Stat(savePath); err == nil {
				// File exists, skip download
				// fmt.Printf("Skipping existing blob: %s\n", savePath)
				continue
			}

			if err := d.downloadBlob(ctx, job.DID, img, savePath); err != nil {
				log.Printf("ERROR: Failed to download blob for DID %s (ref: %s): %v", job.DID, img.Ref, err)
				d.counter.mutex.Lock()
				d.counter.FailedDownloads++
				d.counter.mutex.Unlock()
				continue
			}
			d.counter.mutex.Lock()
			d.counter.DownloadedImages++
			d.counter.mutex.Unlock()
		}
	}
}

func (d *BlobDownloader) extractImages(record map[string]interface{}) ([]BlobInfo, bool) {
    var images []BlobInfo

    // 递归查找图片信息
    var findImages func(v interface{})
    findImages = func(v interface{}) {
        switch val := v.(type) {
        case map[string]interface{}:
            // 检查是否是blob类型
            if typeStr, ok := val["$type"].(string); ok && typeStr == "blob" {
                // 处理新格式的blob
                if ref, ok := val["ref"].(map[string]interface{}); ok {
					if refStr, ok := ref["$link"].(string); ok {
                        size, _ := val["size"].(float64)
                        mimeType, _ := val["mimeType"].(string)
                        if strings.HasPrefix(mimeType, "image/") {
                            img := BlobInfo{
                                Ref:      refStr,
                                MimeType: mimeType,
                                Size:     int(size),
                            }
                            images = append(images, img)
                        }
                    }
                }
            } else if blob, ok := val["blob"].(map[string]interface{}); ok {
                // 处理旧格式的blob
                if ref, ok := blob["ref"].(string); ok {
                    mimeType, _ := blob["mimeType"].(string)
                    size, _ := blob["size"].(float64)
                    
                    if strings.HasPrefix(mimeType, "image/") {
                        img := BlobInfo{
                            Ref:      ref,
                            MimeType: mimeType,
                            Size:     int(size),
                        }
                        images = append(images, img)
                    }
                }
            }
            
            // 继续递归搜索
            for _, subVal := range val {
                findImages(subVal)
            }
        case []interface{}:
            for _, item := range val {
                findImages(item)
            }
        }
    }

    findImages(record)
    return images, len(images) > 0
}

func (d *BlobDownloader) downloadBlob(ctx context.Context, did string, blob BlobInfo, savePath string) error {
    // 等待令牌可用（速率限制）
    if err := d.rateLimiter.Wait(ctx); err != nil {
        return fmt.Errorf("rate limiter wait failed: %w", err)
    }
	did = strings.Replace(did, "did_plc_", "did:plc:", 1)
	atid, err := syntax.ParseAtIdentifier(did)
	if err != nil {
		return fmt.Errorf("failed to parse atid: %w", err)
	}
	// first look up the DID and PDS for this repo
	dir := identity.DefaultDirectory()
	ident, err := dir.Lookup(ctx, *atid)
	if err != nil {
		return err
	}
	xrpcc := xrpc.Client{
		Host: ident.PDSEndpoint(),
	}
	if xrpcc.Host == "" {
		return fmt.Errorf("no PDS endpoint for identity")
	}


	blobBytes, err := comatproto.SyncGetBlob(ctx, &xrpcc, blob.Ref, ident.DID.String())
			if err != nil {
				return err
			}
	return os.WriteFile(savePath, blobBytes, 0666)
	
    // Get PDS endpoint
	// u, _, err := resolver.GetPDSEndpointAndPublicKey(ctx, did)
	// if err != nil {
	// 	return fmt.Errorf("failed to resolve PDS endpoint for DID %s: %w", did, err)
	// }
    
	// client := xrpcauth.NewAnonymousClient(ctx)
	// client.Host = u.String()
	// userAgent := fmt.Sprintf("Go-http-client/1.1 indexerbot/0.1 (based on github.com/uabluerail/indexer; %s)", contactInfo)
	// client.UserAgent = &userAgent

    // 下载 blob
	// resp, err := atproto.SyncGetBlob(ctx, client, blob.Ref, did)
    // if err != nil {
	// 	return fmt.Errorf("failed to download blob from %s: %w", client.Host, err)
    // }

    // 保存文件
    // return os.WriteFile(savePath, resp, 0644)
}

// 添加进度打印方法
func (d *BlobDownloader) printProgress() {
	d.counter.mutex.Lock()
	defer d.counter.mutex.Unlock()

	fmt.Printf("\rUsers: %d/%d | Records: %d | Found: %d | Downloaded: %d | Failed: %d",
		d.counter.ProcessedUsers,
		d.counter.Users,
		d.counter.Records,
            d.counter.FoundImages,
            d.counter.DownloadedImages,
            d.counter.FailedDownloads)
}

func main() {
	// baseDir := "/mydata/records_follows"
	baseDir := os.Args[1]
	numWorkers := 20 // Concurrently process 20 users

	downloader := NewBlobDownloader(baseDir)
	jobs := make(chan Job, 100)
	var wg sync.WaitGroup

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				downloader.ProcessUserRecords(ctx, job)
			}
		}()
	}

	// Start progress reporter
	progressCtx, stopProgress := context.WithCancel(context.Background())
	defer stopProgress()
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				downloader.printProgress()
			case <-progressCtx.Done():
				return
			}
		}
	}()

	startTime := time.Now()

	// Find jobs
	var userJobs []Job
	err := filepath.WalkDir(baseDir, func(path string, de fs.DirEntry, err error) error {
    if err != nil {
			return err
		}
		if !de.IsDir() && de.Name() == "records.json" {
			did := filepath.Base(filepath.Dir(path))
			userJobs = append(userJobs, Job{DID: did, Path: path})
		}
		return nil
	})

	if err != nil {
		log.Fatalf("Failed to walk directory: %v", err)
	}

	downloader.counter.Users = int64(len(userJobs))

	// Dispatch jobs
	for _, job := range userJobs {
		jobs <- job
	}
	close(jobs)

	wg.Wait()
	stopProgress() // stop printing progress
	downloader.printProgress() // print final state
	fmt.Println()              // for a new line after progress

	duration := time.Since(startTime)
	fmt.Println("\nProcessing complete!")
	fmt.Printf("Total time: %v\n", duration)
	downloader.counter.mutex.Lock()
	fmt.Printf("Total users: %d\n", downloader.counter.Users)
	fmt.Printf("Processed users: %d\n", downloader.counter.ProcessedUsers)
	fmt.Printf("Total records processed: %d\n", downloader.counter.Records)
	fmt.Printf("Images found: %d\n", downloader.counter.FoundImages)
	fmt.Printf("Images downloaded: %d\n", downloader.counter.DownloadedImages)
	fmt.Printf("Failed downloads: %d\n", downloader.counter.FailedDownloads)
	downloader.counter.mutex.Unlock()
}