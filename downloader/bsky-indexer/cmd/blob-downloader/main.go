package main

import (
    "context"
    "encoding/json"
    "fmt"
    "os"
    "path/filepath"
    "strings"
    "time"
    "golang.org/x/time/rate"
	"github.com/gocql/gocql"      
    // "github.com/bluesky-social/indigo/atproto/syntax"
    "github.com/bluesky-social/indigo/api/atproto"
    // "github.com/bluesky-social/indigo/xrpc"
    // "github.com/scylladb/gocqlx/qb"
    "github.com/scylladb/gocqlx/v3"
    "github.com/uabluerail/indexer/util/resolver"
    "github.com/uabluerail/bsky-tools/xrpcauth"
)

// BlobInfo 存储 blob 的基本信息
type BlobInfo struct {
    Ref       string `json:"ref"`
    MimeType  string `json:"mimeType"`
    Size      int    `json:"size"`
}

// 添加计数器结构体
type ProgressCounter struct {
    TotalRecords     int
    ProcessedRecords int
    FoundImages      int
    DownloadedImages int
    FailedDownloads  int
}

type RecordDownloader struct {
    session   *gocqlx.Session  // 确保这里是指针类型
    batchSize int
    baseDir   string
    counter   *ProgressCounter
    rateLimiter *rate.Limiter  // 添加速率限制器
}

var skipCollections = map[string]struct{}{
    // "app.bsky.actor.profile":        {},
    "app.bsky.graph.follow":         {},
    // "app.bsky.graph.block":          {},
    // "app.bsky.feed.repost":          {},
    "app.bsky.feed.like":            {},
    // "app.bsky.feed.post":            {},
    "chat.bsky.actor.declaration":   {},
    "app.bsky.graph.starterpack":    {},
    "app.bsky.graph.listblock":      {},
    "app.bsky.graph.list":           {},
    "app.bsky.feed.threadgate":      {},
    "app.bsky.graph.listitem":       {},
    "app.bsky.feed.postgate":        {},
    // "blue.2048.game":                {},
    // "social.pinksky.app.preference": {},
    "place.stream.key":              {},
    // "app.bsky.feed.generator":       {},
    // "blue.flashes.actor.portfolio":  {},
    // "blue.flashes.actor.profile":    {},
}

func NewRecordDownloader(session *gocqlx.Session, baseDir string) *RecordDownloader {
    // 创建速率限制器：每分钟50次 = 每1.2秒一次
    limiter := rate.NewLimiter(rate.Every(1200*time.Millisecond), 1)
    
    return &RecordDownloader{
        session:   session,
        batchSize: 1000,  // 每批处理的记录数
        baseDir:   baseDir,
        counter:   &ProgressCounter{},
        rateLimiter: limiter,
    }
}

var contactInfo = "shange0403@gmail.com"

// 记录类型：
// app.bsky.actor.profile
// app.bsky.graph.follow
// app.bsky.graph.block
// app.bsky.feed.repost
// app.bsky.graph.follow
// app.bsky.actor.profile
// app.bsky.feed.repost
// app.bsky.feed.like
// app.bsky.feed.post
// app.bsky.graph.block
// chat.bsky.actor.declaration
// app.bsky.graph.starterpack
// app.bsky.graph.listblock
// app.bsky.graph.list
// app.bsky.feed.threadgate
// app.bsky.graph.listitem
// app.bsky.feed.postgate
// blue.2048.game
// social.pinksky.app.preference
// place.stream.key
//  app.bsky.feed.generator
//  blue.flashes.actor.portfolio
//  blue.flashes.actor.profile
func (d *RecordDownloader) ProcessAllRecords(ctx context.Context) error {
    fmt.Println("开始处理记录...")
    startTime := time.Now()
    type row struct {
        Repo       string
        Collection string
        Rkey       string
        Record     string
    }
    // type_collect := "app.bsky.actor.profile"
    // 先获取所有不同的 (repo, collection) 组合
    var lastRepo, lastCollection string
    for {
        // 构建分区查询
        var partitionQuery string
        var partitionValues []interface{}
        if lastRepo == "" {
            partitionQuery = "SELECT DISTINCT repo, collection FROM bluesky.records WHERE token(repo, collection) > token('', '') LIMIT ?"
            partitionValues = []interface{}{d.batchSize}
        } else {
            partitionQuery = "SELECT DISTINCT repo, collection FROM bluesky.records WHERE token(repo, collection) > token(?, ?) LIMIT ?"
            partitionValues = []interface{}{lastRepo, lastCollection, d.batchSize}
        }

        // 执行分区查询
        partitionStmt := d.session.Session.Query(partitionQuery, partitionValues...)
        partitionStmt.WithContext(ctx)
        partitionIter := partitionStmt.Iter()

        var currentRepo, currentCollection string
        partitionsInBatch := 0

        // 遍历每个分区
        for partitionIter.Scan(&currentRepo, &currentCollection) {
            partitionsInBatch++
            
            // 更新最后一个分区的位置
            lastRepo = currentRepo
            lastCollection = currentCollection

           
            
            if _, exists := skipCollections[currentCollection]; exists {
                continue
            }

            // 在当前分区内获取所有记录
            recordQuery := "SELECT repo, collection, rkey, record FROM bluesky.records WHERE repo = ? AND collection = ?"
            recordStmt := d.session.Session.Query(recordQuery, currentRepo, currentCollection)
            recordStmt.WithContext(ctx)
            recordIter := recordStmt.Iter()

            var r row
            lastProgressUpdate := time.Now()

            // 处理当前分区内的所有记录
            for recordIter.Scan(&r.Repo, &r.Collection, &r.Rkey, &r.Record) {
                d.counter.ProcessedRecords++
                // fmt.Println(r.Record)
                // 每秒更新一次进度
                if time.Since(lastProgressUpdate) > time.Second {
                    d.printProgress()
                    lastProgressUpdate = time.Now()
                }

                // 解析记录内容
                var recordData map[string]interface{}
                if err := json.Unmarshal([]byte(r.Record), &recordData); err != nil {
                    fmt.Printf("错误: 解析记录 %s 失败: %v\n", r.Rkey, err)
                    continue
                }
               
                // 提取图片信息
                images, ok := d.extractImages(recordData)
                if !ok {
                    continue
                }
                d.counter.FoundImages += len(images)

                // 为每个用户创建目录
                userDir := filepath.Join(d.baseDir, r.Repo, r.Collection)
                if err := os.MkdirAll(userDir, 0755); err != nil {
                    fmt.Printf("错误: 创建目录 %s 失败: %v\n", r.Repo, err)
                    continue
                }
                
                // 下载每个图片
                for i, img := range images {
                    savePath := filepath.Join(userDir, fmt.Sprintf("%s_%d", r.Rkey, i))
                    fmt.Printf("正在下载: %s -> %s\n", img.Ref, savePath)
                    
                    if err := d.downloadBlob(ctx, r.Repo, img, savePath); err != nil {
                        fmt.Printf("错误: 下载 blob %s 失败: %v\n", r.Rkey, err)
                        d.counter.FailedDownloads++
                        continue
                    }
                    d.counter.DownloadedImages++
                }
            }

            if err := recordIter.Close(); err != nil {
                return fmt.Errorf("error closing record iterator: %w", err)
            }
        }

        if err := partitionIter.Close(); err != nil {
            return fmt.Errorf("error closing partition iterator: %w", err)
        }

        // 如果这批次分区数小于批次大小，说明已经处理完所有分区
        if partitionsInBatch < d.batchSize {
            break
        }

        // 打印批次处理信息
        fmt.Printf("\n完成批次处理，当前已处理记录数: %d\n", d.counter.ProcessedRecords)
    }

    // 打印最终统计信息
    duration := time.Since(startTime)
    fmt.Printf("\n完成处理!\n")
    fmt.Printf("总用时: %v\n", duration)
    fmt.Printf("处理记录数: %d\n", d.counter.ProcessedRecords)
    fmt.Printf("发现图片数: %d\n", d.counter.FoundImages)
    fmt.Printf("成功下载数: %d\n", d.counter.DownloadedImages)
    fmt.Printf("下载失败数: %d\n", d.counter.FailedDownloads)
    fmt.Printf("平均处理速度: %.2f 记录/秒\n", float64(d.counter.ProcessedRecords)/duration.Seconds())

    return nil
}

func (d *RecordDownloader) extractImages(record map[string]interface{}) ([]BlobInfo, bool) {
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
                    if refStr, ok := ref["/"].(string); ok {
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

func (d *RecordDownloader) downloadBlob(ctx context.Context, did string, blob BlobInfo, savePath string) error {
    // 等待令牌可用（速率限制）
    if err := d.rateLimiter.Wait(ctx); err != nil {
        return fmt.Errorf("rate limiter wait failed: %w", err)
    }

    // fmt.Printf("did: %s\n", did)
    // os.Exit(0)

    // parsedDID, err := syntax.ParseDID(did)
    // if err != nil {
    //     return fmt.Errorf("invalid DID format: %w", err)
    // }


    // Get PDS endpoint
	u, _, err := resolver.GetPDSEndpointAndPublicKey(ctx, did)
	if err != nil {
		return fmt.Errorf("failed to resolve PDS endpoint: %w", err)
	}
    
	client := xrpcauth.NewAnonymousClient(ctx)
	client.Host = u.String()
	userAgent := fmt.Sprintf("Go-http-client/1.1 indexerbot/0.1 (based on github.com/uabluerail/indexer; %s)", contactInfo)
	client.UserAgent = &userAgent

    // 下载 blob
    resp, err := atproto.SyncGetBlob(ctx, client,  blob.Ref,did)
    if err != nil {
        return fmt.Errorf("failed to download blob: %w", err)
    }
    
    // 根据 MIME 类型添加正确的文件扩展名
    ext := ".bin"
    switch blob.MimeType {
    case "image/jpeg":
        ext = ".jpg"
    case "image/png":
        ext = ".png"
    case "image/gif":
        ext = ".gif"
    }

    savePath = savePath + ext

    // 保存文件
    return os.WriteFile(savePath, resp, 0644)
}

// 添加进度打印方法
func (d *RecordDownloader) printProgress() {
    if d.counter.TotalRecords > 0 {
        progress := float64(d.counter.ProcessedRecords) / float64(d.counter.TotalRecords) * 100
        fmt.Printf("\r进度: %.2f%% (%d/%d) | 找到图片: %d | 已下载: %d | 失败: %d",
            progress,
            d.counter.ProcessedRecords,
            d.counter.TotalRecords,
            d.counter.FoundImages,
            d.counter.DownloadedImages,
            d.counter.FailedDownloads)
    }
}

func main() {
    
    cluster := gocql.NewCluster("172.18.0.3")
    cluster.Keyspace = "bluesky"
    cluster.Consistency = gocql.Quorum
    cluster.Timeout = time.Second * 30
	cluster.Port = 9042

    
    session, err := gocqlx.WrapSession(cluster.CreateSession())
    if err != nil {
        panic(fmt.Sprintf("Failed to wrap session: %v", err))
    }
    defer session.Close()

    
    downloader := NewRecordDownloader(&session, "/mydata/mmrecord")
    
    
    ctx, cancel := context.WithTimeout(context.Background(), 24*time.Hour)
    defer cancel()

    
    if err := downloader.ProcessAllRecords(ctx); err != nil {
        panic(err)
    }
}