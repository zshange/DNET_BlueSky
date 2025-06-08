package main

import (
	"context"
	"encoding/csv"
	"fmt"
	// "io"
	// "net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

type Scheduler struct {
	fsManager   *FileSystemManager
	output      chan<- WorkItem
	csvInputDir string

	mu         sync.Mutex
	queue      map[string]*CSVEntry
	inProgress map[string]*CSVEntry
	csvProcessed map[string]time.Time // 文件名 -> 最后处理时间
	csvProgress  map[string]int       // 文件名 -> 已导入的行数
	csvCompleted map[string]bool      // 文件名 -> 是否完全处理完成
}

func NewScheduler(output chan<- WorkItem, fsManager *FileSystemManager, csvInputDir string) *Scheduler {
	return &Scheduler{
		fsManager:    fsManager,
		output:       output,
		csvInputDir:  csvInputDir,
		queue:        map[string]*CSVEntry{},
		inProgress:   map[string]*CSVEntry{},
		csvProcessed: map[string]time.Time{},
		csvProgress:  map[string]int{},
		csvCompleted: map[string]bool{},
	}
}

func (s *Scheduler) Start(ctx context.Context) error {
	go s.run(ctx)
	return nil
}

func (s *Scheduler) run(ctx context.Context) {
	log := zerolog.Ctx(ctx)
	t := time.NewTicker(30 * time.Second) // 修改为每30秒检查一次
	defer t.Stop()

	// 初始化时立即加载CSV文件
	if err := s.loadCSVFiles(ctx); err != nil {
		log.Error().Err(err).Msgf("Failed to load CSV files: %s", err)
	}

	done := make(chan string)
	
	for {
		s.mu.Lock()
		queueLen := len(s.queue)
		s.mu.Unlock()
		
		if queueLen > 0 {
			// 有任务时的处理分支
			next := WorkItem{signal: make(chan struct{})}
			s.mu.Lock()
			var selectedEntry *CSVEntry
			for _, entry := range s.queue {
				selectedEntry = entry
				break
			}
			s.mu.Unlock()
			
			if selectedEntry != nil {
				next.CSVEntry = selectedEntry
				
				// 阻塞式select - 借鉴record-indexer的稳定模式
				select {
				case <-ctx.Done():
					log.Info().Msg("🔄 Scheduler正在关闭...")
					return
					
				case <-t.C:
					// 定期填充队列
					go func() {
						if err := s.loadCSVFiles(ctx); err != nil {
							log.Error().Err(err).Msgf("Failed to reload CSV files: %s", err)
						}
					}()
					
				case s.output <- next:
					// 任务发送成功
					log.Debug().Str("did", next.CSVEntry.DID).Msg("📤 任务已发送到worker")
					s.mu.Lock()
					delete(s.queue, next.CSVEntry.DID)
					s.inProgress[next.CSVEntry.DID] = next.CSVEntry
					s.mu.Unlock()
					
					// 简化的监控goroutine - 正确处理超时情况
					go func(did string, ch chan struct{}) {
						select {
						case <-ch:
							// 任务正常完成
							log.Debug().Str("did", did).Msg("✅ 用户处理完成")
							done <- did
						case <-time.After(5 * time.Minute):
							// 超时处理 - 不发送done信号，让任务继续在inProgress中
							log.Warn().Str("did", did).Msg("⏰ 用户处理超时，但允许继续处理")
							// 注意：不发送done信号，任务保持在inProgress状态
						case <-ctx.Done():
							// 程序关闭 - 发送done信号进行清理
							done <- did
						}
					}(next.CSVEntry.DID, next.signal)
					s.updateQueueLenMetrics()
					
				case did := <-done:
					// 处理完成信号
					s.mu.Lock()
					wasInProgress := s.inProgress[did] != nil
					// oldinProgressLen := len(s.inProgress)
					delete(s.inProgress, did)
					newInProgressLen := len(s.inProgress)
					s.mu.Unlock()
					
					log.Info().
						Str("did", did).
						Bool("was_in_progress", wasInProgress).
						// Int("old_in_progress", oldinProgressLen).
						Int("remaining_in_progress", newInProgressLen).
						Msg("✅ 用户处理完成，从进行中列表移除")
						
					s.updateQueueLenMetrics()
				}
			}
		} else {
			// 无任务时的处理分支
			select {
			case <-ctx.Done():
				log.Info().Msg("🔄 Scheduler正在关闭...")
				return
				
			case <-t.C:
				// 定期填充队列
				if err := s.loadCSVFiles(ctx); err != nil {
					log.Error().Err(err).Msgf("Failed to reload CSV files: %s", err)
				}
				
			case did := <-done:
				// 处理完成信号
				s.mu.Lock()
				wasInProgress := s.inProgress[did] != nil
				delete(s.inProgress, did)
				newInProgressLen := len(s.inProgress)
				s.mu.Unlock()
				
				log.Info().
					Str("did", did).
					Bool("was_in_progress", wasInProgress).
					Int("remaining_in_progress", newInProgressLen).
					Msg("✅ 用户处理完成，从进行中列表移除")
				s.updateQueueLenMetrics()
			}
		}
	}
}

func (s *Scheduler) loadCSVFiles(ctx context.Context) error {
	const maxQueueLen = 100000
	log := zerolog.Ctx(ctx)

	s.mu.Lock()
	queueLen := len(s.queue)
	s.mu.Unlock()
	
	// 只有当队列为空时才填充
	if queueLen > 0 {
		return nil
	}

	// 扫描CSV目录
	files, err := filepath.Glob(filepath.Join(s.csvInputDir, "*.csv"))
	if err != nil {
		return fmt.Errorf("failed to scan CSV directory: %w", err)
	}

	if len(files) == 0 {
		log.Warn().Msgf("No CSV files found in directory: %s", s.csvInputDir)
		return nil
	}
	
	// ✅ 确保文件按名称排序（可选改进）
	sort.Strings(files)
	log.Debug().Msgf("📁 Processing %d CSV files in sorted order", len(files))

	processed := 0
	for _, filePath := range files {
		if processed >= maxQueueLen {
			break
		}
		
		fileName := filepath.Base(filePath)
		
		// 检查文件是否已经完全处理过
		s.mu.Lock()
		isCompleted := s.csvCompleted[fileName]
		startRow := s.csvProgress[fileName]
		s.mu.Unlock()
		
		// 跳过已完成的文件
		if isCompleted {
			log.Debug().Msgf("Skipping completed file: %s", fileName)
			continue
		}
		
		entries, newStartRow, isFileCompleted, err := s.parseCSVFileWithProgress(filePath, fileName, startRow)
		if err != nil {
			log.Error().Err(err).Msgf("Failed to parse CSV file %s: %s", fileName, err)
			continue
		}

		// 将新条目添加到队列
		s.mu.Lock()
		newEntries := 0
		for _, entry := range entries {
			if processed >= maxQueueLen {
				break
			}
			if s.queue[entry.DID] != nil || s.inProgress[entry.DID] != nil {
				continue
			}
			s.queue[entry.DID] = entry
			processed++
			newEntries++
			usersQueued.Inc()
		}
		// 更新文件导入进度和完成状态
		s.csvProgress[fileName] = newStartRow
		s.csvCompleted[fileName] = isFileCompleted
		s.csvProcessed[fileName] = time.Now()
		s.mu.Unlock()
		
		if newEntries > 0 {
			if isFileCompleted {
				log.Info().Msgf("Loaded %d new entries from file: %s (rows %d-%d) - FILE COMPLETED", newEntries, fileName, startRow, newStartRow-1)
			} else {
				log.Info().Msgf("Loaded %d new entries from file: %s (rows %d-%d)", newEntries, fileName, startRow, newStartRow-1)
			}
		}
		
		// 如果队列已满，跳出循环
		if processed >= maxQueueLen {
			break
		}
	}

	s.updateQueueLenMetrics()
	return nil
}

func (s *Scheduler) parseCSVFile(filePath, fileName string) ([]*CSVEntry, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	if len(records) == 0 {
		return nil, fmt.Errorf("empty CSV file")
	}

	// 检查头部是否正确
	header := records[0]
	didIndex := -1
	repoIndex := -1
	
	for i, col := range header {
		switch strings.ToLower(strings.TrimSpace(col)) {
		case "did":
			didIndex = i
		case "repos", "record_count", "repo_count":
			repoIndex = i
		}
	}

	if didIndex == -1 {
		return nil, fmt.Errorf("DID column not found in CSV header")
	}

	var entries []*CSVEntry
	seenDIDs := make(map[string]bool)

	for _, record := range records[1:] { // 跳过头部
		if len(record) <= didIndex {
			continue
		}

		did := strings.TrimSpace(record[didIndex])
		if did == "" {
			continue
		}

		// 避免同一文件中的重复DID
		if seenDIDs[did] {
			continue
		}
		seenDIDs[did] = true

		repoCount := int64(0)
		if repoIndex != -1 && len(record) > repoIndex {
			if count, err := strconv.ParseInt(strings.TrimSpace(record[repoIndex]), 10, 64); err == nil {
				repoCount = count
			}
		}

		entry := &CSVEntry{
			FileName:  fileName,
			DID:       did,
			RepoCount: repoCount,
			Processed: false,
		}

		entries = append(entries, entry)
	}

	return entries, nil
}

func (s *Scheduler) parseCSVFileWithProgress(filePath, fileName string, startRow int) ([]*CSVEntry, int, bool, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, 0, false, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, 0, false, err
	}

	if len(records) == 0 {
		return nil, 0, false, fmt.Errorf("empty CSV file")
	}

	// 检查头部是否正确
	header := records[0]
	didIndex := -1
	repoIndex := -1
	
	for i, col := range header {
		switch strings.ToLower(strings.TrimSpace(col)) {
		case "did":
			didIndex = i
		case "repos", "record_count", "repo_count":
			repoIndex = i
		}
	}

	if didIndex == -1 {
		return nil, 0, false, fmt.Errorf("DID column not found in CSV header")
	}

	var entries []*CSVEntry
	seenDIDs := make(map[string]bool)

	// 从startRow开始处理记录
	dataRecords := records[1:] // 跳过头部
	totalRows := len(dataRecords)
	
	// 如果已经处理完所有行，返回空结果和完成标记
	if startRow >= totalRows {
		return nil, startRow, true, nil
	}

	currentRow := startRow
	for i := startRow; i < totalRows; i++ {
		record := dataRecords[i]
		currentRow = i + 1 // +1因为跳过了头部

		if len(record) <= didIndex {
			continue
		}

		did := strings.TrimSpace(record[didIndex])
		if did == "" {
			continue
		}

		// 避免同一文件中的重复DID
		if seenDIDs[did] {
			continue
		}
		seenDIDs[did] = true

		repoCount := int64(0)
		if repoIndex != -1 && len(record) > repoIndex {
			if count, err := strconv.ParseInt(strings.TrimSpace(record[repoIndex]), 10, 64); err == nil {
				repoCount = count
			}
		}

		entry := &CSVEntry{
			FileName:  fileName,
			DID:       did,
			RepoCount: repoCount,
			Processed: false,
		}

		entries = append(entries, entry)
		
		// 限制每次加载的条目数量，避免内存问题
		if len(entries) >= 10000 {
			break
		}
	}

	// 判断文件是否完全处理完成
	isCompleted := currentRow >= totalRows
	
	return entries, currentRow, isCompleted, nil
}

func (s *Scheduler) updateQueueLenMetrics() {
	queueLength.WithLabelValues("queued").Set(float64(len(s.queue)))
	queueLength.WithLabelValues("inProgress").Set(float64(len(s.inProgress)))
}



