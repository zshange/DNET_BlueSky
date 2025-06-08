package main

import (
	"context"
	"encoding/json"
	// "fmt"
	"os"
	"path/filepath"
	// "strings"
	"sync"
	"time"
)

// CSVEntry CSV文件条目（内存中跟踪）
type CSVEntry struct {
	FileName      string    `json:"file_name"`
	DID           string    `json:"did"`
	RepoCount     int64     `json:"repo_count"`
	Processed     bool      `json:"processed"`
	ProcessedAt   time.Time `json:"processed_at"`
	FailedAttempts int      `json:"failed_attempts"`
	LastError     string    `json:"last_error,omitempty"`
	Status        string    `json:"status"`  // pending, processing, completed, failed, abandoned
}

// CSVFileStatus CSV文件处理状态
type CSVFileStatus struct {
	FileName        string    `json:"file_name"`
	FilePath        string    `json:"file_path"`
	Status          string    `json:"status"`  // pending, processing, completed, failed
	TotalRecords    int       `json:"total_records"`
	ProcessedRecords int      `json:"processed_records"`
	SuccessRecords  int       `json:"success_records"`
	FailedRecords   int       `json:"failed_records"`
	AbandonedRecords int      `json:"abandoned_records"`
	CurrentIndex    int       `json:"current_index"`
	StartTime       time.Time `json:"start_time"`
	LastUpdateTime  time.Time `json:"last_update_time"`
	CompletedTime   time.Time `json:"completed_time,omitempty"`
	LastError       string    `json:"last_error,omitempty"`
}

// UserProfile 用户档案（文件系统存储）
type UserProfile struct {
	DID            string    `json:"did"`
	Handle         string    `json:"handle"`
	DisplayName    string    `json:"display_name,omitempty"`
	Description    string    `json:"description,omitempty"`
	Avatar         string    `json:"avatar,omitempty"`
	Banner         string    `json:"banner,omitempty"`
	FollowersCount int64     `json:"followers_count"`
	FollowsCount   int64     `json:"follows_count"`
	PostsCount     int64     `json:"posts_count"`
	RecordsCount   int64     `json:"records_count"`
	Status         string    `json:"status"` // pending, processing, completed, failed, deleted
	LastError      string    `json:"last_error,omitempty"`
	FailedAttempts int       `json:"failed_attempts"`
	LastProcessed  time.Time `json:"last_processed"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// DownloadStats 下载统计（文件存储）
type DownloadStats struct {
	TotalUsers        int64     `json:"total_users"`
	ProcessedUsers    int64     `json:"processed_users"`
	CompletedUsers    int64     `json:"completed_users"`
	FailedUsers       int64     `json:"failed_users"`
	DeletedUsers      int64     `json:"deleted_users"`
	TotalRecords      int64     `json:"total_records"`
	UsersWithRecords  int64     `json:"users_with_records"`
	LastProcessedFile string    `json:"last_processed_file"`
	LastProcessedDID  string    `json:"last_processed_did"`
	StartTime         time.Time `json:"start_time"`
	LastUpdateTime    time.Time `json:"last_update_time"`
}

// FileSystemManager 文件系统状态管理器
type FileSystemManager struct {
	baseDir    string
	statsFile  string
	statusDir  string
	mu         sync.RWMutex
	stats      *DownloadStats
	processed  map[string]bool // DID -> processed
	
	// 异步统计更新
	statsUpdates chan map[string]interface{}
	statsCtx     context.Context
	statsCancel  context.CancelFunc
}

func NewFileSystemManager(baseDir string) *FileSystemManager {
	statusDir := filepath.Join(baseDir, ".status")
	os.MkdirAll(statusDir, 0755)
	
	ctx, cancel := context.WithCancel(context.Background())
	
	fm := &FileSystemManager{
		baseDir:      baseDir,
		statsFile:    filepath.Join(statusDir, "stats.json"),
		statusDir:    statusDir,
		processed:    make(map[string]bool),
		statsUpdates: make(chan map[string]interface{}, 1000), // 大缓冲区
		statsCtx:     ctx,
		statsCancel:  cancel,
	}
	
	fm.loadStats()
	fm.loadProcessedList()
	
	// 启动异步统计更新goroutine
	go fm.statsUpdateWorker()
	
	return fm
}

func (fm *FileSystemManager) loadStats() {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	
	data, err := os.ReadFile(fm.statsFile)
	if err != nil {
		// 文件不存在，创建新的统计
		fm.stats = &DownloadStats{
			StartTime:      time.Now(),
			LastUpdateTime: time.Now(),
		}
		return
	}
	
	var stats DownloadStats
	if err := json.Unmarshal(data, &stats); err != nil {
		fm.stats = &DownloadStats{
			StartTime:      time.Now(),
			LastUpdateTime: time.Now(),
		}
		return
	}
	
	fm.stats = &stats
}

func (fm *FileSystemManager) saveStats() error {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	
	fm.stats.LastUpdateTime = time.Now()
	data, err := json.MarshalIndent(fm.stats, "", "  ")
	if err != nil {
		return err
	}
	
	return os.WriteFile(fm.statsFile, data, 0644)
}

func (fm *FileSystemManager) loadProcessedList() {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	
	// 扫描已处理的用户目录
	entries, err := os.ReadDir(fm.baseDir)
	if err != nil {
		return
	}
	
	for _, entry := range entries {
		if entry.IsDir() && entry.Name() != ".status" {
			// 检查是否有profile_meta.json文件（实际保存的文件）
			profilePath := filepath.Join(fm.baseDir, entry.Name(), "profile_meta.json")
			if _, err := os.Stat(profilePath); err == nil {
				// 从目录名恢复DID
				did := fm.unsanitizeDID(entry.Name())
				fm.processed[did] = true
			}
		}
	}
}

func (fm *FileSystemManager) IsProcessed(did string) bool {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.processed[did]
}

func (fm *FileSystemManager) MarkProcessed(did string) {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	// log.Info().Msgf("MarkProcessed %s", did)
	// fmt.Println("MarkProcessed", did)
	fm.processed[did] = true
}

func (fm *FileSystemManager) UpdateStats(updates map[string]interface{}) error {
	// 非阻塞发送到异步更新队列
	select {
	case fm.statsUpdates <- updates:
		return nil
	default:
		// 队列满了，直接返回成功（丢弃更新以避免阻塞）
		return nil
	}
}

// statsUpdateWorker 异步统计更新工作器
func (fm *FileSystemManager) statsUpdateWorker() {
	ticker := time.NewTicker(5 * time.Second) // 每5秒批量更新一次
	defer ticker.Stop()
	
	pendingUpdates := make(map[string]int64)
	var lastProcessedDID, lastProcessedFile string
	
	for {
		select {
		case <-fm.statsCtx.Done():
			// 程序关闭前最后一次保存
			fm.applyPendingUpdates(pendingUpdates, lastProcessedDID, lastProcessedFile)
			return
			
		case update := <-fm.statsUpdates:
			// 收集待更新的统计
			for key, value := range update {
				switch key {
				case "processed_users", "completed_users", "failed_users", "deleted_users", "total_records", "users_with_records":
					if delta, ok := value.(int64); ok {
						pendingUpdates[key] += delta
					}
				case "last_processed_did":
					if did, ok := value.(string); ok {
						lastProcessedDID = did
					}
				case "last_processed_file":
					if file, ok := value.(string); ok {
						lastProcessedFile = file
					}
				}
			}
			
		case <-ticker.C:
			// 定期批量应用更新
			fm.applyPendingUpdates(pendingUpdates, lastProcessedDID, lastProcessedFile)
			// 重置待更新的统计
			pendingUpdates = make(map[string]int64)
			lastProcessedDID = ""
			lastProcessedFile = ""
		}
	}
}

// applyPendingUpdates 应用待处理的统计更新
func (fm *FileSystemManager) applyPendingUpdates(pendingUpdates map[string]int64, lastProcessedDID, lastProcessedFile string) {
	if len(pendingUpdates) == 0 && lastProcessedDID == "" && lastProcessedFile == "" {
		return
	}
	
	fm.mu.Lock()
	defer fm.mu.Unlock()
	
	// 应用数值更新
	for key, delta := range pendingUpdates {
		switch key {
		case "processed_users":
			fm.stats.ProcessedUsers += delta
		case "completed_users":
			fm.stats.CompletedUsers += delta
		case "failed_users":
			fm.stats.FailedUsers += delta
		case "deleted_users":
			fm.stats.DeletedUsers += delta
		case "total_records":
			fm.stats.TotalRecords += delta
		case "users_with_records":
			fm.stats.UsersWithRecords += delta
		}
	}
	
	// 应用字符串更新
	if lastProcessedDID != "" {
		fm.stats.LastProcessedDID = lastProcessedDID
	}
	if lastProcessedFile != "" {
		fm.stats.LastProcessedFile = lastProcessedFile
	}
	
	// 保存到文件（可能失败，但不影响程序运行）
	if err := fm.saveStats(); err != nil {
		// 静默失败，避免影响主流程
	}
}

func (fm *FileSystemManager) GetStats() *DownloadStats {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	
	// 返回副本
	stats := *fm.stats
	return &stats
}

func (fm *FileSystemManager) SaveUserProfile(profile *UserProfile) error {
	userDir := filepath.Join(fm.baseDir, fm.sanitizeDID(profile.DID))
	if err := os.MkdirAll(userDir, 0755); err != nil {
		return err
	}
	
	profilePath := filepath.Join(userDir, "profile_meta.json")
	data, err := json.MarshalIndent(profile, "", "  ")
	if err != nil {
		return err
	}
	
	return os.WriteFile(profilePath, data, 0644)
}

func (fm *FileSystemManager) LoadUserProfile(did string) (*UserProfile, error) {
	userDir := filepath.Join(fm.baseDir, fm.sanitizeDID(did))
	profilePath := filepath.Join(userDir, "profile_meta.json")
	
	data, err := os.ReadFile(profilePath)
	if err != nil {
		return nil, err
	}
	
	var profile UserProfile
	err = json.Unmarshal(data, &profile)
	return &profile, err
}

func (fm *FileSystemManager) sanitizeDID(did string) string {
	// 将DID转换为安全的文件夹名
	return sanitizeDIDName(did)
}

func (fm *FileSystemManager) unsanitizeDID(dirName string) string {
	// 从文件夹名恢复DID
	return unsanitizeDIDName(dirName)
}

// 全局函数
func sanitizeDIDName(did string) string {
	// 简单且有效的方法
	result := ""
	for _, r := range did {
		switch r {
		case ':', '/', '\\', '*', '?', '"', '<', '>', '|':
			result += "_"
		default:
			result += string(r)
		}
	}
	return result
}

func unsanitizeDIDName(dirName string) string {
	// 简单的反向转换 - 这个不是完美的，但对于显示目的足够了
	return dirName
}

// Close 关闭FileSystemManager并确保所有待处理的统计更新都被保存
func (fm *FileSystemManager) Close() {
	if fm.statsCancel != nil {
		fm.statsCancel() // 这会触发statsUpdateWorker的最后一次保存
		
		// 等待一小段时间让worker完成最后的保存
		time.Sleep(100 * time.Millisecond)
	}
} 