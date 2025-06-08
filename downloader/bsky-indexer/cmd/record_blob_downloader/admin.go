package main

import (
	"encoding/json"
	"net/http"
)

func AddAdminHandlers(fsManager *FileSystemManager) {
	http.HandleFunc("/stats", handleStats(fsManager))
	http.HandleFunc("/health", handleHealth())
}

func handleStats(fsManager *FileSystemManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		stats := fsManager.GetStats()
		
		response := map[string]interface{}{
			"status":              "running",
			"storage_backend":     "filesystem",
			"total_users":         stats.TotalUsers,
			"processed_users":     stats.ProcessedUsers,
			"completed_users":     stats.CompletedUsers,
			"failed_users":        stats.FailedUsers,
			"deleted_users":       stats.DeletedUsers,
			"total_records":       stats.TotalRecords,
			"users_with_records":  stats.UsersWithRecords,
			"last_processed_file": stats.LastProcessedFile,
			"last_processed_did":  stats.LastProcessedDID,
			"start_time":          stats.StartTime,
			"last_update_time":    stats.LastUpdateTime,
		}
		
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}
}

func handleHealth() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		health := map[string]interface{}{
			"status":         "healthy",
			"service":        "record-blob-downloader",
			"storage_backend": "filesystem",
		}
		
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(health)
	}
} 