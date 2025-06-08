package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	// "sync"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"

	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/repo"
	"github.com/bluesky-social/indigo/util"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/ipfs/go-cid"
	"github.com/imax9000/errors"
	"github.com/rs/zerolog"
)

const (
	followersThreshold = 20 // followers大于20才下载records
	maxRetries        = 3   // 最大重试次数
	requestTimeout    = 90 * time.Second // 请求超时
)

type WorkItem struct {
	CSVEntry *CSVEntry
	signal   chan struct{}
}

type WorkerPool struct {
	fsManager        *FileSystemManager
	input            <-chan WorkItem
	limiter          *Limiter
	contactInfo      string
	recordsBaseDir   string
	followRecordsDir string // 符合followers标准的用户额外存储目录
	// bskyHandle     string // 不再需要认证参数
	// bskyPassword   string // 不再需要认证参数

	workerSignals []chan struct{}
	// resize        chan int // 移除动态resize功能
	
	// 认证客户端池 - 不再需要
	// clientPool    chan *xrpc.Client
	// clientPoolMu  sync.Mutex
	// poolSize      int
}

func NewWorkerPool(input <-chan WorkItem, fsManager *FileSystemManager, size int, limiter *Limiter, 
	contactInfo, recordsBaseDir, followRecordsDir string) *WorkerPool {
	
	r := &WorkerPool{
		fsManager:        fsManager,
		input:            input,
		limiter:          limiter,
		contactInfo:      contactInfo,
		recordsBaseDir:   recordsBaseDir,
		followRecordsDir: followRecordsDir,
		// clientPool:     make(chan *xrpc.Client, size),
		// poolSize:       size,
	}
	r.workerSignals = make([]chan struct{}, size)
	for i := range r.workerSignals {
		r.workerSignals[i] = make(chan struct{})
	}
	return r
}

func (p *WorkerPool) Start(ctx context.Context) error {
	go p.run(ctx)
	return nil
}

func (p *WorkerPool) run(ctx context.Context) {
	for _, ch := range p.workerSignals {
		go p.worker(ctx, ch)
	}
	workerPoolSize.Set(float64(len(p.workerSignals)))

	for {
		select {
		case <-ctx.Done():
			for _, ch := range p.workerSignals {
				close(ch)
			}
			return
		}
	}
}

func (p *WorkerPool) worker(ctx context.Context, signal chan struct{}) {
	log := zerolog.Ctx(ctx)
	defer func() {
        if r := recover(); r != nil {
            log.Info().Interface("panic", r).Msg("Worker panic recovered")
            // 可以考虑重启Worker
        }
    }()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-signal:
			log.Info().Msgf("🔍 worker signal received, exiting %s", signal)
			return
		case work := <-p.input:
			log.Info().Msgf("🔍 worker received work %s", work.CSVEntry.DID)
			if err := p.doWork(ctx, work); err != nil {
				log.Error().Err(err).Msgf("Work task %q failed: %s", work.CSVEntry.DID, err)
				
				// 更新失败计数
				p.updateUserProfileStatus(work.CSVEntry.DID, "failed", err.Error())
				// 更新统计
				if err := p.fsManager.UpdateStats(map[string]interface{}{
					"processed_users": int64(1),
					"failed_users":   int64(1),
				}); err != nil {
					log.Warn().Err(err).Msg("Failed to update failure stats")
				}
				usersProcessed.WithLabelValues("false").Inc()
			} else {
				// ✅ 成功时也需要更新文件系统统计
				if err := p.fsManager.UpdateStats(map[string]interface{}{
					"processed_users": int64(1),
					"completed_users": int64(1),
				}); err != nil {
					log.Warn().Err(err).Msg("Failed to update success stats")
				}
				
				usersProcessed.WithLabelValues("true").Inc()
			}
			
			// 标记为已处理
			// log.Info().Msgf("fm processed %s", work.CSVEntry.DID)
			// p.fsManager.MarkProcessed(work.CSVEntry.DID)
			// log.Info().Msgf("worker closing signal %s", work.CSVEntry.DID)
			close(work.signal)
		}
	}
}

func (p *WorkerPool) doWork(ctx context.Context, work WorkItem) error {
	log := zerolog.Ctx(ctx).With().Str("did", work.CSVEntry.DID).Logger()

	// ✅ 删除已处理检查，直接处理所有用户

	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		if attempt > 1 {
			// Simple exponential backoff
			backoffDuration := time.Duration(attempt-1) * 30 * time.Second
			log.Warn().
				Int("attempt", attempt).
				Int("max_retries", maxRetries).
				Dur("backoff", backoffDuration).
				Err(lastErr).
				Msg("Retrying user processing")
			
			// 指数退避
			select {
			case <-time.After(backoffDuration):
				log.Debug().Msg("⏰ 退避等待完成，继续重试")
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		err := p.processUser(ctx, work)
		if err == nil {
			if attempt > 1 {
				log.Info().
					Int("attempt", attempt).
					Msg("✅ 重试成功，用户处理完成")
			}
			return nil
		}
		
		lastErr = err
		log.Warn().
			Err(err).
			Int("attempt", attempt).
			Int("max_retries", maxRetries).
			Msg("⚠️ 用户处理失败")
		
		// 如果是用户不可访问错误，不需要重试
		if p.isUserNotAccessible(err) {
			log.Info().Msg("🚫 用户不可访问，直接标记为删除，不进行重试")
			return p.deleteUser(ctx, work.CSVEntry.DID)
		}
		
		// 如果不是最后一次尝试，显示将要重试的信息
		if attempt < maxRetries {
			log.Info().
				Int("next_attempt", attempt+1).
				Int("max_retries", maxRetries).
				Msg("🕐 将在退避后重试")
		}
	}
	
	log.Error().
		Err(lastErr).
		Int("max_retries", maxRetries).
		Msg("❌ 达到最大重试次数，用户处理最终失败")
	
	return fmt.Errorf("failed after %d attempts: %w", maxRetries, lastErr)
}

func (p *WorkerPool) processUser(ctx context.Context, work WorkItem) error {
	log := zerolog.Ctx(ctx).With().Str("did", work.CSVEntry.DID).Logger()

	log.Info().Msg("🚀 开始处理用户")

	// 创建匿名客户端 (不需要认证即可获取profile)
	log.Debug().Msg("📡 创建匿名客户端...")
	client := &xrpc.Client{
		Host:   "https://public.api.bsky.app",
		Client: util.RobustHTTPClient(),
	}
	client.Client.Timeout = requestTimeout
	userAgent := fmt.Sprintf("Go-http-client/1.1 bsky-downloader/0.1 (%s)", p.contactInfo)
	client.UserAgent = &userAgent
	
	log.Debug().Msg("✅ 匿名客户端创建成功")

	// 设置用户状态为处理中
	p.updateUserProfileStatus(work.CSVEntry.DID, "processing", "")

	// 第一步：获取用户profile (不需要认证)
	log.Info().Msg("📋 正在获取用户档案...")
	profile, err := p.fetchProfile(ctx, client, work.CSVEntry.DID)
	if err != nil {
		log.Error().Err(err).Msg("❌ 获取用户档案失败")
		
		// 检查具体错误类型并记录服务端信息
		if xrpcErr, ok := errors.As[*xrpc.Error](err); ok {
			log.Error().
				Str("endpoint", "https://public.api.bsky.app").
				Int("http_status", xrpcErr.StatusCode).
				Str("wrapped_error", fmt.Sprintf("%v", xrpcErr.Wrapped)).
				Msg("🌐 公共API服务端响应")
		}
		

		
		return fmt.Errorf("failed to fetch profile: %w", err)
	}
	
	// 显示用户基本信息
	followersCount := int64(0)
	if profile.FollowersCount != nil {
		followersCount = *profile.FollowersCount
	}
	followsCount := int64(0)
	if profile.FollowsCount != nil {
		followsCount = *profile.FollowsCount
	}
	postsCount := int64(0)
	if profile.PostsCount != nil {
		postsCount = *profile.PostsCount
	}
	
	displayName := "未设置"
	if profile.DisplayName != nil && *profile.DisplayName != "" {
		displayName = *profile.DisplayName
	}
	
	log.Info().
		Str("handle", profile.Handle).
		Str("display_name", displayName).
		Int64("followers", followersCount).
		Int64("follows", followsCount).
		Int64("posts", postsCount).
		Msg("✅ 用户档案获取成功")

	// 保存profile信息
	log.Debug().Msg("💾 保存用户档案文件...")
	userDir := filepath.Join(p.recordsBaseDir, sanitizeDIDName(work.CSVEntry.DID))
	if err := os.MkdirAll(userDir, 0755); err != nil {
		return fmt.Errorf("failed to create user directory: %w", err)
	}

	profilePath := filepath.Join(userDir, "profile.json")
	if err := p.saveJSON(profilePath, profile); err != nil {
		return fmt.Errorf("failed to save profile: %w", err)
	}
	log.Debug().Str("path", profilePath).Msg("✅ 用户档案文件保存成功")

	// 创建用户档案记录
	userProfile := &UserProfile{
		DID:            work.CSVEntry.DID,
		Handle:         profile.Handle,
		FollowersCount: followersCount,
		Status:         "completed",
		LastProcessed:  time.Now(),
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
		FailedAttempts: 0,
	}

	if profile.DisplayName != nil {
		userProfile.DisplayName = *profile.DisplayName
	}
	if profile.Description != nil {
		userProfile.Description = *profile.Description
	}
	if profile.Avatar != nil {
		userProfile.Avatar = *profile.Avatar
	}
	if profile.Banner != nil {
		userProfile.Banner = *profile.Banner
	}
	if profile.FollowsCount != nil {
		userProfile.FollowsCount = *profile.FollowsCount
	}
	if profile.PostsCount != nil {
		userProfile.PostsCount = *profile.PostsCount
	}

	recordsCount := int64(0)
	
	// 第二步：检查是否需要下载records
	if followersCount > followersThreshold {
		log.Info().
			Int64("followers", followersCount).
			Int64("threshold", followersThreshold).
			Msg("📦 用户符合条件，开始下载完整记录...")
		
		records, err := p.fetchUserRecords(ctx, work.CSVEntry.DID)
		if err != nil {
			log.Warn().Err(err).Msg("⚠️ 下载用户记录失败，但档案已保存")
			
			// 记录具体的失败原因
			if xrpcErr, ok := errors.As[*xrpc.Error](err); ok {
				log.Warn().
					Str("wrapped_error", fmt.Sprintf("%v", xrpcErr.Wrapped)).
					Int("pds_status", xrpcErr.StatusCode).
					Msg("🏠 PDS服务器响应详情")
			}
			
			// 不返回错误，因为profile已经成功保存
		} else {
			recordsCount = int64(len(records))
			log.Info().Int64("records_count", recordsCount).Msg("✅ 用户记录获取成功")
			
			// 保存到原有目录
			recordsPath := filepath.Join(userDir, "records.json")
			log.Debug().Str("path", recordsPath).Msg("💾 保存用户记录文件...")
			if err := p.saveJSON(recordsPath, records); err != nil {
				log.Warn().Err(err).Msg("⚠️ 保存用户记录文件失败")
			} else {
				log.Debug().Msg("✅ 用户记录文件保存成功")
			}
			
			// 额外保存到高followers用户目录
			followUserDir := filepath.Join(p.followRecordsDir, sanitizeDIDName(work.CSVEntry.DID))
			if err := os.MkdirAll(followUserDir, 0755); err != nil {
				log.Warn().Err(err).Str("follow_dir", followUserDir).Msg("⚠️ 创建高followers用户目录失败")
			} else {
				// 保存profile到高followers目录
				followProfilePath := filepath.Join(followUserDir, "profile.json")
				if err := p.saveJSON(followProfilePath, profile); err != nil {
					log.Warn().Err(err).Msg("⚠️ 保存高followers用户档案失败")
				} else {
					log.Debug().Str("path", followProfilePath).Msg("✅ 高followers用户档案保存成功")
				}
				
				// 保存records到高followers目录
				followRecordsPath := filepath.Join(followUserDir, "records.json")
				if err := p.saveJSON(followRecordsPath, records); err != nil {
					log.Warn().Err(err).Msg("⚠️ 保存高followers用户记录失败")
				} else {
					log.Info().
						Str("path", followRecordsPath).
						Int64("followers", followersCount).
						Int64("records", recordsCount).
						Msg("✅ 高followers用户数据额外保存完成")
				}
			}
		}
		
		// 更新统计
		if err := p.fsManager.UpdateStats(map[string]interface{}{
			"users_with_records": int64(1),
		}); err != nil {
			log.Warn().Err(err).Msg("⚠️ 更新用户记录统计失败")
		}
	} else {
		log.Info().
			Int64("followers", followersCount).
			Int64("threshold", followersThreshold).
			Msg("⏭️ 用户粉丝数未达到阈值，跳过记录下载")
	}

	userProfile.RecordsCount = recordsCount
	
	// 保存用户档案元数据
	log.Debug().Msg("💾 保存用户元数据...")
	if err := p.fsManager.SaveUserProfile(userProfile); err != nil {
		log.Warn().Err(err).Msg("⚠️ 保存用户档案元数据失败")
	} else {
		log.Debug().Msg("✅ 用户元数据保存成功")
	}

	// 更新全局统计
	log.Debug().Msg("📊 更新全局统计...")
	if err := p.fsManager.UpdateStats(map[string]interface{}{
		"processed_users":   int64(1),
		"completed_users":   int64(1),
		"total_records":     recordsCount,
		"last_processed_did": work.CSVEntry.DID,
	}); err != nil {
		log.Warn().Err(err).Msg("⚠️ 更新全局统计失败")
	}

	log.Info().
		Int64("followers", followersCount).
		Int64("records", recordsCount).
		Str("handle", profile.Handle).
		Msg("🎉 用户处理完成")
	return nil
}

func (p *WorkerPool) fetchProfile(ctx context.Context, client *xrpc.Client, did string) (*bsky.ActorDefs_ProfileViewDetailed, error) {
	if p.limiter != nil {
		if err := p.limiter.Wait(ctx, "public.api.bsky.app"); err != nil {
			return nil, fmt.Errorf("failed to wait on rate limiter: %w", err)
		}
	}

	return bsky.ActorGetProfile(ctx, client, did)
}

func (p *WorkerPool) fetchUserRecords(ctx context.Context, did string) (map[string]interface{}, error) {
	log := zerolog.Ctx(ctx).With().Str("did", did).Logger()
	
	// Parse DID and get PDS endpoint
	atid, err := syntax.ParseAtIdentifier(did)
	if err != nil {
		return nil, fmt.Errorf("failed to parse DID: %w", err)
	}

	dir := identity.DefaultDirectory()
	ident, err := dir.Lookup(ctx, *atid)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup identity: %w", err)
	}

	if ident.PDSEndpoint() == "" {
		return nil, fmt.Errorf("no PDS endpoint for identity")
	}
	
	// Get the host from PDS endpoint for rate limiting
	u, err := url.Parse(ident.PDSEndpoint())
	if err != nil {
		return nil, fmt.Errorf("failed to parse PDS endpoint: %w", err)
	}

retry:
	if p.limiter != nil {
		if err := p.limiter.Wait(ctx, u.Host); err != nil {
			return nil, fmt.Errorf("failed to wait on rate limiter: %w", err)
		}
	}

	// Create PDS client
	xrpcc := xrpc.Client{
		Host:   ident.PDSEndpoint(),
		Client: util.RobustHTTPClient(),
	}
	xrpcc.Client.Timeout = requestTimeout
	
	userAgent := fmt.Sprintf("Go-http-client/1.1 bsky-downloader/0.1 (%s)", p.contactInfo)
	xrpcc.UserAgent = &userAgent

	// Fetch repository data
	repoBytes, err := comatproto.SyncGetRepo(ctx, &xrpcc, ident.DID.String(), "")
	if err != nil {
		if xrpcErr, ok := errors.As[*xrpc.Error](err); ok {
			if xrpcErr.IsThrottled() && xrpcErr.Ratelimit != nil {
				log.Debug().Str("pds", u.Host).Msgf("Hit a rate limit (%s), sleeping until %s", xrpcErr.Ratelimit.Policy, xrpcErr.Ratelimit.Reset)
				time.Sleep(time.Until(xrpcErr.Ratelimit.Reset))
				goto retry
			}
		}
		return nil, fmt.Errorf("failed to fetch repo: %w", err)
	}
	
	if len(repoBytes) == 0 {
		return nil, fmt.Errorf("PDS returned zero bytes")
	}

	// Parse repository data
	r, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(repoBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to read repo from CAR: %w", err)
	}

	// Extract records
	records := make(map[string]interface{})
	err = r.ForEach(ctx, "", func(k string, v cid.Cid) error {
		_, rec, err := r.GetRecord(ctx, k)
		if err != nil {
			log.Warn().Err(err).Str("record_key", k).Msg("Failed to get record, skipping")
			return nil // continue processing other records
		}
		records[k] = rec
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to extract records: %w", err)
	}

	return records, nil
}

func (p *WorkerPool) saveJSON(path string, data interface{}) error {
	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, jsonData, 0644)
}

func (p *WorkerPool) isUserNotAccessible(err error) bool {
	// 检查错误信息
	errStr := strings.ToLower(err.Error())
	isNotAccessible := strings.Contains(errStr, "not found") ||
		strings.Contains(errStr, "does not exist") ||
		strings.Contains(errStr, "suspended") ||
		strings.Contains(errStr, "deleted") ||
		strings.Contains(errStr, "404") ||
		strings.Contains(errStr, "403") ||
		strings.Contains(errStr, "410") ||
		strings.Contains(errStr, "accountdeactivated") ||   // 账户被停用
		strings.Contains(errStr, "account is deactivated") || // 账户停用的详细消息
		strings.Contains(errStr, "accountsuspended") ||     // 账户被暂停
		strings.Contains(errStr, "account is suspended") || // 账户暂停的详细消息
		strings.Contains(errStr, "accountdeleted") ||       // 账户被删除
		strings.Contains(errStr, "account is deleted")     // 账户删除的详细消息
	
	// // 记录错误类型
	// if isNotAccessible {
	// 	errorTypes.WithLabelValues("user_not_accessible").Inc()
	// } else if strings.Contains(errStr, "unrecognized lexicon type") {
	// 	errorTypes.WithLabelValues("lexicon_parse_error").Inc()
	// } else if strings.Contains(errStr, "timeout") || strings.Contains(errStr, "context deadline exceeded") {
	// 	errorTypes.WithLabelValues("timeout").Inc()
	// } else if strings.Contains(errStr, "authentication failed") {
	// 	errorTypes.WithLabelValues("auth_failed").Inc()
	// } else {
	// 	errorTypes.WithLabelValues("other").Inc()
	// }
	
	return isNotAccessible
}

func (p *WorkerPool) deleteUser(ctx context.Context, did string) error {
	log := zerolog.Ctx(ctx).With().Str("did", did).Logger()
	log.Info().Msg("🗑️ 用户不可访问，开始删除处理...")

	// 标记用户为已删除
	log.Debug().Msg("🏷️ 标记用户状态为已删除...")
	p.updateUserProfileStatus(did, "deleted", "User not accessible")

	// 删除用户文件夹（如果存在）
	userDir := filepath.Join(p.recordsBaseDir, sanitizeDIDName(did))
	if stat, err := os.Stat(userDir); err == nil {
		if stat.IsDir() {
			log.Debug().Str("user_dir", userDir).Msg("🗂️ 删除用户数据目录...")
			if err := os.RemoveAll(userDir); err != nil {
				log.Warn().Err(err).Str("user_dir", userDir).Msg("⚠️ 删除用户目录失败")
			} else {
				log.Debug().Msg("✅ 用户数据目录删除成功")
			}
		}
	} else {
		log.Debug().Msg("📁 用户数据目录不存在，无需删除")
	}

	// 更新统计
	log.Debug().Msg("📊 更新全局统计...")
	if err := p.fsManager.UpdateStats(map[string]interface{}{
		"processed_users": int64(1),
		"deleted_users":   int64(1),
	}); err != nil {
		log.Warn().Err(err).Msg("⚠️ 更新删除用户统计失败")
	}

	log.Info().Msg("✅ 用户删除处理完成")
	return nil
}

func (p *WorkerPool) updateUserProfileStatus(did, status, lastError string) {
	userProfile := &UserProfile{
		DID:           did,
		Status:        status,
		LastError:     lastError,
		LastProcessed: time.Now(),
		UpdatedAt:     time.Now(),
	}
	
	// Try to load existing profile
	if existing, err := p.fsManager.LoadUserProfile(did); err == nil {
		userProfile = existing
		userProfile.Status = status
		userProfile.LastError = lastError
		userProfile.LastProcessed = time.Now()
		userProfile.UpdatedAt = time.Now()
		if status == "failed" {
			userProfile.FailedAttempts++
		} else if status == "completed" {
			userProfile.FailedAttempts = 0
		}
	} else {
		userProfile.CreatedAt = time.Now()
		if status == "failed" {
			userProfile.FailedAttempts = 1
		}
	}
	
	// Save to filesystem
	p.fsManager.SaveUserProfile(userProfile)
}

 