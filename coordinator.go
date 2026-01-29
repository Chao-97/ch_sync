package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

// SyncCoordinator 同步协调器
type SyncCoordinator struct {
	sourceDB *sql.DB
	targetDB *sql.DB
	config   *Config
	state    *StateManager
}

// NewSyncCoordinator 创建同步协调器
func NewSyncCoordinator(sourceDB, targetDB *sql.DB, config *Config) *SyncCoordinator {
	state := NewStateManager(config.Sync.StateFile)
	return &SyncCoordinator{
		sourceDB: sourceDB,
		targetDB: targetDB,
		config:   config,
		state:    state,
	}
}

// SyncAllTables 并行同步所有表
func (c *SyncCoordinator) SyncAllTables(ctx context.Context) error {
	// 过滤出启用的表
	enabledTables := []TableConfig{}
	for _, table := range c.config.Tables {
		if table.Enabled {
			enabledTables = append(enabledTables, table)
		}
	}

	if len(enabledTables) == 0 {
		return fmt.Errorf("no enabled tables to sync")
	}

	log.Printf("🚀 开始同步 %d 个表（最大并发: %d）",
		len(enabledTables), c.config.Sync.MaxConcurrency)

	// 并发控制
	semaphore := make(chan struct{}, c.config.Sync.MaxConcurrency)
	errChan := make(chan error, len(enabledTables))
	var wg sync.WaitGroup

	// 启动同步任务
	for _, tableConfig := range enabledTables {
		wg.Add(1)
		go func(tc TableConfig) {
			defer wg.Done()

			// 获取信号量
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			log.Printf("🚦 %s: 开始同步...", tc.Name)

			// 标记表为进行中
			c.state.MarkTableInProgress(tc.Name)

			// 创建同步器
			syncer, err := NewUniversalSyncer(tc, c.sourceDB, c.targetDB, c.config, c.state)
			if err != nil {
				log.Printf("❌ %s: 创建同步器失败: %v", tc.Name, err)
				errChan <- fmt.Errorf("%s: %w", tc.Name, err)
				return
			}

			// 执行同步
			startTime := time.Now()
			if err := syncer.Sync(ctx); err != nil {
				// 如果是源表为空，则优雅地跳过，不计入错误
				if errors.Is(err, ErrSourceTableEmpty) {
					log.Printf("⏭️  %s: 源表为空，跳过同步", tc.Name)
					return
				}
				log.Printf("❌ %s: 同步失败: %v", tc.Name, err)
				errChan <- fmt.Errorf("%s: %w", tc.Name, err)
				return
			}
			duration := time.Since(startTime)

			// 标记表为已完成
			c.state.MarkTableCompleted(tc.Name)
			tableState := c.state.GetTableState(tc.Name)
			if tableState != nil {
				log.Printf("✅ %s: 同步完成 | 耗时: %s, 记录数: %d",
					tc.Name, FormatDuration(duration), tableState.RecordsSynced)
			} else {
				log.Printf("✅ %s: 同步完成 | 耗时: %s", tc.Name, FormatDuration(duration))
			}
		}(tableConfig)
	}

	// 等待所有任务完成
	wg.Wait()
	close(errChan)

	// 收集错误
	var errors []error
	for err := range errChan {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		log.Printf("❌ 同步失败，错误数量: %d", len(errors))
		for i, err := range errors {
			log.Printf("  %d. %v", i+1, err)
		}
		return fmt.Errorf("sync failed for %d tables", len(errors))
	}

	log.Printf("🎉 所有表同步完成")
	return nil
}

// GetState 获取状态管理器
func (c *SyncCoordinator) GetState() *StateManager {
	return c.state
}

// SyncAllTablesWithSmartMode 智能模式同步所有表
func (c *SyncCoordinator) SyncAllTablesWithSmartMode(ctx context.Context, realtimeThreshold time.Duration) error {
	// 过滤出启用的表
	enabledTables := []TableConfig{}
	for _, table := range c.config.Tables {
		if table.Enabled {
			enabledTables = append(enabledTables, table)
		}
	}

	if len(enabledTables) == 0 {
		return fmt.Errorf("no enabled tables to sync")
	}

	log.Printf("🚀 智能模式：开始同步 %d 个表（最大并发: %d）",
		len(enabledTables), c.config.Sync.MaxConcurrency)
	log.Printf("⚙️  实时模式阈值: %s（延迟超过此值将先追平历史数据）", FormatDuration(realtimeThreshold))

	// 并发控制
	semaphore := make(chan struct{}, c.config.Sync.MaxConcurrency)
	errChan := make(chan error, len(enabledTables))
	var wg sync.WaitGroup

	// 启动同步任务
	for _, tableConfig := range enabledTables {
		wg.Add(1)
		go func(tc TableConfig) {
			defer wg.Done()

			// 获取信号量
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			log.Printf("🚦 %s: 开始智能同步...", tc.Name)

			// 标记表为进行中
			c.state.MarkTableInProgress(tc.Name)

			// 创建同步器
			syncer, err := NewUniversalSyncer(tc, c.sourceDB, c.targetDB, c.config, c.state)
			if err != nil {
				log.Printf("❌ %s: 创建同步器失败: %v", tc.Name, err)
				errChan <- fmt.Errorf("%s: %w", tc.Name, err)
				return
			}

			// 执行智能同步
			startTime := time.Now()
			if err := syncer.SyncWithRealtimeMode(ctx, realtimeThreshold); err != nil {
				// 如果是源表为空，则优雅地跳过，不计入错误
				if errors.Is(err, ErrSourceTableEmpty) {
					log.Printf("⏭️  %s: 源表为空，跳过同步", tc.Name)
					return
				}
				log.Printf("❌ %s: 同步失败: %v", tc.Name, err)
				errChan <- fmt.Errorf("%s: %w", tc.Name, err)
				return
			}
			duration := time.Since(startTime)

			// 标记表为已完成
			c.state.MarkTableCompleted(tc.Name)
			tableState := c.state.GetTableState(tc.Name)
			if tableState != nil {
				log.Printf("✅ %s: 同步完成 | 耗时: %s, 记录数: %d",
					tc.Name, FormatDuration(duration), tableState.RecordsSynced)
			} else {
				log.Printf("✅ %s: 同步完成 | 耗时: %s", tc.Name, FormatDuration(duration))
			}
		}(tableConfig)
	}

	// 等待所有任务完成
	wg.Wait()
	close(errChan)

	// 收集错误
	var errors []error
	for err := range errChan {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		log.Printf("❌ 同步失败，错误数量: %d", len(errors))
		for i, err := range errors {
			log.Printf("  %d. %v", i+1, err)
		}
		return fmt.Errorf("sync failed for %d tables", len(errors))
	}

	return nil
}
