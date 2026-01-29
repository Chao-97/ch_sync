package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// StateManager 状态管理器
type StateManager struct {
	stateFile string
	state     *SyncState
	mu        sync.Mutex
}

// SyncState 同步状态
type SyncState struct {
	RunID       string                 `json:"run_id"`
	StartTime   time.Time              `json:"start_time"`
	LastUpdated time.Time              `json:"last_updated"`
	Tables      map[string]*TableState `json:"tables"`
}

// TableState 表状态
type TableState struct {
	Status            string         `json:"status"` // "pending", "in_progress", "completed"
	LastSyncedTime    time.Time      `json:"last_synced_time"`
	RecordsSynced     int            `json:"records_synced"`
	CompletedSegments []TimeSegment  `json:"completed_segments"`
}

// TimeSegment 时间分段
type TimeSegment struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

// TimeRange 时间范围
type TimeRange struct {
	Start time.Time
	End   time.Time
}

// NewStateManager 创建状态管理器
func NewStateManager(stateFile string) *StateManager {
	sm := &StateManager{
		stateFile: stateFile,
		state: &SyncState{
			RunID:     fmt.Sprintf("sync_%s", time.Now().Format("20060102_150405")),
			StartTime: time.Now(),
			Tables:    make(map[string]*TableState),
		},
	}

	// 尝试加载已有状态
	sm.LoadState()

	return sm
}

// LoadState 加载状态
func (sm *StateManager) LoadState() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	data, err := os.ReadFile(sm.stateFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // 文件不存在，使用默认状态
		}
		return err
	}

	return json.Unmarshal(data, sm.state)
}

// SaveState 保存状态
// SaveState 保存状态到文件（加锁版本）
func (sm *StateManager) SaveState() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	return sm.saveStateUnlocked()
}

// saveStateUnlocked 保存状态到文件（不加锁，内部使用）
func (sm *StateManager) saveStateUnlocked() error {
	sm.state.LastUpdated = time.Now()

	data, err := json.MarshalIndent(sm.state, "", "  ")
	if err != nil {
		return err
	}

	// 原子写入
	tmpFile := sm.stateFile + ".tmp"
	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
		return err
	}

	return os.Rename(tmpFile, sm.stateFile)
}

// IsSegmentCompleted 检查分段是否已完成
func (sm *StateManager) IsSegmentCompleted(tableName string, segment TimeSegment) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	tableState, exists := sm.state.Tables[tableName]
	if !exists {
		return false
	}

	for _, completed := range tableState.CompletedSegments {
		if completed.Start.Equal(segment.Start) && completed.End.Equal(segment.End) {
			return true
		}
	}

	return false
}

// MarkSegmentCompleted 标记分段已完成
func (sm *StateManager) MarkSegmentCompleted(tableName string, segment TimeSegment, recordCount int) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, exists := sm.state.Tables[tableName]; !exists {
		sm.state.Tables[tableName] = &TableState{
			Status:            "in_progress",
			CompletedSegments: []TimeSegment{},
		}
	}

	tableState := sm.state.Tables[tableName]
	tableState.CompletedSegments = append(tableState.CompletedSegments, segment)
	tableState.RecordsSynced += recordCount
	tableState.LastSyncedTime = time.Now()

	// 自动保存
	sm.saveStateUnlocked()
}

// MarkTableCompleted 标记表同步完成
func (sm *StateManager) MarkTableCompleted(tableName string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, exists := sm.state.Tables[tableName]; !exists {
		sm.state.Tables[tableName] = &TableState{
			CompletedSegments: []TimeSegment{},
		}
	}

	sm.state.Tables[tableName].Status = "completed"
	sm.saveStateUnlocked()
}

// MarkTableInProgress 标记表正在同步
func (sm *StateManager) MarkTableInProgress(tableName string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, exists := sm.state.Tables[tableName]; !exists {
		sm.state.Tables[tableName] = &TableState{
			CompletedSegments: []TimeSegment{},
		}
	}

	sm.state.Tables[tableName].Status = "in_progress"
	sm.saveStateUnlocked()
}

// GetTableState 获取表状态
func (sm *StateManager) GetTableState(tableName string) *TableState {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	return sm.state.Tables[tableName]
}

// ClearState 清空状态
func (sm *StateManager) ClearState() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.state = &SyncState{
		RunID:     fmt.Sprintf("sync_%s", time.Now().Format("20060102_150405")),
		StartTime: time.Now(),
		Tables:    make(map[string]*TableState),
	}

	return sm.SaveState()
}

// GetTotalRecordsSynced 获取总同步记录数
func (sm *StateManager) GetTotalRecordsSynced() int {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	total := 0
	for _, tableState := range sm.state.Tables {
		total += tableState.RecordsSynced
	}
	return total
}
