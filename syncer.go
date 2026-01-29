package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

// ErrSourceTableEmpty 源表为空错误（用于跳过同步）
var ErrSourceTableEmpty = errors.New("source table is empty")

// UniversalSyncer 通用同步器
type UniversalSyncer struct {
	tableName      string
	tableConfig    TableConfig
	tableSchema    *TableSchema
	sourceDB       *sql.DB
	targetDB       *sql.DB
	config         *Config
	state          *StateManager
	deduplicator   *Deduplicator
	colTypeMap     map[string]string // 列名到类型的映射，用于类型转换
	skipCheckpoint bool              // 是否跳过断点续传检查（实时模式使用）
}

// NewUniversalSyncer 创建通用同步器
func NewUniversalSyncer(
	tableConfig TableConfig,
	sourceDB, targetDB *sql.DB,
	config *Config,
	state *StateManager,
) (*UniversalSyncer, error) {
	// 自动检测表结构
	schema, err := DetectTableSchema(sourceDB, tableConfig.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to detect schema for %s: %w", tableConfig.Name, err)
	}

	// 验证时间字段是否存在
	if !schema.HasColumn(tableConfig.TimeField) {
		return nil, fmt.Errorf("time field '%s' not found in table %s. Available columns: %v",
			tableConfig.TimeField, tableConfig.Name, schema.GetColumnNames())
	}

	// 验证去重字段是否存在
	missingKeys := []string{}
	for _, key := range tableConfig.DedupeKeys {
		if !schema.HasColumn(key) {
			missingKeys = append(missingKeys, key)
		}
	}
	if len(missingKeys) > 0 {
		return nil, fmt.Errorf("deduplication keys not found in table %s: %v. Available columns: %v",
			tableConfig.Name, missingKeys, schema.GetColumnNames())
	}

	// 创建去重器
	deduplicator := NewDeduplicator(tableConfig.DedupeKeys, tableConfig.TimeField)

	// 构建列类型映射
	colTypeMap := make(map[string]string)
	for _, col := range schema.Columns {
		colTypeMap[col.Name] = col.Type
	}

	return &UniversalSyncer{
		tableName:      tableConfig.Name,
		tableConfig:    tableConfig,
		tableSchema:    schema,
		sourceDB:       sourceDB,
		targetDB:       targetDB,
		config:         config,
		state:          state,
		deduplicator:   deduplicator,
		colTypeMap:     colTypeMap,
		skipCheckpoint: false, // 默认使用断点续传
	}, nil
}

// Sync 执行同步
func (s *UniversalSyncer) Sync(ctx context.Context) error {
	mode := s.tableConfig.GetEffectiveMode(s.config.Sync.Mode)

	if mode == "full" {
		return s.fullSync(ctx)
	}
	return s.incrementalSync(ctx)
}

// SyncWithRealtimeMode 智能同步：先追平历史数据，再进入实时监控模式
func (s *UniversalSyncer) SyncWithRealtimeMode(ctx context.Context, realtimeThreshold time.Duration) error {
	// 1. 查询目标库和源库的最新时间
	timeField := s.tableConfig.TimeField
	query := fmt.Sprintf("SELECT MAX(%s) FROM %s", timeField, s.tableName)

	var maxTimeTarget sql.NullTime
	err := s.targetDB.QueryRowContext(ctx, query).Scan(&maxTimeTarget)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("failed to query target max time: %w", err)
	}

	var maxTimeSource sql.NullTime
	err = s.sourceDB.QueryRowContext(ctx, query).Scan(&maxTimeSource)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("failed to query source max time: %w", err)
	}

	// 验证时间有效性（ClickHouse 有效范围: 1900-01-01 到 2262-04-11）
	minValidTime := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
	now := time.Now()
	maxFutureTime := now.Add(24 * time.Hour)

	targetTimeValid := maxTimeTarget.Valid && maxTimeTarget.Time.After(minValidTime) && maxTimeTarget.Time.Before(maxFutureTime)
	sourceTimeValid := maxTimeSource.Valid && maxTimeSource.Time.After(minValidTime) && maxTimeSource.Time.Before(maxFutureTime)

	// 2. 判断是否需要历史数据追平
	needCatchup := false

	if !targetTimeValid {
		// 目标库为空或无效
		if !sourceTimeValid {
			// 源库也无效，跳过
			log.Printf("⏭️  %s: 源库无数据，跳过同步", s.tableName)
			return ErrSourceTableEmpty
		}
		log.Printf("📊 %s: 目标库为空或时间无效，开始初始化同步...", s.tableName)
		needCatchup = true
	} else if sourceTimeValid {
		// 都有效，计算延迟（用源库和目标库的差值）
		lag := maxTimeSource.Time.Sub(maxTimeTarget.Time)
		if lag > realtimeThreshold {
			log.Printf("📊 %s: 数据延迟 %s（源库: %s, 目标库: %s），开始追平历史数据...",
				s.tableName, FormatDuration(lag),
				maxTimeSource.Time.Format("2006-01-02 15:04:05"),
				maxTimeTarget.Time.Format("2006-01-02 15:04:05"))
			needCatchup = true
		}
	}

	if needCatchup {
		// 历史追平模式：使用断点续传
		s.skipCheckpoint = false

		// 执行历史数据同步
		if err := s.incrementalSync(ctx); err != nil {
			// 如果是源表为空错误，直接返回
			if errors.Is(err, ErrSourceTableEmpty) {
				return err
			}
			return fmt.Errorf("failed to catch up historical data: %w", err)
		}

		log.Printf("✅ %s: 历史数据已追平", s.tableName)
	}

	// 3. 进入实时增量模式：不使用断点续传
	log.Printf("🔄 %s: 已进入实时增量模式（监控最新变化）", s.tableName)
	s.skipCheckpoint = true
	return s.realtimeIncrementalSync(ctx)
}

// realtimeIncrementalSync 实时增量同步（只同步最新的时间窗口）
// 使用双向时间窗口检查，防止数据库切换时的数据丢失
func (s *UniversalSyncer) realtimeIncrementalSync(ctx context.Context) error {
	timeField := s.tableConfig.TimeField
	query := fmt.Sprintf("SELECT MAX(%s) FROM %s", timeField, s.tableName)

	// 1. 查询目标库最新时间
	var maxTimeTarget sql.NullTime
	err := s.targetDB.QueryRowContext(ctx, query).Scan(&maxTimeTarget)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("failed to query target max time: %w", err)
	}

	// 2. 查询源库最新时间
	var maxTimeSource sql.NullTime
	err = s.sourceDB.QueryRowContext(ctx, query).Scan(&maxTimeSource)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("failed to query source max time: %w", err)
	}

	// 验证时间有效性
	minValidTime := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
	now := time.Now()
	maxFutureTime := now.Add(24 * time.Hour)

	targetTimeValid := maxTimeTarget.Valid && maxTimeTarget.Time.After(minValidTime) && maxTimeTarget.Time.Before(maxFutureTime)
	sourceTimeValid := maxTimeSource.Valid && maxTimeSource.Time.After(minValidTime) && maxTimeSource.Time.Before(maxFutureTime)

	// 3. 确定同步时间窗口
	var startTime, endTime time.Time
	backwardWindow := 5 * time.Minute // 回溯窗口

	if !targetTimeValid {
		// 目标库为空或时间无效
		if !sourceTimeValid {
			// 源库也无有效数据，不同步
			return nil
		}
		// 从5分钟前开始
		startTime = now.Add(-backwardWindow)
		endTime = maxTimeSource.Time
	} else if !sourceTimeValid {
		// 源库为空（罕见情况），不同步
		return nil
	} else {
		// 4. 双向时间窗口策略
		// 使用回溯窗口从目标库最新时间往前检查
		startTime = maxTimeTarget.Time.Add(-backwardWindow)
		// endTime 使用源库最大时间，并加 1 秒确保包含边界数据
		endTime = maxTimeSource.Time.Add(1 * time.Second)

		// 5. 检测数据库切换场景
		if maxTimeSource.Time.Before(maxTimeTarget.Time) {
			log.Printf("⚠️  %s: 检测到源库时间(%s)早于目标库时间(%s)，可能发生了数据库切换",
				s.tableName,
				maxTimeSource.Time.Format("2006-01-02 15:04:05"),
				maxTimeTarget.Time.Format("2006-01-02 15:04:05"))
			log.Printf("🔍 %s: 回溯检查最近 %v 的数据，确保不遗漏切换窗口期的数据...",
				s.tableName, backwardWindow)

			// 在切换场景下，endTime 使用源库最大时间 + 1秒
			// startTime 已经是 maxTimeTarget - backwardWindow
			// 这样可以捕获切换窗口期内未同步的数据
		} else {
			// 正常场景：源库时间 >= 目标库时间
			// 使用较小的回溯窗口（5秒），提高实时性
			startTime = maxTimeTarget.Time.Add(-5 * time.Second)
		}
	}

	// 2. 查询源库是否有新数据
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s >= ? AND %s <= ?",
		s.tableName, timeField, timeField)

	var newRecordCount int64
	err = s.sourceDB.QueryRowContext(ctx, countQuery, startTime, endTime).Scan(&newRecordCount)
	if err != nil {
		return fmt.Errorf("failed to count new records: %w", err)
	}

	if newRecordCount == 0 {
		// 没有新数据，静默返回
		return nil
	}

	log.Printf("🔍 %s: 检测到 %d 条新记录（%s ~ %s）",
		s.tableName, newRecordCount,
		startTime.Format("15:04:05"),
		endTime.Format("15:04:05"))

	// 3. 同步新数据
	segment := TimeSegment{Start: startTime, End: endTime}
	recordCount, err := s.syncSegment(ctx, segment)
	if err != nil {
		return fmt.Errorf("failed to sync new records: %w", err)
	}

	if recordCount > 0 {
		log.Printf("✅ %s: 实时同步完成，新增 %d 条记录", s.tableName, recordCount)
	}

	return nil
}

// incrementalSync 增量同步
func (s *UniversalSyncer) incrementalSync(ctx context.Context) error {
	// 1. 确定时间范围
	timeRange, err := s.determineTimeRange()
	if err != nil {
		return err
	}

	// 如果时间范围无效（开始时间>=结束时间），跳过同步
	if !timeRange.Start.Before(timeRange.End) {
		log.Printf("⏭️  %s: 无需同步（已是最新）", s.tableName)
		return nil
	}

	log.Printf("📊 %s: 同步时间范围 %s ~ %s",
		s.tableName, timeRange.Start.Format(time.RFC3339), timeRange.End.Format(time.RFC3339))

	// 2. 按天分段
	segments := s.segmentTimeRange(timeRange)
	log.Printf("📦 %s: 分为 %d 个日分段", s.tableName, len(segments))

	// 3. 逐段同步
	totalRecords := 0
	for i, segment := range segments {
		// 检查是否已完成（断点续传）
		if !s.skipCheckpoint && s.state.IsSegmentCompleted(s.tableName, segment) {
			log.Printf("⏭️  %s: 分段 %d/%d 已完成，跳过", s.tableName, i+1, len(segments))
			continue
		}

		// 同步该分段
		recordCount, err := s.syncSegment(ctx, segment)
		if err != nil {
			return fmt.Errorf("failed to sync segment %v: %w", segment, err)
		}

		totalRecords += recordCount

		// 保存检查点（仅在非跳过检查点模式下）
		if !s.skipCheckpoint {
			s.state.MarkSegmentCompleted(s.tableName, segment, recordCount)
		}

		log.Printf("✅ %s: 分段 %d/%d 完成，同步 %d 条记录",
			s.tableName, i+1, len(segments), recordCount)
	}

	log.Printf("🎉 %s: 增量同步完成，总计 %d 条记录", s.tableName, totalRecords)
	return nil
}

// determineTimeRange 确定同步的时间范围
func (s *UniversalSyncer) determineTimeRange() (TimeRange, error) {
	timeField := s.tableConfig.TimeField

	var startTime, endTime time.Time

	log.Printf("⏱️  %s: 开始确定时间范围...", s.tableName)

	// 确定结束时间
	if s.config.TimeRange.End != "" {
		var err error
		endTime, err = time.Parse(time.RFC3339, s.config.TimeRange.End)
		if err != nil {
			return TimeRange{}, fmt.Errorf("invalid end time: %w", err)
		}
		log.Printf("⏱️  %s: 使用配置的结束时间: %s", s.tableName, endTime.Format(time.RFC3339))
	} else {
		// 查询源库的最新时间作为结束时间
		query := fmt.Sprintf("SELECT MAX(%s) FROM %s", timeField, s.tableName)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		var maxTimeSource sql.NullTime
		err := s.sourceDB.QueryRowContext(ctx, query).Scan(&maxTimeSource)
		if err != nil && err != sql.ErrNoRows {
			return TimeRange{}, fmt.Errorf("failed to query source max time: %w", err)
		}

		// 验证源库时间有效性
		minValidTime := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
		now := time.Now()
		maxFutureTime := now.Add(24 * time.Hour)

		if maxTimeSource.Valid && maxTimeSource.Time.After(minValidTime) && maxTimeSource.Time.Before(maxFutureTime) {
			// 使用源库最新时间 + 1秒，确保包含边界数据
			endTime = maxTimeSource.Time.Add(1 * time.Second)
			log.Printf("⏱️  %s: 使用源库最新时间作为结束时间: %s (含边界)", s.tableName, maxTimeSource.Time.Format(time.RFC3339))
		} else {
			// 源库无有效数据
			log.Printf("⏭️  %s: 源库无有效数据，跳过同步", s.tableName)
			return TimeRange{}, ErrSourceTableEmpty
		}
	}

	// 确定开始时间
	if s.config.TimeRange.AutoDetect {
		// 查询目标库的最大时间
		log.Printf("🔍 %s: 正在查询目标库最新时间（字段: %s）...", s.tableName, timeField)
		query := fmt.Sprintf("SELECT MAX(%s) FROM %s", timeField, s.tableName)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		var maxTime sql.NullTime
		err := s.targetDB.QueryRowContext(ctx, query).Scan(&maxTime)
		if err != nil && err != sql.ErrNoRows {
			log.Printf("❌ %s: 查询最大时间失败: %v", s.tableName, err)
			return TimeRange{}, fmt.Errorf("failed to query max time: %w", err)
		}

		// 验证时间有效性（ClickHouse 有效范围: 1900-01-01 到 2262-04-11）
		minValidTime := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
		isValidTime := maxTime.Valid && maxTime.Time.After(minValidTime) && maxTime.Time.Before(endTime.Add(24*time.Hour))

		if isValidTime {
			startTime = maxTime.Time.Add(1 * time.Millisecond) // 从最大时间后 1ms 开始
			log.Printf("🔍 %s: 检测到目标库最新时间 %s，从该时间后开始同步", s.tableName, maxTime.Time.Format(time.RFC3339))
		} else {
			// 目标库为空，检查源库是否有数据
			log.Printf("🔍 %s: 目标库为空，检查源库是否有数据...", s.tableName)
			sourceQuery := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s", timeField, timeField, s.tableName)

			var minTimeSource, maxTimeSource sql.NullTime
			err := s.sourceDB.QueryRowContext(ctx, sourceQuery).Scan(&minTimeSource, &maxTimeSource)
			if err != nil && err != sql.ErrNoRows {
				log.Printf("❌ %s: 查询源库时间范围失败: %v", s.tableName, err)
				return TimeRange{}, fmt.Errorf("failed to query source time range: %w", err)
			}

			// 如果源库也没有数据，跳过同步
			if !minTimeSource.Valid || !maxTimeSource.Valid {
				log.Printf("⏭️  %s: 源库无数据，跳过同步", s.tableName)
				return TimeRange{}, ErrSourceTableEmpty
			}

			// 源库有数据，使用 fallback 时间或源库最小时间
			fallbackTime := time.Now().AddDate(0, 0, -s.config.TimeRange.FallbackDays)
			if minTimeSource.Time.After(fallbackTime) {
				// 如果源库最早数据比 fallback 时间还新，就从源库最早数据开始
				startTime = minTimeSource.Time
				log.Printf("🔍 %s: 源库最早数据时间 %s，从该时间开始同步", s.tableName, minTimeSource.Time.Format(time.RFC3339))
			} else {
				// 否则使用 fallback 时间
				startTime = fallbackTime
				log.Printf("⚠️  %s: 目标库为空，从 %d 天前开始: %s", s.tableName, s.config.TimeRange.FallbackDays, startTime.Format(time.RFC3339))
			}
		}
	} else if s.config.TimeRange.Start != "" {
		var err error
		startTime, err = time.Parse(time.RFC3339, s.config.TimeRange.Start)
		if err != nil {
			return TimeRange{}, fmt.Errorf("invalid start time: %w", err)
		}
		log.Printf("⏱️  %s: 使用配置的开始时间: %s", s.tableName, startTime.Format(time.RFC3339))
	} else {
		startTime = time.Now().AddDate(0, 0, -30) // 默认 30 天
		log.Printf("⏱️  %s: 使用默认30天前作为开始时间: %s", s.tableName, startTime.Format(time.RFC3339))
	}

	log.Printf("✅ %s: 时间范围确定完成", s.tableName)
	return TimeRange{Start: startTime, End: endTime}, nil
}

// syncSegment 同步一个时间分段
func (s *UniversalSyncer) syncSegment(ctx context.Context, segment TimeSegment) (int, error) {
	timeField := s.tableConfig.TimeField
	batchSize := s.tableConfig.GetEffectiveBatchSize(s.config.Sync.BatchSize)

	log.Printf("⏰ %s: 同步时间段 %s ~ %s",
		s.tableName,
		segment.Start.Format("2006-01-02 15:04:05"),
		segment.End.Format("2006-01-02 15:04:05"))

	// 1. 查询目标库已存在的去重键
	existingKeys, err := s.deduplicator.FetchExistingKeys(
		s.targetDB, s.tableName, segment, s.tableSchema,
	)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch existing keys: %w", err)
	}
	log.Printf("🔑 %s: 目标库已有 %d 条记录（该时间段）", s.tableName, len(existingKeys))

	// 2. 构建查询 SQL（查询所有字段）
	columns := s.tableSchema.GetColumnNames()
	columnsStr := strings.Join(columns, ", ")

	query := fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s >= ? AND %s < ? ORDER BY %s",
		columnsStr, s.tableName, timeField, timeField, timeField,
	)

	// 3. 流式查询源库数据
	log.Printf("🔍 %s: 开始查询源库数据...", s.tableName)
	rows, err := s.sourceDB.QueryContext(ctx, query, segment.Start, segment.End)
	if err != nil {
		return 0, fmt.Errorf("failed to query source: %w", err)
	}
	defer rows.Close()

	// 4. 批量读取、去重、插入
	totalInserted := 0
	totalScanned := 0
	totalSkipped := 0
	batch := make([]map[string]interface{}, 0, batchSize)
	batchCount := 0

	for rows.Next() {
		totalScanned++

		// 扫描一行数据
		record, err := s.scanRow(rows, columns)
		if err != nil {
			return totalInserted, fmt.Errorf("failed to scan row: %w", err)
		}

		// 检查是否已存在（去重）
		key := s.deduplicator.BuildKey(record)
		if existingKeys[key] {
			totalSkipped++
			continue // 跳过已存在的记录
		}

		batch = append(batch, record)

		// 批量插入
		if len(batch) >= batchSize {
			batchCount++
			inserted, err := s.insertBatch(ctx, batch, columns)
			if err != nil {
				return totalInserted, fmt.Errorf("failed to insert batch: %w", err)
			}
			totalInserted += inserted
			batch = batch[:0] // 清空

			log.Printf("📦 %s: 批次 #%d 插入 %d 条 | 累计: 扫描 %d, 插入 %d, 跳过 %d",
				s.tableName, batchCount, inserted, totalScanned, totalInserted, totalSkipped)
		}
	}

	if err := rows.Err(); err != nil {
		return totalInserted, fmt.Errorf("error iterating rows: %w", err)
	}

	// 5. 插入剩余数据
	if len(batch) > 0 {
		batchCount++
		inserted, err := s.insertBatch(ctx, batch, columns)
		if err != nil {
			return totalInserted, fmt.Errorf("failed to insert final batch: %w", err)
		}
		totalInserted += inserted

		log.Printf("📦 %s: 批次 #%d 插入 %d 条 | 累计: 扫描 %d, 插入 %d, 跳过 %d",
			s.tableName, batchCount, inserted, totalScanned, totalInserted, totalSkipped)
	}

	log.Printf("✨ %s: 时间段完成 - 扫描 %d 条, 新增 %d 条, 跳过 %d 条",
		s.tableName, totalScanned, totalInserted, totalSkipped)

	return totalInserted, nil
}

// scanRow 扫描一行数据到 map
func (s *UniversalSyncer) scanRow(rows *sql.Rows, columns []string) (map[string]interface{}, error) {
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	if err := rows.Scan(valuePtrs...); err != nil {
		return nil, err
	}

	record := make(map[string]interface{})
	for i, col := range columns {
		record[col] = values[i]
	}

	return record, nil
}

// insertBatch 批量插入数据
func (s *UniversalSyncer) insertBatch(ctx context.Context, batch []map[string]interface{}, columns []string) (int, error) {
	if len(batch) == 0 {
		return 0, nil
	}

	// 使用 ClickHouse 原生批量插入
	columnsStr := strings.Join(columns, ", ")
	query := fmt.Sprintf("INSERT INTO %s (%s)", s.tableName, columnsStr)

	// 开始批量插入
	tx, err := s.targetDB.Begin()
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	// 逐行插入
	for _, record := range batch {
		values := make([]interface{}, len(columns))
		for i, col := range columns {
			val := record[col]

			// 特殊处理 Decimal 类型：将 string 转为 decimal.Decimal
			if typeStr, ok := s.colTypeMap[col]; ok && strings.Contains(typeStr, "Decimal") {
				if valStr, ok := val.(string); ok {
					if d, err := decimal.NewFromString(valStr); err == nil {
						values[i] = d
						continue
					}
				} else if valBytes, ok := val.([]byte); ok {
					// 某些驱动可能返回 []byte
					if d, err := decimal.NewFromString(string(valBytes)); err == nil {
						values[i] = d
						continue
					}
				}
			}

			// 特殊处理 DateTime 类型：验证时间范围
			if typeStr, ok := s.colTypeMap[col]; ok && strings.Contains(typeStr, "DateTime") {
				if t, ok := val.(time.Time); ok {
					// ClickHouse DateTime 范围: 1900-01-01 到 2262-04-11
					minTime := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
					maxTime := time.Date(2262, 4, 11, 23, 47, 16, 0, time.UTC)

					if t.Before(minTime) || t.After(maxTime) || t.IsZero() {
						// 超出范围或零值，使用默认时间（1970-01-01）
						values[i] = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
						continue
					}
				}
			}

			values[i] = val
		}

		_, err := stmt.ExecContext(ctx, values...)
		if err != nil {
			return 0, fmt.Errorf("failed to insert row: %w", err)
		}
	}

	// 提交事务
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return len(batch), nil
}

// fullSync 全量同步
func (s *UniversalSyncer) fullSync(ctx context.Context) error {
	log.Printf("🔄 %s: 开始全量同步", s.tableName)

	batchSize := s.tableConfig.GetEffectiveBatchSize(s.config.Sync.BatchSize)
	columns := s.tableSchema.GetColumnNames()
	columnsStr := strings.Join(columns, ", ")

	query := fmt.Sprintf("SELECT %s FROM %s", columnsStr, s.tableName)

	rows, err := s.sourceDB.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query source: %w", err)
	}
	defer rows.Close()

	totalInserted := 0
	batch := make([]map[string]interface{}, 0, batchSize)

	for rows.Next() {
		record, err := s.scanRow(rows, columns)
		if err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		batch = append(batch, record)

		if len(batch) >= batchSize {
			inserted, err := s.insertBatch(ctx, batch, columns)
			if err != nil {
				return fmt.Errorf("failed to insert batch: %w", err)
			}
			totalInserted += inserted
			batch = batch[:0]

			log.Printf("📦 %s: 已同步 %d 条记录", s.tableName, totalInserted)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %w", err)
	}

	if len(batch) > 0 {
		inserted, err := s.insertBatch(ctx, batch, columns)
		if err != nil {
			return fmt.Errorf("failed to insert final batch: %w", err)
		}
		totalInserted += inserted
	}

	log.Printf("🎉 %s: 全量同步完成，总计 %d 条记录", s.tableName, totalInserted)
	return nil
}

// segmentTimeRange 将时间范围分割为按天的分段
func (s *UniversalSyncer) segmentTimeRange(timeRange TimeRange) []TimeSegment {
	if !s.config.Sync.DailySegmentation {
		return []TimeSegment{{Start: timeRange.Start, End: timeRange.End}}
	}

	segments := []TimeSegment{}
	current := timeRange.Start

	for current.Before(timeRange.End) {
		dayEnd := time.Date(current.Year(), current.Month(), current.Day(), 23, 59, 59, 999999999, current.Location())
		dayEnd = dayEnd.Add(1 * time.Nanosecond) // 下一天的 00:00:00

		if dayEnd.After(timeRange.End) {
			dayEnd = timeRange.End
		}

		segments = append(segments, TimeSegment{Start: current, End: dayEnd})
		current = dayEnd
	}

	return segments
}
