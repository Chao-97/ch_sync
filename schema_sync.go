package main

import (
	"database/sql"
	"fmt"
	"log"
)

// SchemaSyncer 表结构同步器
type SchemaSyncer struct {
	sourceDB *sql.DB
	targetDB *sql.DB
	config   *SchemaSyncConfig
}

// NewSchemaSyncer 创建表结构同步器
func NewSchemaSyncer(sourceDB, targetDB *sql.DB, config *SchemaSyncConfig) *SchemaSyncer {
	return &SchemaSyncer{
		sourceDB: sourceDB,
		targetDB: targetDB,
		config:   config,
	}
}

// SyncTableSchema 同步表结构
func (ss *SchemaSyncer) SyncTableSchema(tableName string) error {
	log.Printf("🔧 开始同步表结构: %s", tableName)

	// 1. 获取源表结构
	sourceSchema, err := DetectTableSchema(ss.sourceDB, tableName)
	if err != nil {
		return fmt.Errorf("failed to detect source schema: %w", err)
	}

	// 2. 检查目标表是否存在
	exists, err := ss.tableExists(tableName)
	if err != nil {
		return fmt.Errorf("failed to check table existence: %w", err)
	}

	if !exists {
		// 3. 目标表不存在，创建新表
		if !ss.config.CreateIfNotExists {
			return fmt.Errorf("table %s does not exist in target database", tableName)
		}
		return ss.createTable(tableName, sourceSchema)
	} else {
		// 4. 目标表存在，对比并同步新增字段
		if ss.config.SkipColumnCheck {
			log.Printf("⏭️  跳过字段检查: %s", tableName)
			return nil
		}
		return ss.syncColumns(tableName, sourceSchema)
	}
}

// tableExists 检查表是否存在
func (ss *SchemaSyncer) tableExists(tableName string) (bool, error) {
	query := `
		SELECT count(*)
		FROM system.tables
		WHERE database = currentDatabase() AND name = ?
	`
	var count int
	err := ss.targetDB.QueryRow(query, tableName).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// createTable 在目标库创建表
func (ss *SchemaSyncer) createTable(tableName string, schema *TableSchema) error {
	log.Printf("📝 创建表 %s...", tableName)

	// 从源库获取完整的 CREATE TABLE 语句
	createSQL, err := ss.getCreateTableSQL(tableName)
	if err != nil {
		return fmt.Errorf("failed to get CREATE TABLE SQL: %w", err)
	}

	// 在目标库执行创建语句
	_, err = ss.targetDB.Exec(createSQL)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	log.Printf("✅ 表 %s 创建成功", tableName)
	return nil
}

// getCreateTableSQL 获取源表的创建语句
func (ss *SchemaSyncer) getCreateTableSQL(tableName string) (string, error) {
	query := fmt.Sprintf("SHOW CREATE TABLE %s", tableName)
	var createSQL string
	err := ss.sourceDB.QueryRow(query).Scan(&createSQL)
	return createSQL, err
}

// syncColumns 同步新增字段
func (ss *SchemaSyncer) syncColumns(tableName string, sourceSchema *TableSchema) error {
	if !ss.config.SyncNewColumns {
		log.Printf("⏭️  跳过字段同步: %s", tableName)
		return nil
	}

	// 1. 获取目标表结构
	targetSchema, err := DetectTableSchema(ss.targetDB, tableName)
	if err != nil {
		return fmt.Errorf("failed to detect target schema: %w", err)
	}

	// 2. 对比字段差异
	newColumns := ss.findNewColumns(sourceSchema, targetSchema)

	if len(newColumns) == 0 {
		log.Printf("✅ 表 %s 结构一致，无需更新", tableName)
		return nil
	}

	log.Printf("🔍 表 %s 发现 %d 个新字段: %v",
		tableName, len(newColumns), getColumnNames(newColumns))

	// 3. 添加新字段
	for _, col := range newColumns {
		err := ss.addColumn(tableName, col)
		if err != nil {
			return fmt.Errorf("failed to add column %s: %w", col.Name, err)
		}
		log.Printf("✅ 添加字段 %s.%s (%s)", tableName, col.Name, col.Type)
	}

	return nil
}

// findNewColumns 找出源表中存在但目标表中不存在的字段
func (ss *SchemaSyncer) findNewColumns(sourceSchema, targetSchema *TableSchema) []ColumnInfo {
	targetCols := make(map[string]bool)
	for _, col := range targetSchema.Columns {
		targetCols[col.Name] = true
	}

	var newColumns []ColumnInfo
	for _, col := range sourceSchema.Columns {
		if !targetCols[col.Name] {
			newColumns = append(newColumns, col)
		}
	}

	return newColumns
}

// addColumn 添加新字段
func (ss *SchemaSyncer) addColumn(tableName string, col ColumnInfo) error {
	// 构建 ALTER TABLE 语句
	alterSQL := fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s",
		tableName, col.Name, col.Type)

	// 添加默认值（如果有）
	if col.DefaultValue != "" {
		alterSQL += fmt.Sprintf(" DEFAULT %s", col.DefaultValue)
	}

	_, err := ss.targetDB.Exec(alterSQL)
	return err
}

// getColumnNames 获取字段名列表
func getColumnNames(columns []ColumnInfo) []string {
	names := make([]string, len(columns))
	for i, col := range columns {
		names[i] = col.Name
	}
	return names
}
