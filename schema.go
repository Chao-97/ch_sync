package main

import (
	"database/sql"
	"fmt"
	"strings"
)

// TableSchema 表结构信息
type TableSchema struct {
	TableName   string
	Columns     []ColumnInfo
	OrderBy     []string // ORDER BY 字段
	PartitionBy string   // PARTITION BY 表达式
	Engine      string   // 表引擎
}

// ColumnInfo 字段信息
type ColumnInfo struct {
	Name         string
	Type         string
	DefaultValue string
	IsNullable   bool
}

// DetectTableSchema 自动检测表结构
func DetectTableSchema(db *sql.DB, tableName string) (*TableSchema, error) {
	schema := &TableSchema{TableName: tableName}

	// 1. 从 system.columns 获取字段信息
	query := `
		SELECT name, type, default_expression
		FROM system.columns
		WHERE database = currentDatabase() AND table = ?
		ORDER BY position
	`
	rows, err := db.Query(query, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var col ColumnInfo
		var defaultExpr sql.NullString
		err := rows.Scan(&col.Name, &col.Type, &defaultExpr)
		if err != nil {
			return nil, fmt.Errorf("failed to scan column: %w", err)
		}
		if defaultExpr.Valid {
			col.DefaultValue = defaultExpr.String
		}
		// ClickHouse 没有明确的 nullable 标记，通过类型判断
		col.IsNullable = strings.Contains(col.Type, "Nullable")
		schema.Columns = append(schema.Columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating columns: %w", err)
	}

	if len(schema.Columns) == 0 {
		return nil, fmt.Errorf("table %s not found or has no columns", tableName)
	}

	// 2. 从 system.tables 获取表元信息
	query = `
		SELECT engine, sorting_key, partition_key
		FROM system.tables
		WHERE database = currentDatabase() AND name = ?
	`
	var sortingKey, partitionKey sql.NullString
	err = db.QueryRow(query, tableName).Scan(
		&schema.Engine, &sortingKey, &partitionKey,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query table info: %w", err)
	}

	// 3. 解析 ORDER BY 字段
	if sortingKey.Valid && sortingKey.String != "" {
		schema.OrderBy = parseOrderByKeys(sortingKey.String)
	}
	if partitionKey.Valid {
		schema.PartitionBy = partitionKey.String
	}

	return schema, nil
}

// parseOrderByKeys 解析 ORDER BY 字符串
// 例如: "end_at, user_id, support_model_id" → ["end_at", "user_id", "support_model_id"]
func parseOrderByKeys(sortingKey string) []string {
	keys := strings.Split(sortingKey, ",")
	result := make([]string, 0, len(keys))
	for _, key := range keys {
		key = strings.TrimSpace(key)
		if key != "" {
			result = append(result, key)
		}
	}
	return result
}

// GetColumnNames 获取所有字段名
func (ts *TableSchema) GetColumnNames() []string {
	names := make([]string, len(ts.Columns))
	for i, col := range ts.Columns {
		names[i] = col.Name
	}
	return names
}

// HasColumn 检查表是否包含指定字段
func (ts *TableSchema) HasColumn(columnName string) bool {
	for _, col := range ts.Columns {
		if col.Name == columnName {
			return true
		}
	}
	return false
}

// GetColumn 获取指定字段信息
func (ts *TableSchema) GetColumn(columnName string) *ColumnInfo {
	for i := range ts.Columns {
		if ts.Columns[i].Name == columnName {
			return &ts.Columns[i]
		}
	}
	return nil
}

// String 返回表结构的字符串表示
func (ts *TableSchema) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Table: %s\n", ts.TableName))
	sb.WriteString(fmt.Sprintf("Engine: %s\n", ts.Engine))
	if len(ts.OrderBy) > 0 {
		sb.WriteString(fmt.Sprintf("OrderBy: %v\n", ts.OrderBy))
	}
	if ts.PartitionBy != "" {
		sb.WriteString(fmt.Sprintf("PartitionBy: %s\n", ts.PartitionBy))
	}
	sb.WriteString("Columns:\n")
	for i, col := range ts.Columns {
		sb.WriteString(fmt.Sprintf("  %d. %s %s", i+1, col.Name, col.Type))
		if col.DefaultValue != "" {
			sb.WriteString(fmt.Sprintf(" DEFAULT %s", col.DefaultValue))
		}
		sb.WriteString("\n")
	}
	return sb.String()
}
