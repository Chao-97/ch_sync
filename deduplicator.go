package main

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// Deduplicator 去重器
type Deduplicator struct {
	dedupeKeys []string // 去重字段列表
	timeField  string   // 时间字段（用于查询范围）
}

// NewDeduplicator 创建去重器
func NewDeduplicator(dedupeKeys []string, timeField string) *Deduplicator {
	return &Deduplicator{
		dedupeKeys: dedupeKeys,
		timeField:  timeField,
	}
}

// FetchExistingKeys 查询目标库已存在的去重键
func (d *Deduplicator) FetchExistingKeys(
	db *sql.DB,
	tableName string,
	segment TimeSegment,
	schema *TableSchema,
) (map[string]bool, error) {
	// 验证所有去重字段是否存在于目标表中
	missingKeys := []string{}
	for _, key := range d.dedupeKeys {
		if !schema.HasColumn(key) {
			missingKeys = append(missingKeys, key)
		}
	}

	if len(missingKeys) > 0 {
		return nil, fmt.Errorf("deduplication keys not found in table %s: %v. Available columns: %v",
			tableName, missingKeys, schema.GetColumnNames())
	}

	// 构建查询 SQL
	keysStr := strings.Join(d.dedupeKeys, ", ")
	query := fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s >= ? AND %s < ?",
		keysStr, tableName, d.timeField, d.timeField,
	)

	rows, err := db.Query(query, segment.Start, segment.End)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	existingKeys := make(map[string]bool)

	for rows.Next() {
		// 扫描去重字段
		values := make([]interface{}, len(d.dedupeKeys))
		valuePtrs := make([]interface{}, len(d.dedupeKeys))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		// 构建复合键
		key := d.buildKeyFromValues(values)
		existingKeys[key] = true
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return existingKeys, nil
}

// BuildKey 从记录构建去重键
func (d *Deduplicator) BuildKey(record map[string]interface{}) string {
	values := make([]interface{}, len(d.dedupeKeys))
	for i, key := range d.dedupeKeys {
		values[i] = record[key]
	}
	return d.buildKeyFromValues(values)
}

// buildKeyFromValues 从值列表构建复合键
func (d *Deduplicator) buildKeyFromValues(values []interface{}) string {
	parts := make([]string, len(values))
	for i, val := range values {
		parts[i] = d.formatValue(val)
	}
	return strings.Join(parts, "|") // 用 | 分隔多个字段
}

// formatValue 格式化值为字符串
func (d *Deduplicator) formatValue(val interface{}) string {
	if val == nil {
		return "<NULL>"
	}

	switch v := val.(type) {
	case time.Time:
		return v.Format(time.RFC3339Nano)
	case string:
		return v
	case []byte:
		return string(v)
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", v)
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", v)
	case float32, float64:
		return fmt.Sprintf("%f", v)
	case bool:
		return fmt.Sprintf("%t", v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// GetDedupeKeys 获取去重字段列表
func (d *Deduplicator) GetDedupeKeys() []string {
	return d.dedupeKeys
}

// GetTimeField 获取时间字段
func (d *Deduplicator) GetTimeField() string {
	return d.timeField
}
