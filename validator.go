package main

import (
	"database/sql"
	"fmt"
	"log"
)

// Validator 数据验证器
type Validator struct {
	sourceDB *sql.DB
	targetDB *sql.DB
	config   *Config
}

// NewValidator 创建验证器
func NewValidator(sourceDB, targetDB *sql.DB, config *Config) *Validator {
	return &Validator{
		sourceDB: sourceDB,
		targetDB: targetDB,
		config:   config,
	}
}

// ValidateTable 验证表的数据完整性
func (v *Validator) ValidateTable(tableName string, timeField string, timeRange TimeRange) error {
	if v.config.Sync.SkipValidation {
		return nil
	}

	log.Printf("🔍 验证 %s 的数据完整性...", tableName)

	// 查询源库记录数
	sourceCount, err := v.countRecords(v.sourceDB, tableName, timeField, timeRange)
	if err != nil {
		return fmt.Errorf("failed to count source records: %w", err)
	}

	// 查询目标库记录数
	targetCount, err := v.countRecords(v.targetDB, tableName, timeField, timeRange)
	if err != nil {
		return fmt.Errorf("failed to count target records: %w", err)
	}

	// 验证阈值
	threshold := float64(sourceCount) * v.config.Sync.ValidationRatio

	log.Printf("📊 %s: 源库 %d 条，目标库 %d 条", tableName, sourceCount, targetCount)

	if float64(targetCount) < threshold {
		return fmt.Errorf(
			"validation failed: expected ~%d (%.1f%%), got %d",
			int(threshold), v.config.Sync.ValidationRatio*100, targetCount,
		)
	}

	log.Printf("✅ %s: 验证通过 (%.2f%%)",
		tableName, float64(targetCount)/float64(sourceCount)*100)
	return nil
}

// countRecords 统计记录数
func (v *Validator) countRecords(db *sql.DB, tableName, timeField string, timeRange TimeRange) (int, error) {
	query := fmt.Sprintf(
		"SELECT COUNT(*) FROM %s WHERE %s >= ? AND %s < ?",
		tableName, timeField, timeField,
	)

	var count int
	err := db.QueryRow(query, timeRange.Start, timeRange.End).Scan(&count)
	return count, err
}

// ValidateAllTables 验证所有启用的表
func (v *Validator) ValidateAllTables(timeRange TimeRange) map[string]error {
	results := make(map[string]error)

	for _, tableConfig := range v.config.Tables {
		if !tableConfig.Enabled {
			continue
		}

		err := v.ValidateTable(tableConfig.Name, tableConfig.TimeField, timeRange)
		results[tableConfig.Name] = err
	}

	return results
}

// PrintValidationSummary 打印验证摘要
func (v *Validator) PrintValidationSummary(results map[string]error) {
	fmt.Println("\n========================================")
	fmt.Println("数据验证报告")
	fmt.Println("========================================")

	passCount := 0
	failCount := 0

	for tableName, err := range results {
		if err == nil {
			fmt.Printf("✅ %s: 验证通过\n", tableName)
			passCount++
		} else {
			fmt.Printf("❌ %s: %v\n", tableName, err)
			failCount++
		}
	}

	fmt.Println("========================================")
	fmt.Printf("通过: %d, 失败: %d, 总计: %d\n", passCount, failCount, len(results))
	fmt.Println("========================================")
}
