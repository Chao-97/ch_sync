package main

import (
	"fmt"
	"strings"
	"time"
)

// FormatDuration 格式化时间段
func FormatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%.1f秒", d.Seconds())
	}
	if d < time.Hour {
		return fmt.Sprintf("%.1f分钟", d.Minutes())
	}
	return fmt.Sprintf("%.1f小时", d.Hours())
}

// FormatNumber 格式化数字（添加千分位分隔符）
func FormatNumber(n int) string {
	str := fmt.Sprintf("%d", n)
	if len(str) <= 3 {
		return str
	}

	// 从后往前每3位添加逗号
	var result strings.Builder
	for i, c := range str {
		if i > 0 && (len(str)-i)%3 == 0 {
			result.WriteRune(',')
		}
		result.WriteRune(c)
	}
	return result.String()
}

// CountEnabledTables 统计启用的表数量
func CountEnabledTables(tables []TableConfig) int {
	count := 0
	for _, table := range tables {
		if table.Enabled {
			count++
		}
	}
	return count
}

// FilterTables 根据表名列表过滤表配置
func FilterTables(tables []TableConfig, selectedNames []string) []TableConfig {
	if len(selectedNames) == 0 {
		return tables
	}

	// 创建表名集合
	nameSet := make(map[string]bool)
	for _, name := range selectedNames {
		nameSet[strings.TrimSpace(name)] = true
	}

	// 过滤表
	filtered := []TableConfig{}
	for _, table := range tables {
		if nameSet[table.Name] {
			filtered = append(filtered, table)
		}
	}

	return filtered
}

// PrintSyncPlan 打印同步计划
func PrintSyncPlan(config *Config) {
	fmt.Println("\n========================================")
	fmt.Println("同步计划预览")
	fmt.Println("========================================")
	fmt.Printf("源数据库: %s @ %v\n", config.Source.Database, config.Source.Addr)
	fmt.Printf("目标数据库: %s @ %v\n", config.Target.Database, config.Target.Addr)
	fmt.Printf("同步模式: %s\n", config.Sync.Mode)
	fmt.Printf("并发数: %d\n", config.Sync.MaxConcurrency)
	fmt.Printf("批量大小: %d\n", config.Sync.BatchSize)
	fmt.Printf("按天分段: %v\n", config.Sync.DailySegmentation)
	fmt.Printf("启用压缩: %v\n", config.Sync.EnableCompression)

	if config.Sync.SchemaSync.Enabled {
		fmt.Println("\n表结构同步:")
		fmt.Printf("  自动创建表: %v\n", config.Sync.SchemaSync.CreateIfNotExists)
		fmt.Printf("  同步新增字段: %v\n", config.Sync.SchemaSync.SyncNewColumns)
	}

	fmt.Println("\n待同步表:")
	count := 1
	for _, table := range config.Tables {
		if table.Enabled {
			mode := table.GetEffectiveMode(config.Sync.Mode)
			batchSize := table.GetEffectiveBatchSize(config.Sync.BatchSize)
			fmt.Printf("  %d. %s\n", count, table.Name)
			fmt.Printf("     - 模式: %s\n", mode)
			fmt.Printf("     - 时间字段: %s\n", table.TimeField)
			fmt.Printf("     - 去重键: %v\n", table.DedupeKeys)
			fmt.Printf("     - 批量大小: %d\n", batchSize)
			count++
		}
	}
	fmt.Println("========================================")
}

// PrintFinalReport 打印最终报告
func PrintFinalReport(config *Config, duration time.Duration, state *StateManager) {
	fmt.Println("\n========================================")
	fmt.Println("同步完成报告")
	fmt.Println("========================================")
	fmt.Printf("总耗时: %s\n", FormatDuration(duration))
	fmt.Printf("同步表数: %d\n", CountEnabledTables(config.Tables))
	fmt.Printf("总记录数: %s\n", FormatNumber(state.GetTotalRecordsSynced()))

	// 打印每个表的统计
	fmt.Println("\n各表详情:")
	for i, table := range config.Tables {
		if !table.Enabled {
			continue
		}
		tableState := state.GetTableState(table.Name)
		if tableState != nil {
			fmt.Printf("  %d. %s: %s 条记录\n",
				i+1, table.Name, FormatNumber(tableState.RecordsSynced))
		}
	}

	fmt.Println("========================================")
}

// ValidateTimeRange 验证时间范围配置
func ValidateTimeRange(config *TimeRangeConfig) error {
	if config.Start != "" {
		_, err := time.Parse(time.RFC3339, config.Start)
		if err != nil {
			return fmt.Errorf("invalid start time format (expected RFC3339): %w", err)
		}
	}

	if config.End != "" {
		_, err := time.Parse(time.RFC3339, config.End)
		if err != nil {
			return fmt.Errorf("invalid end time format (expected RFC3339): %w", err)
		}
	}

	if config.Start != "" && config.End != "" {
		start, _ := time.Parse(time.RFC3339, config.Start)
		end, _ := time.Parse(time.RFC3339, config.End)
		if start.After(end) {
			return fmt.Errorf("start time must be before end time")
		}
	}

	return nil
}

// AskConfirmation 询问用户确认
func AskConfirmation(message string) bool {
	fmt.Printf("\n⚠️  %s (yes/no): ", message)
	var response string
	fmt.Scanln(&response)
	return strings.ToLower(strings.TrimSpace(response)) == "yes"
}
