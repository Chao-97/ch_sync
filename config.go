package main

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config 主配置结构
type Config struct {
	Source     DatabaseConfig   `yaml:"source"`
	Target     DatabaseConfig   `yaml:"target"`
	Sync       SyncConfig       `yaml:"sync"`
	Tables     []TableConfig    `yaml:"tables"`
	TimeRange  TimeRangeConfig  `yaml:"time_range"`
	Monitoring MonitoringConfig `yaml:"monitoring"`
}

// DatabaseConfig 数据库连接配置
type DatabaseConfig struct {
	Addr     []string `yaml:"addr"`
	Database string   `yaml:"database"`
	Username string   `yaml:"username"`
	Password string   `yaml:"password"`
}

// SyncConfig 同步配置
type SyncConfig struct {
	Mode              string           `yaml:"mode"`
	BatchSize         int              `yaml:"batch_size"`
	MaxConcurrency    int              `yaml:"max_concurrency"`
	DailySegmentation bool             `yaml:"daily_segmentation"`
	EnableCompression bool             `yaml:"enable_compression"`
	DialTimeout       int              `yaml:"dial_timeout"`
	QueryTimeout      int              `yaml:"query_timeout"`
	SchemaSync        SchemaSyncConfig `yaml:"schema_sync"`
	StateFile         string           `yaml:"state_file"`
	Resume            bool             `yaml:"resume"`
	SkipValidation    bool             `yaml:"skip_validation"`
	ValidationRatio   float64          `yaml:"validation_ratio"`
}

// SchemaSyncConfig 表结构同步配置
type SchemaSyncConfig struct {
	Enabled           bool `yaml:"enabled"`
	CreateIfNotExists bool `yaml:"create_if_not_exists"`
	SyncNewColumns    bool `yaml:"sync_new_columns"`
	SkipColumnCheck   bool `yaml:"skip_column_check"`
}

// TableConfig 表同步配置
type TableConfig struct {
	Name       string   `yaml:"name"`
	Mode       string   `yaml:"mode"`
	TimeField  string   `yaml:"time_field"`
	DedupeKeys []string `yaml:"dedupe_keys"`
	BatchSize  int      `yaml:"batch_size"`
	Enabled    bool     `yaml:"enabled"`
}

// TimeRangeConfig 时间范围配置
type TimeRangeConfig struct {
	Start        string `yaml:"start"`
	End          string `yaml:"end"`
	AutoDetect   bool   `yaml:"auto_detect"`
	FallbackDays int    `yaml:"fallback_days"`
}

// MonitoringConfig 监控配置
type MonitoringConfig struct {
	ProgressBars   bool `yaml:"progress_bars"`
	VerboseLogging bool `yaml:"verbose_logging"`
	DryRun         bool `yaml:"dry_run"`
}

// LoadConfig 从 YAML 文件加载配置
func LoadConfig(configPath string) (*Config, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config Config
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	// 设置默认值
	if config.Sync.BatchSize == 0 {
		config.Sync.BatchSize = 2000
	}
	if config.Sync.MaxConcurrency == 0 {
		config.Sync.MaxConcurrency = 3
	}
	if config.Sync.DialTimeout == 0 {
		config.Sync.DialTimeout = 10
	}
	if config.Sync.QueryTimeout == 0 {
		config.Sync.QueryTimeout = 300
	}
	if config.Sync.ValidationRatio == 0 {
		config.Sync.ValidationRatio = 0.95
	}
	if config.TimeRange.FallbackDays == 0 {
		config.TimeRange.FallbackDays = 30
	}
	if config.Sync.StateFile == "" {
		config.Sync.StateFile = "/tmp/clickhouse_sync_state.json"
	}

	return &config, nil
}

// GetEffectiveMode 获取表的有效同步模式（表配置优先于全局配置）
func (tc *TableConfig) GetEffectiveMode(globalMode string) string {
	if tc.Mode != "" {
		return tc.Mode
	}
	return globalMode
}

// GetEffectiveBatchSize 获取表的有效批量大小
func (tc *TableConfig) GetEffectiveBatchSize(globalBatchSize int) int {
	if tc.BatchSize > 0 {
		return tc.BatchSize
	}
	return globalBatchSize
}

// Validate 验证配置的合法性
func (c *Config) Validate() error {
	// 验证数据库配置
	if len(c.Source.Addr) == 0 {
		return fmt.Errorf("source database address is required")
	}
	if c.Source.Database == "" {
		return fmt.Errorf("source database name is required")
	}
	if len(c.Target.Addr) == 0 {
		return fmt.Errorf("target database address is required")
	}
	if c.Target.Database == "" {
		return fmt.Errorf("target database name is required")
	}

	// 验证同步模式
	if c.Sync.Mode != "full" && c.Sync.Mode != "incremental" {
		return fmt.Errorf("sync mode must be 'full' or 'incremental', got: %s", c.Sync.Mode)
	}

	// 验证表配置
	if len(c.Tables) == 0 {
		return fmt.Errorf("no tables configured for sync")
	}

	enabledCount := 0
	for i, table := range c.Tables {
		if !table.Enabled {
			continue
		}
		enabledCount++

		if table.Name == "" {
			return fmt.Errorf("table[%d]: name is required", i)
		}
		if table.TimeField == "" {
			return fmt.Errorf("table[%d] (%s): time_field is required", i, table.Name)
		}
		if len(table.DedupeKeys) == 0 {
			return fmt.Errorf("table[%d] (%s): dedupe_keys is required", i, table.Name)
		}

		// 验证表的同步模式
		mode := table.GetEffectiveMode(c.Sync.Mode)
		if mode != "full" && mode != "incremental" {
			return fmt.Errorf("table[%d] (%s): invalid mode: %s", i, table.Name, mode)
		}
	}

	if enabledCount == 0 {
		return fmt.Errorf("no enabled tables found")
	}

	return nil
}
