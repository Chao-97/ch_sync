package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// ConnectClickHouse 连接到 ClickHouse 数据库
func ConnectClickHouse(dbConfig DatabaseConfig, syncConfig SyncConfig) (*sql.DB, error) {
	options := &clickhouse.Options{
		Addr: dbConfig.Addr,
		Auth: clickhouse.Auth{
			Database: dbConfig.Database,
			Username: dbConfig.Username,
			Password: dbConfig.Password,
		},
		DialTimeout: time.Duration(syncConfig.DialTimeout) * time.Second,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": syncConfig.QueryTimeout,
		},
	}

	// 如果禁用压缩，则不设置压缩
	if !syncConfig.EnableCompression {
		options.Compression = &clickhouse.Compression{
			Method: clickhouse.CompressionNone,
		}
	}

	conn := clickhouse.OpenDB(options)

	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(syncConfig.DialTimeout)*time.Second)
	defer cancel()

	if err := conn.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// 设置连接池参数
	conn.SetMaxOpenConns(10)
	conn.SetMaxIdleConns(5)
	conn.SetConnMaxLifetime(time.Hour)

	return conn, nil
}

// TestConnection 测试数据库连接
func TestConnection(db *sql.DB) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var result int
	err := db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
	if err != nil {
		return fmt.Errorf("connection test failed: %w", err)
	}

	if result != 1 {
		return fmt.Errorf("unexpected result from connection test: %d", result)
	}

	return nil
}

// GetDatabaseVersion 获取数据库版本
func GetDatabaseVersion(db *sql.DB) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var version string
	err := db.QueryRowContext(ctx, "SELECT version()").Scan(&version)
	if err != nil {
		return "", fmt.Errorf("failed to get database version: %w", err)
	}

	return version, nil
}
