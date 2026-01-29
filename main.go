package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	// 1. 解析命令行参数
	configPath := flag.String("config", "config.yaml", "配置文件路径")
	dryRun := flag.Bool("dry-run", false, "预览模式（不实际执行）")
	resume := flag.Bool("resume", false, "断点续传")
	tables := flag.String("tables", "", "指定同步的表（逗号分隔）")
	clearState := flag.Bool("clear-state", false, "清空状态文件")
	skipConfirm := flag.Bool("yes", false, "跳过确认提示")
	loopInterval := flag.Int("loop-interval", 10, "循环间隔（秒）")
	realtimeThreshold := flag.Int("realtime-threshold", 300, "实时模式阈值（秒），延迟超过此值先追平历史")
	flag.Parse()

	// 2. 加载配置
	log.Println("📖 加载配置文件...")
	config, err := LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("❌ 加载配置失败: %v", err)
	}

	// 覆盖配置参数
	if *dryRun {
		config.Monitoring.DryRun = true
	}
	if *resume {
		config.Sync.Resume = true
	}

	// 3. 过滤表
	if *tables != "" {
		selectedTables := strings.Split(*tables, ",")
		config.Tables = FilterTables(config.Tables, selectedTables)
		log.Printf("📋 已选择 %d 个表进行同步", len(config.Tables))
	}

	// 4. 验证配置
	if err := config.Validate(); err != nil {
		log.Fatalf("❌ 配置验证失败: %v", err)
	}

	// 验证时间范围
	if err := ValidateTimeRange(&config.TimeRange); err != nil {
		log.Fatalf("❌ 时间范围配置无效: %v", err)
	}

	// 5. 预览模式
	if config.Monitoring.DryRun {
		PrintSyncPlan(config)
		log.Println("\n✅ 预览模式完成，未执行实际同步")
		return
	}

	// 6. 连接数据库
	log.Println("🔌 连接源数据库...")
	sourceDB, err := ConnectClickHouse(config.Source, config.Sync)
	if err != nil {
		log.Fatalf("❌ 连接源数据库失败: %v", err)
	}
	defer sourceDB.Close()

	log.Println("🔌 连接目标数据库...")
	targetDB, err := ConnectClickHouse(config.Target, config.Sync)
	if err != nil {
		log.Fatalf("❌ 连接目标数据库失败: %v", err)
	}
	defer targetDB.Close()

	log.Println("✅ 数据库连接成功")

	// 获取数据库版本信息
	sourceVersion, _ := GetDatabaseVersion(sourceDB)
	targetVersion, _ := GetDatabaseVersion(targetDB)
	log.Printf("📌 源数据库版本: %s", sourceVersion)
	log.Printf("📌 目标数据库版本: %s", targetVersion)

	// 7. 清空状态（如果指定）
	if *clearState {
		stateManager := NewStateManager(config.Sync.StateFile)
		if err := stateManager.ClearState(); err != nil {
			log.Fatalf("❌ 清空状态失败: %v", err)
		}
		log.Println("🗑️  状态文件已清空")
		return
	}

	// 8. 打印同步计划
	PrintSyncPlan(config)

	// 9. 确认执行
	if !*skipConfirm {
		if !AskConfirmation("即将开始同步，是否继续?") {
			log.Println("❌ 取消同步")
			return
		}
	}

	// 10. 表结构同步
	if config.Sync.SchemaSync.Enabled {
		log.Println("\n🔧 开始同步表结构...")
		schemaSyncer := NewSchemaSyncer(sourceDB, targetDB, &config.Sync.SchemaSync)

		for _, tableConfig := range config.Tables {
			if !tableConfig.Enabled {
				continue
			}

			err := schemaSyncer.SyncTableSchema(tableConfig.Name)
			if err != nil {
				log.Fatalf("❌ 表结构同步失败 (%s): %v", tableConfig.Name, err)
			}
		}

		log.Println("✅ 所有表结构同步完成")
	}

	// 11. 执行数据同步（智能循环模式）
	log.Println("🚀 开始数据同步...")
	ctx := context.Background()
	coordinator := NewSyncCoordinator(sourceDB, targetDB, config)

	// 设置信号处理（用于优雅退出）
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// 智能循环模式
	log.Printf("🔄 智能循环模式已启用")
	log.Printf("⚙️  实时阈值: %d 秒（延迟超过此值会先追平历史数据）", *realtimeThreshold)
	log.Printf("⚙️  循环间隔: %d 秒", *loopInterval)
	log.Printf("💡 按 Ctrl+C 退出\n")

	realtimeThresholdDuration := time.Duration(*realtimeThreshold) * time.Second
	cycleCount := 0

	for {
		cycleCount++
		log.Printf("\n========================================")
		log.Printf("🔄 开始第 %d 次同步循环", cycleCount)
		log.Printf("========================================\n")

		startTime := time.Now()
		err := coordinator.SyncAllTablesWithSmartMode(ctx, realtimeThresholdDuration)
		duration := time.Since(startTime)

		if err != nil {
			log.Printf("❌ 第 %d 次同步循环失败: %v", cycleCount, err)
		} else {
			log.Printf("✅ 第 %d 次同步循环完成，耗时: %s", cycleCount, FormatDuration(duration))
		}

		// 等待指定间隔或接收退出信号
		log.Printf("\n💤 等待 %d 秒后开始下一次同步...", *loopInterval)
		select {
		case <-sigChan:
			log.Println("\n\n⚠️  收到终止信号，正在优雅退出...")
			log.Printf("📊 总共完成 %d 次同步循环", cycleCount)
			PrintFinalReport(config, time.Duration(0), coordinator.GetState())
			log.Println("\n✅ 同步任务已安全退出！")
			return
		case <-time.After(time.Duration(*loopInterval) * time.Second):
			// 继续下一次循环
		}
	}
}

func init() {
	// 设置日志格式
	log.SetFlags(log.Ldate | log.Ltime)
	log.SetOutput(os.Stdout)
}
