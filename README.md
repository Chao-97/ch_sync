# ClickHouse 通用增量同步工具

一个**通用的** ClickHouse 数据库同步工具，支持全量和增量同步。通过配置文件指定表名、时间字段、去重键，工具自动检测表结构并执行同步，无需为每个表编写专门代码。

## 特性

- ✅ **通用性**: 一套代码适用于任何 ClickHouse 表
- ✅ **配置驱动**: 业务逻辑通过配置文件描述
- ✅ **自动检测**: 表结构、字段类型自动从数据库读取
- ✅ **表结构同步**: 自动创建表和同步新增字段
- ✅ **灵活去重**: 支持单字段和组合字段去重
- ✅ **断点续传**: 支持中断后继续同步
- ✅ **并行同步**: 支持多表并行同步
- ✅ **按天分段**: 自动将时间范围分割为按天的分段
- ✅ **自动时间检测**: 自动从目标库检测最新时间
- ✅ **智能同步**: 先追平历史数据，再进入实时增量监控模式
- ✅ **数据库切换保护**: 双向时间窗口检查，防止切换时数据丢失

## 安装

### 1. 安装依赖

```bash
go mod download
```

### 2. 构建

```bash
go build -o ch_sync
```

## 配置文件

配置文件 `config.yaml` 包含以下部分:

### 数据库连接配置

```yaml
source:
  addr: ["source-host:9000"]
  database: "your_database"
  username: "your_username"
  password: "your_password"

target:
  addr: ["target-host:9000"]
  database: "your_database"
  username: "your_username"
  password: "your_password"
```

### 同步配置

```yaml
sync:
  mode: "incremental"              # 同步模式: "full" 或 "incremental"
  batch_size: 2000                 # 批量插入大小
  max_concurrency: 3               # 最多同时同步的表数量
  daily_segmentation: true         # 是否按天分段
  enable_compression: true         # 是否启用 LZ4 压缩
  dial_timeout: 10                 # 连接超时（秒）
  query_timeout: 300               # 查询超时（秒）

  # 表结构同步配置
  schema_sync:
    enabled: true                  # 是否启用表结构同步
    create_if_not_exists: true     # 表不存在时是否自动创建
    sync_new_columns: true         # 是否同步新增字段
    skip_column_check: false       # 是否跳过字段检查（快速模式）
```

### 表配置

```yaml
tables:
  - name: "your_table_name"
    mode: "incremental"
    time_field: "created_at"           # 时间字段
    dedupe_keys: ["id"]                # 去重字段
    batch_size: 2000
    enabled: true
```

## 使用方法

### 基本用法

```bash
# 使用默认配置文件
./ch_sync

# 指定配置文件
./ch_sync --config config.yaml
```

### 预览模式

不实际执行同步，仅查看同步计划:

```bash
./ch_sync --config config.yaml --dry-run
```

### 指定同步的表

只同步指定的表（逗号分隔）:

```bash
./ch_sync --config config.yaml --tables "table1,table2"
```

### 跳过确认提示

自动确认，不等待用户输入:

```bash
./ch_sync --config config.yaml --yes
```

### 断点续传

从上次中断的地方继续同步:

```bash
./ch_sync --config config.yaml --resume
```

### 清空状态文件

清空之前的同步状态:

```bash
./ch_sync --config config.yaml --clear-state
```

### 智能循环同步（默认模式）

程序默认运行在智能循环模式下，会自动：

1. **首次运行或数据延迟较大时**: 追平历史数据
2. **数据基本同步后**: 进入实时增量监控模式，只同步最新变化

```bash
# 使用默认配置（60秒循环间隔，300秒实时阈值）
./ch_sync --config config.yaml --yes

# 自定义循环间隔（例如每30秒检查一次）
./ch_sync --config config.yaml --yes --loop-interval 30

# 自定义实时阈值（例如延迟超过600秒才重新追平）
./ch_sync --config config.yaml --yes --realtime-threshold 600
```

智能模式特点:

- **自动判断**: 根据数据延迟自动决定是追平历史还是实时同步
- **高效实时**: 实时模式下只查询最近几秒的新数据，效率极高
- **无缝切换**: 如果检测到延迟过大，自动切换回追平模式
- **持续运行**: 7x24小时不间断运行，按Ctrl+C优雅退出
- **进度可见**: 显示循环次数、同步耗时、新增记录数

### 完整示例

```bash
# 智能循环同步（推荐）
./ch_sync --config config.yaml --yes

# 自定义循环间隔为30秒
./ch_sync --config config.yaml --yes --loop-interval 30

# 自定义实时阈值为600秒
./ch_sync --config config.yaml --yes --realtime-threshold 600

# 只同步指定表
./ch_sync --config config.yaml --yes --tables "table1,table2"

# 预览模式（查看同步计划）
./ch_sync --config config.yaml --dry-run
```

## 命令行参数

| 参数 | 说明 | 默认值 |
| --- | --- | --- |
| `--config` | 配置文件路径 | `config.yaml` |
| `--dry-run` | 预览模式（不实际执行） | `false` |
| `--resume` | 断点续传 | `false` |
| `--tables` | 指定同步的表（逗号分隔） | 全部启用的表 |
| `--clear-state` | 清空状态文件 | `false` |
| `--yes` | 跳过确认提示 | `false` |
| `--loop-interval` | 循环间隔（秒） | `60` |
| `--realtime-threshold` | 实时模式阈值（秒），延迟超过此值先追平历史 | `300` |

## 工作流程

### 智能同步流程

1. **加载配置**: 读取配置文件，验证配置项
2. **连接数据库**: 建立源数据库和目标数据库连接
3. **表结构同步**: 检查并同步表结构（可选）
4. **智能数据同步**（循环执行）:
   - 检测每个表的数据延迟
   - **延迟 > 阈值**: 执行历史数据追平（分段同步）
   - **延迟 ≤ 阈值**: 实时增量同步（只查询最近变化）
   - 去重、批量插入
   - 保存断点状态
5. **等待下一个周期**: 休眠指定间隔后继续循环
6. **优雅退出**: 按Ctrl+C退出，完成当前循环后安全退出

## 表结构同步

工具支持自动同步表结构:

- **自动创建表**: 目标库表不存在时，自动从源库复制完整的表结构
- **自动同步新增字段**: 检测源库和目标库的字段差异，自动添加新字段
- **安全性保障**: 只支持新增字段，不删除或修改已有字段

## 去重策略

支持灵活的去重配置:

### 单字段去重

```yaml
dedupe_keys: ["id"]
```

### 组合字段去重

```yaml
dedupe_keys: ["created_at", "user_id"]
```

工具会自动构建复合键进行去重。

## 智能循环模式

### 运行模式说明

程序默认运行在**智能循环模式**下，类似Redis的主从同步机制：

1. **初始阶段**（历史数据追平）
   - 检测目标库的最新时间
   - 如果目标库为空或延迟超过阈值（默认300秒）
   - 自动执行历史数据追平，按天分段同步

2. **实时阶段**（增量监控）
   - 当历史数据追平后
   - 进入实时增量模式
   - 每次循环只查询最近5秒的新数据
   - 检测到新数据立即同步，无新数据则静默跳过

3. **自动切换**
   - 如果某次检测发现延迟又变大（超过阈值）
   - 自动切换回历史追平模式
   - 追平后再次进入实时模式

### 使用场景

- **场景1**: 目标库为空，首次同步
  - 程序会先追平所有历史数据
  - 追平完成后自动进入实时监控

- **场景2**: 目标库有数据，继续同步
  - 如果延迟小于300秒，直接实时监控
  - 如果延迟大于300秒，先追平再实时

- **场景3**: 7x24小时运行
  - 程序持续运行，每60秒检查一次
  - 有新数据就同步，无新数据就等待
  - 不会对源库造成太大压力

### 后台运行

```bash
# 使用提供的脚本
./run_smart_sync.sh

# 或使用nohup后台运行
nohup ./ch_sync --config config.yaml --yes >> ch_sync.log 2>&1 &

# 使用screen运行
screen -S ch_sync
./ch_sync --config config.yaml --yes
# 按 Ctrl+A, D 分离会话

# 使用tmux运行
tmux new -s ch_sync
./ch_sync --config config.yaml --yes
# 按 Ctrl+B, D 分离会话
```

### 监控运行状态

```bash
# 查看实时日志
tail -f ch_sync.log

# 查看进程
ps aux | grep ch_sync

# 查看同步状态
cat /tmp/clickhouse_sync_state.json | jq '.'

# 停止程序（优雅退出）
kill -INT $(ps aux | grep "ch_sync --config" | grep -v grep | awk '{print $2}')
```

## 日志输出

工具会输出详细的同步日志，包括:

- 📖 配置加载
- 🔌 数据库连接
- 🔧 表结构同步
- 🔄 循环次数和模式切换
- 📊 数据延迟检测
- 🔍 新数据检测
- 📦 批量同步进度
- 🔑 去重统计
- ✅ 同步完成统计
- 💤 等待下一循环

### 日志示例

```log
🔄 智能循环模式已启用
⚙️  实时阈值: 300 秒（延迟超过此值会先追平历史数据）
⚙️  循环间隔: 60 秒
💡 按 Ctrl+C 退出

========================================
🔄 开始第 1 次同步循环
========================================

🚀 智能模式：开始同步 3 个表（最大并发: 3）
⚙️  实时模式阈值: 5m0s（延迟超过此值将先追平历史数据）
🚦 table1: 开始智能同步...
📊 table1: 数据延迟 2h30m，开始追平历史数据...
📊 table1: 同步时间范围 2026-01-28T10:00:00+08:00 ~ 2026-01-29T12:30:00+08:00
✅ table1: 历史数据已追平
🔄 table1: 已进入实时增量模式（监控最新变化）
🔍 table1: 检测到 15 条新记录（12:30:05 ~ 12:30:10）
✅ table1: 实时同步完成，新增 15 条记录
✅ table1: 同步完成 | 耗时: 3.2s, 记录数: 2,845

🚦 table2: 开始智能同步...
🔄 table2: 已进入实时增量模式（监控最新变化）
✅ table2: 同步完成 | 耗时: 0.1s, 记录数: 0

✅ 第 1 次同步循环完成，耗时: 15.3s

💤 等待 60 秒后开始下一次同步...
```

## 常见问题

### 1. 如何添加新表同步？

只需在 `config.yaml` 的 `tables` 部分添加配置:

```yaml
tables:
  - name: "new_table"
    mode: "incremental"
    time_field: "created_at"
    dedupe_keys: ["id"]
    enabled: true
```

### 2. 如何修改批量大小？

全局修改:

```yaml
sync:
  batch_size: 5000
```

或针对单个表修改:

```yaml
tables:
  - name: "large_table"
    batch_size: 5000
```

### 3. 如何跳过某个表？

设置 `enabled: false`:

```yaml
tables:
  - name: "skip_table"
    enabled: false
```

### 4. 同步中断后如何继续？

使用 `--resume` 参数:

```bash
./ch_sync --config config.yaml --resume
```

### 5. 如何清空状态重新同步？

使用 `--clear-state` 参数:

```bash
./ch_sync --config config.yaml --clear-state
./ch_sync --config config.yaml --yes
```

## 架构设计

工具采用模块化设计:

- **config.go**: 配置文件解析
- **connection.go**: 数据库连接管理
- **schema.go**: 表结构检测
- **schema_sync.go**: 表结构同步
- **deduplicator.go**: 去重逻辑
- **state.go**: 状态管理
- **syncer.go**: 核心同步逻辑
- **coordinator.go**: 并行协调
- **validator.go**: 数据验证
- **utils.go**: 工具函数
- **main.go**: 主程序入口

## 性能优化

- 批量插入: 减少数据库交互次数
- 并行同步: 多表同时同步
- 按天分段: 控制单次查询数据量
- LZ4 压缩: 减少网络传输
- 连接池: 复用数据库连接

## 注意事项

1. 确保目标库用户有 `CREATE TABLE` 和 `ALTER TABLE` 权限
2. 增量同步依赖时间字段，确保时间字段准确
3. 去重键应该能唯一标识记录
4. 大表同步建议调大 `batch_size` 和 `max_concurrency`
5. 定时任务建议使用 `--yes` 参数跳过确认

## 许可证

MIT License
