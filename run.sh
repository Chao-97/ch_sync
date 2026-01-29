#!/bin/bash

# ClickHouse 智能同步工具启动脚本

set -e

# 配置文件路径
CONFIG_FILE="config.yaml"

# 日志文件
LOG_DIR="./log"
LOG_FILE="$LOG_DIR/ch_sync_$(date +%Y%m%d_%H%M%S).log"

# 创建日志目录
mkdir -p "$LOG_DIR"

# 颜色输出
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}ClickHouse 智能同步工具${NC}"
echo -e "${GREEN}========================================${NC}"

# 检查配置文件是否存在
if [ ! -f "$CONFIG_FILE" ]; then
    echo -e "${RED}错误: 配置文件 $CONFIG_FILE 不存在${NC}"
    exit 1
fi

# 检查可执行文件是否存在
if [ ! -f "./ch_sync" ]; then
    echo -e "${YELLOW}未找到可执行文件，开始构建...${NC}"
    go build -o ch_sync
    echo -e "${GREEN}构建完成${NC}"
fi

# 显示菜单
echo ""
echo "运行模式:"
echo -e "${BLUE}1. 启动智能循环同步（推荐）${NC}"
echo "   - 自动追平历史数据"
echo "   - 进入实时增量监控"
echo "   - 7x24小时运行，Ctrl+C退出"
echo ""
echo "其他操作:"
echo "2. 预览同步计划（dry-run）"
echo "3. 只同步指定表"
echo "4. 清空状态文件重新开始"
echo "5. 查看同步状态"
echo "6. 查看最新日志"
echo "0. 退出"
echo ""

read -p "请输入选项 [0-6]: " choice

case $choice in
    1)
        echo ""
        echo -e "${GREEN}========================================${NC}"
        echo -e "${GREEN}启动智能循环同步${NC}"
        echo -e "${GREEN}========================================${NC}"
        echo ""
        echo "模式: 智能循环同步"
        echo "  - 首次: 追平历史数据"
        echo "  - 之后: 实时增量监控"
        echo ""
        echo "默认配置:"
        echo "  - 循环间隔: 10秒"
        echo "  - 实时阈值: 300秒"
        echo "  - 表数量: 7张"
        echo ""
        echo -e "${YELLOW}提示: 按 Ctrl+C 可优雅退出${NC}"
        echo ""
        read -p "是否使用默认配置启动? (yes/no): " confirm

        if [ "$confirm" = "yes" ]; then
            echo -e "${GREEN}开始启动...${NC}"
            echo ""
            ./ch_sync --config "$CONFIG_FILE" --yes 2>&1 | tee "$LOG_FILE"
        else
            read -p "请输入循环间隔（秒，默认10）: " interval
            interval=${interval:-10}
            read -p "请输入实时阈值（秒，默认300）: " threshold
            threshold=${threshold:-300}
            echo ""
            echo -e "${GREEN}使用自定义配置启动...${NC}"
            echo "  循环间隔: ${interval}秒"
            echo "  实时阈值: ${threshold}秒"
            echo ""
            ./ch_sync --config "$CONFIG_FILE" --yes \
                --loop-interval "$interval" \
                --realtime-threshold "$threshold" 2>&1 | tee "$LOG_FILE"
        fi
        ;;
    2)
        echo -e "${GREEN}正在预览同步计划...${NC}"
        ./ch_sync --config "$CONFIG_FILE" --dry-run
        ;;
    3)
        read -p "请输入表名（多个表用逗号分隔）: " tables
        echo ""
        echo -e "${GREEN}开始智能同步指定表: $tables${NC}"
        echo ""
        ./ch_sync --config "$CONFIG_FILE" --yes --tables "$tables" 2>&1 | tee "$LOG_FILE"
        ;;
    4)
        echo ""
        echo -e "${YELLOW}警告: 这将清空所有同步状态，需要重新追平历史数据！${NC}"
        read -p "确定要清空状态文件吗? (yes/no): " confirm
        if [ "$confirm" = "yes" ]; then
            ./ch_sync --config "$CONFIG_FILE" --clear-state
            echo -e "${GREEN}状态文件已清空${NC}"
        else
            echo -e "${YELLOW}取消操作${NC}"
        fi
        ;;
    5)
        state_file="/tmp/clickhouse_sync_state.json"
        if [ -f "$state_file" ]; then
            echo -e "${GREEN}同步状态:${NC}"
            echo ""
            if command -v jq &> /dev/null; then
                cat "$state_file" | jq '.'
            else
                cat "$state_file"
            fi
        else
            echo -e "${YELLOW}未找到状态文件${NC}"
        fi
        ;;
    6)
        latest_log=$(ls -t "$LOG_DIR"/ch_sync_*.log 2>/dev/null | head -1)
        if [ -n "$latest_log" ]; then
            echo -e "${GREEN}显示最新日志: $latest_log${NC}"
            echo ""
            tail -100 "$latest_log"
        else
            echo -e "${YELLOW}未找到日志文件${NC}"
        fi
        ;;
    0)
        echo -e "${GREEN}退出${NC}"
        exit 0
        ;;
    *)
        echo -e "${RED}无效的选项${NC}"
        exit 1
        ;;
esac

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}操作完成${NC}"
echo -e "${GREEN}========================================${NC}"
