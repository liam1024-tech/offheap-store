#!/usr/bin/env bash
# ==============================================================================
# Off-Heap Store: Continuous 10-Minute Patrol Daemon
# Periodically inspects soak engine, analyzes metric drifts, checks edge cases,
# updates the diagnostic report, and performs closed-loop recovery if needed.
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export JAVA_HOME="/home/antigravity/.jdks/jdk-25"
export PATH="$JAVA_HOME/bin:$PATH"

CYCLE_FILE="/home/antigravity/patrol_cycle.txt"
PID_FILE="/home/antigravity/patrol_daemon.pid"
DAEMON_LOG="/home/antigravity/patrol_daemon.log"

if [[ ! -f "$CYCLE_FILE" ]]; then
    echo "1" > "$CYCLE_FILE"
fi

echo "$$" > "$PID_FILE"
echo "=== 10-Minute Patrol Daemon Started at $(date) (PID: $$) ===" >> "$DAEMON_LOG"

while true; do
    CYCLE=$(cat "$CYCLE_FILE" 2>/dev/null || echo "1")
    echo ">>> Running Patrol Cycle #${CYCLE} at $(date) <<<" >> "$DAEMON_LOG"

    # 1. 多维立体检查压测引擎是否存活 (防跨容器/子shell进程隔离误判)
    SOAK_RUNNING=0
    # 维度 A: PID 文件及进程存活探测
    if [[ -f "/home/antigravity/soak_engine.pid" ]]; then
        SOAK_PID=$(cat /home/antigravity/soak_engine.pid 2>/dev/null || echo "")
        if [[ -n "$SOAK_PID" ]] && kill -0 "$SOAK_PID" 2>/dev/null; then
            SOAK_RUNNING=1
        fi
    fi
    # 维度 B: 进程关键字匹配
    if [[ "$SOAK_RUNNING" -eq 0 ]] && ps aux | grep -v grep | grep -q "ContinuousSoakStressEngine"; then
        SOAK_RUNNING=1
    fi
    # 维度 C: 日志心跳刷新探测 (30秒内有写入则必定运行中)
    if [[ "$SOAK_RUNNING" -eq 0 && -f "/home/antigravity/soak_engine.log" ]]; then
        NOW_SEC=$(date +%s)
        LOG_MTIME=$(stat -c %Y /home/antigravity/soak_engine.log 2>/dev/null || echo 0)
        LOG_AGE=$((NOW_SEC - LOG_MTIME))
        if [[ "$LOG_AGE" -lt 30 ]]; then
            SOAK_RUNNING=1
        fi
    fi

    if [[ "$SOAK_RUNNING" -eq 0 ]]; then
        echo "[CLOSED-LOOP ALERT] ContinuousSoakStressEngine not running! Triggering automatic recovery..." >> "$DAEMON_LOG"
        nohup "$JAVA_HOME/bin/java" \
            --enable-native-access=ALL-UNNAMED \
            -ea \
            -Xms1g -Xmx2g \
            -XX:+UseZGC \
            -cp "$SCRIPT_DIR/target/classes:$SCRIPT_DIR/target/test-classes" \
            offheap.ContinuousSoakStressEngine \
            --duration-minutes 10080 \
            --workers 6 \
            --policy TINY_LFU \
            --slots-per-class 2000 \
            --key-space 50000 \
            >> /home/antigravity/soak_engine.log 2>&1 &
        sleep 5
    fi

    # 2. 执行指标统计与阶段性报告生成
    python3 "$SCRIPT_DIR/patrol_analyzer.py" "$CYCLE" >> "$DAEMON_LOG" 2>&1

    # 3. 递增巡检轮次计数
    NEXT_CYCLE=$((CYCLE + 1))
    echo "$NEXT_CYCLE" > "$CYCLE_FILE"

    # 4. 休眠 10 分钟 (600 秒) 直至下一轮巡检
    sleep 600
done
