#!/usr/bin/env bash
# ==============================================================================
# Off-Heap Store: 7-Day Continuous Soak Live Dashboard & Inspector
# ==============================================================================
set -euo pipefail

ENGINE_LOG="/home/antigravity/soak_engine.log"
EDGE_LOG="/home/antigravity/edge_cases.log"

echo "================================================================================"
echo "   📊 Off-Heap Store 7-Day Continuous Soak Live Dashboard"
echo "================================================================================"

PID=$(pgrep -f "ContinuousSoakStressEngine" | head -n 1 || true)

if [[ -z "$PID" ]]; then
    echo "Status: [STOPPED / INACTIVE]"
else
    echo "Status        : [RUNNING ACTIVE]"
    echo "Process PID   : $PID"
    ps -p "$PID" -o pid,%cpu,%mem,etime,rss,command | tail -n 1
fi

echo "────────────────────────────────────────────────────────────────────────────────"
echo "最新监控日志采样 (/home/antigravity/soak_engine.log):"
echo "────────────────────────────────────────────────────────────────────────────────"
if [[ -f "$ENGINE_LOG" ]]; then
    head -n 1 "$ENGINE_LOG"
    tail -n 6 "$ENGINE_LOG"
else
    echo "(No soak_engine.log generated yet)"
fi

echo "────────────────────────────────────────────────────────────────────────────────"
echo "边缘异常检测 (/home/antigravity/edge_cases.log):"
echo "────────────────────────────────────────────────────────────────────────────────"
if [[ -f "$EDGE_LOG" ]] && [[ -s "$EDGE_LOG" ]]; then
    echo "⚠️ 发现异常记录:"
    tail -n 10 "$EDGE_LOG"
else
    echo "✔ 零异常 / 零错误记录 (All operations running clean, 0 anomalies)"
fi
echo "================================================================================"
