#!/usr/bin/env bash
# ==============================================================================
# Off-Heap Store: 7-Day Continuous Soak & Extreme Stress Launcher
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export JAVA_HOME="/home/antigravity/.jdks/jdk-25"
export PATH="$JAVA_HOME/bin:$PATH"

PID_FILE="/home/antigravity/soak_engine.pid"
DAEMON_LOG="/home/antigravity/soak_daemon.log"
ENGINE_LOG="/home/antigravity/soak_engine.log"
EDGE_LOG="/home/antigravity/edge_cases.log"

if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    echo "Soak engine is already running (PID: $(cat "$PID_FILE"))."
    echo "Use ./check_soak_status.sh to monitor progress or ./stop_7day_soak.sh to stop it."
    exit 0
fi

echo "================================================================================"
echo "   🚀 Starting Off-Heap Store 7-Day Continuous Soak & Extreme Chaos Engine 🚀"
echo "================================================================================"
echo "Runtime JVM : $($JAVA_HOME/bin/java -version 2>&1 | head -n 1)"
echo "Target Log  : $ENGINE_LOG"
echo "Daemon Log  : $DAEMON_LOG"
echo "Duration    : 10,080 minutes (7 days / 168 hours)"
echo "Concurrency : 6 Worker Threads (Mixed Read/Write/CAS/TTL/Batch/Crash)"
echo "================================================================================"

# 确保编译最新字节码
cd "$SCRIPT_DIR"
./build.sh > /dev/null

# 启动 7 天不间断混沌压测后台守护进程
nohup "$JAVA_HOME/bin/java" \
    --enable-native-access=ALL-UNNAMED \
    -ea \
    -Xms1g -Xmx2g \
    -XX:+UseZGC -XX:+ZGenerational \
    -cp target/classes:target/test-classes \
    offheap.ContinuousSoakStressEngine \
    --duration-minutes 10080 \
    --workers 6 \
    --policy TINY_LFU \
    --slots-per-class 2000 \
    --key-space 50000 \
    --reset-storage \
    > "$DAEMON_LOG" 2>&1 &

ENGINE_PID=$!
echo "$ENGINE_PID" > "$PID_FILE"

echo "✔ Soak engine started in background successfully! (PID: $ENGINE_PID)"
echo "Run './check_soak_status.sh' at any time to inspect live telemetry."
