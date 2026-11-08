#!/usr/bin/env bash
# ==============================================================================
# Off-Heap Store: Stop 7-Day Continuous Soak Engine
# ==============================================================================
set -euo pipefail

PID_FILE="/home/antigravity/soak_engine.pid"

if [[ ! -f "$PID_FILE" ]] || ! kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    echo "Soak engine is not currently running."
    rm -f "$PID_FILE"
    exit 0
fi

PID=$(cat "$PID_FILE")
echo "Stopping Soak Engine (PID: $PID)..."
kill -TERM "$PID" 2>/dev/null || true

for i in {1..10}; do
    if ! kill -0 "$PID" 2>/dev/null; then
        echo "✔ Soak engine stopped cleanly."
        rm -f "$PID_FILE"
        exit 0
    fi
    sleep 1
done

if kill -0 "$PID" 2>/dev/null; then
    echo "Force killing PID $PID..."
    kill -KILL "$PID" 2>/dev/null || true
fi
rm -f "$PID_FILE"
echo "✔ Soak engine stopped."
