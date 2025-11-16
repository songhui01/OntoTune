#!/bin/bash
# chmod +x restart_onto.sh
# ./restart_onto.sh clean             # <-- delete old data
# ./restart_onto.sh path=~/log.txt    # <-- save output
# ./restart_onto.sh clean path=~/log.txt

echo ">>> Killing running main.py processes..."
PIDS=$(ps aux | grep 'python3.*main.py' | grep -v grep | awk '{print $2}')
if [ -n "$PIDS" ]; then
    echo ">>> Found PIDs: $PIDS"
    sudo kill -9 $PIDS
    echo ">>> Waiting 2 seconds for cleanup..."
    sleep 2
else
    echo ">>> No running main.py process found."
fi

# Parse optional log path
LOG_PATH=""
for arg in "$@"; do
    if [[ "$arg" == path=* ]]; then
        LOG_PATH="${arg#path=}"
        LOG_PATH="${LOG_PATH/#\~/$HOME}"
    fi
done

# Handle cleanup
if [[ "$1" == "clean" ]]; then
    echo ">>> [CLEAN] Removing onto.db..."
    rm -f onto.db
    rm -f onto2.db
    echo ">>> [CLEAN] Removing onto*_model directories..."
    rm -rf onto*_model
else
    echo ">>> Skipping data cleanup (use './restart_onto.sh clean' to enable)"
fi

echo ">>> Starting main.py..."
if [ -n "$LOG_PATH" ]; then
    echo ">>> Logging output to $LOG_PATH"
    python3 -u main.py | tee "$LOG_PATH"
else
    python3 main.py
fi
