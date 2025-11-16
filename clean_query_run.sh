#!/bin/bash
# chmod +x clean_query_run.sh
# ./clean_query_run.sh

echo ">>> Killing all running run_queries processes..."

PIDS=$(ps -eo pid,cmd | grep run_queries | grep -v grep | awk '{print $1}')
if [ -n "$PIDS" ]; then
    echo ">>> Found PIDs: $PIDS"
    echo "$PIDS" | xargs kill -9
    echo ">>> Killed."
else
    echo ">>> No run_queries process found."
fi
