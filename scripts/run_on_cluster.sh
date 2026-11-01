#!/usr/bin/env bash
set -euo pipefail

# run_on_cluster.sh
# Submits a python script to the Multipass Spark cluster
# Usage: ./scripts/run_on_cluster.sh <script_path> [extra spark-submit args...]

if [ "$#" -lt 1 ]; then
    echo "Usage: $0 <script_path> [extra spark-submit args...]"
    exit 1
fi

SCRIPT="$1"
shift

if ! multipass info spark-master >/dev/null 2>&1; then
    echo "Error: spark-master Multipass VM is not running."
    echo "Run ./scripts/setup_multipass_cluster.sh first."
    exit 1
fi

MASTER_IP=$(multipass info spark-master | grep "IPv4:" | awk '{print $2}')
REMOTE_TMP="/tmp/spark_job_$(date +%s).py"

echo "Copying $SCRIPT to spark-master:$REMOTE_TMP..."
multipass transfer "$SCRIPT" "spark-master:$REMOTE_TMP"

echo "Submitting job to spark://${MASTER_IP}:7077..."
multipass exec spark-master -- /opt/spark/bin/spark-submit \
    --master "spark://${MASTER_IP}:7077" \
    --deploy-mode client \
    "$@" \
    "$REMOTE_TMP"
