#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

RAW_PATH="${1:-scripts/part1_foundations/01_spark_architecture.py}"

# Resolve absolute path on host
if [[ "$RAW_PATH" = /* ]]; then
    ABS_PATH="$RAW_PATH"
else
    ABS_PATH="$(cd "$(dirname "$RAW_PATH")" 2>/dev/null && pwd)/$(basename "$RAW_PATH")" || ABS_PATH="${PROJECT_ROOT}/${RAW_PATH}"
fi

# Convert to in-container path under /opt/spark/work-dir
if [[ "$ABS_PATH" == "$PROJECT_ROOT"* ]]; then
    REL_PATH="${ABS_PATH#$PROJECT_ROOT/}"
    CONTAINER_PATH="/opt/spark/work-dir/${REL_PATH}"
else
    CONTAINER_PATH="/opt/spark/work-dir/${RAW_PATH#./}"
fi

echo "=========================================================="
echo " Submitting Spark Job to Kubernetes Master"
echo " Host Script:      ${ABS_PATH}"
echo " Container Script: ${CONTAINER_PATH}"
echo "=========================================================="

MASTER_POD=$(kubectl get pod -n spark -l app=spark-master -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)

if [ -z "$MASTER_POD" ]; then
    echo "Error: No spark-master pod found in namespace 'spark'!"
    echo "Please ensure the cluster is running with: ${SCRIPT_DIR}/setup_kind_spark.sh"
    exit 1
fi

kubectl exec -i -n spark "$MASTER_POD" -- /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master-service:7077 \
  --conf spark.driver.host=spark-master-service \
  --conf spark.driver.bindAddress=0.0.0.0 \
  "${CONTAINER_PATH}"
