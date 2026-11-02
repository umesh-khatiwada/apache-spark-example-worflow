#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

CLUSTER_NAME="spark-cluster"

echo "=== 1. Checking / Creating Kind Cluster '${CLUSTER_NAME}' ==="
if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
    echo "Cluster '${CLUSTER_NAME}' already exists."
else
    kind create cluster --config "${SCRIPT_DIR}/kind-cluster-config.yaml" --name "${CLUSTER_NAME}"
fi

kubectl cluster-info --context "kind-${CLUSTER_NAME}"

echo "=== 2. Applying Namespace & RBAC ==="
kubectl apply -f "${SCRIPT_DIR}/spark-rbac.yaml"

echo "=== 3. Applying Shared Storage (PV & PVC) ==="
kubectl apply -f "${SCRIPT_DIR}/spark-storage.yaml"

echo "=== 4. Deploying Spark Master, Workers & Jupyter ==="
kubectl apply -f "${SCRIPT_DIR}/spark-standalone-k8s.yaml"
kubectl apply -f "${SCRIPT_DIR}/spark-jupyter.yaml"

echo "=== 5. Waiting for Spark Master, Workers & Jupyter to be Ready ==="
kubectl wait --for=condition=ready pod -l app=spark-master -n spark --timeout=120s
kubectl wait --for=condition=ready pod -l app=spark-worker -n spark --timeout=180s
kubectl wait --for=condition=ready pod -l app=spark-jupyter -n spark --timeout=180s

# Ensure permissions on data/warehouse for spark container (user 1001)
docker exec "${CLUSTER_NAME}-control-plane" chmod -R 777 /mnt/spark-workspace/data/warehouse 2>/dev/null || true

echo "=== Cluster Ready ==="
kubectl get pods,svc,pv,pvc -n spark
echo ""
echo "Spark Web UI:    http://localhost:30080"
echo "Jupyter Lab URL: http://localhost:8888 (or http://localhost:30888)"
