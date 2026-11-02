#!/usr/bin/env bash
# ==============================================================================
# Script: destroy_cluster.sh
# Purpose: Cleanly and completely destroy the Spark Kind Kubernetes cluster,
#          associated pods, services, storage, and Docker container nodes.
# ==============================================================================

set -eo pipefail

CLUSTER_NAME="${1:-spark-cluster}"

echo "=================================================================="
echo " Destroying Kubernetes Kind Cluster: '${CLUSTER_NAME}'"
echo "=================================================================="

# Check if kind is installed
if ! command -v kind &>/dev/null; then
    echo "Error: 'kind' command not found. Please install Kind."
    exit 1
fi

# Check if the cluster actually exists
EXISTING_CLUSTERS=$(kind get clusters 2>/dev/null || true)

if echo "$EXISTING_CLUSTERS" | grep -qx "${CLUSTER_NAME}"; then
    echo "[1/3] Cluster '${CLUSTER_NAME}' found. Checking for active namespaces/resources..."

    # Optional graceful cleanup if kubectl can talk to the cluster
    if kubectl cluster-info --context "kind-${CLUSTER_NAME}" &>/dev/null; then
        echo "      Cleaning up 'spark' namespace workloads..."
        kubectl delete namespace spark --timeout=30s --ignore-not-found=true 2>/dev/null || true
    fi

    echo "[2/3] Deleting Kind cluster '${CLUSTER_NAME}' and its container nodes..."
    kind delete cluster --name "${CLUSTER_NAME}"
    echo "      ✓ Kind cluster '${CLUSTER_NAME}' deleted successfully."
else
    echo "[!] Cluster '${CLUSTER_NAME}' does not exist in Kind. Skipping cluster deletion."
fi

# Clean up any leftover stopped containers or dangling kind network artifacts specifically for this cluster
echo "[3/3] Checking for any lingering Docker artifacts for '${CLUSTER_NAME}'..."
LEFTOVER_CONTAINERS=$(docker ps -a --filter "name=${CLUSTER_NAME}" -q 2>/dev/null || true)
if [ -n "$LEFTOVER_CONTAINERS" ]; then
    echo "      Removing leftover containers matching '${CLUSTER_NAME}'..."
    docker rm -f $LEFTOVER_CONTAINERS
fi

echo ""
echo "=================================================================="
echo " Verification: Remaining Kind Clusters:"
echo "=================================================================="
kind get clusters 2>/dev/null || echo "No active kind clusters."

echo ""
echo "=================================================================="
echo " Active Docker Containers:"
echo "=================================================================="
docker ps --format "table {{.ID}}\t{{.Image}}\t{{.Status}}\t{{.Names}}"

echo ""
echo "✓ All resources for '${CLUSTER_NAME}' have been destroyed."
