#!/usr/bin/env bash
set -euo pipefail

# teardown_multipass_cluster.sh
# Stops and deletes Multipass Spark cluster VMs

NODES=("spark-master" "spark-worker-1" "spark-worker-2")

echo "Stopping and purging Multipass nodes..."
for node in "${NODES[@]}"; do
    if multipass list | grep -q "$node"; then
        echo "Deleting $node..."
        multipass delete "$node"
    fi
done

multipass purge
echo "Multipass Spark cluster teardown complete."
