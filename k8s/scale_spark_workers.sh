#!/usr/bin/env bash
# ==============================================================================
# Script: scale_spark_workers.sh
# Purpose: Add, remove, or set the number of Spark Worker nodes connected to
#          the Spark Master in the Kubernetes cluster.
# ==============================================================================

set -eo pipefail

NAMESPACE="spark"
DEPLOYMENT="spark-workers"

show_usage() {
    echo "Usage: $0 [add|remove|set|status] [number]"
    echo ""
    echo "Commands:"
    echo "  add [N]       Add N worker node(s) (default: 1)"
    echo "  remove [N]    Remove N worker node(s) (default: 1, min: 1)"
    echo "  set <N>       Scale directly to N worker nodes"
    echo "  status        Display current Spark workers and cluster resources"
    echo ""
    echo "Examples:"
    echo "  $0 add          # Adds 1 worker node"
    echo "  $0 add 2        # Adds 2 worker nodes"
    echo "  $0 remove       # Removes 1 worker node"
    echo "  $0 set 4        # Sets total worker nodes to 4"
    echo "  $0 status       # View active workers and cores"
    exit 1
}

# Ensure cluster is accessible
if ! kubectl get namespace "${NAMESPACE}" &>/dev/null; then
    echo "Error: Namespace '${NAMESPACE}' not found. Is the Spark cluster running?"
    exit 1
fi

get_current_replicas() {
    kubectl get deployment "${DEPLOYMENT}" -n "${NAMESPACE}" -o jsonpath='{.spec.replicas}' 2>/dev/null || echo "0"
}

show_status() {
    echo "=================================================================="
    echo " Spark Worker Status in Kubernetes (${NAMESPACE})"
    echo "=================================================================="
    kubectl get pods -n "${NAMESPACE}" -l app=spark-worker -o wide
    echo ""
    echo "=================================================================="
    echo " Spark Master Web UI Summary (http://localhost:30080)"
    echo "=================================================================="
    if curl -s --connect-timeout 2 http://localhost:30080 &>/dev/null; then
        curl -s http://localhost:30080 | grep -E "Alive Workers|Cores in use|Memory in use" | sed -e 's/<[^>]*>//g' -e 's/^[ \t]*/  • /' || true
    else
        echo "  (Master Web UI port 30080 not reachable or still starting)"
    fi
    echo ""
}

ACTION="${1:-status}"
COUNT="${2:-1}"

CURRENT=$(get_current_replicas)

case "$ACTION" in
    add)
        NEW_COUNT=$((CURRENT + COUNT))
        echo "Scaling Spark Workers: ${CURRENT} -> ${NEW_COUNT} (+${COUNT})..."
        kubectl scale deployment "${DEPLOYMENT}" -n "${NAMESPACE}" --replicas="${NEW_COUNT}"
        echo "Waiting for new worker pods to be ready..."
        kubectl wait --for=condition=ready pod -l app=spark-worker -n "${NAMESPACE}" --timeout=120s
        sleep 3
        echo "✓ Successfully added worker node(s)."
        show_status
        ;;
    remove)
        NEW_COUNT=$((CURRENT - COUNT))
        if [ "$NEW_COUNT" -lt 1 ]; then
            echo "Warning: Cannot scale below 1 worker node. Setting to 1."
            NEW_COUNT=1
        fi
        echo "Scaling Spark Workers: ${CURRENT} -> ${NEW_COUNT} (-${COUNT})..."
        kubectl scale deployment "${DEPLOYMENT}" -n "${NAMESPACE}" --replicas="${NEW_COUNT}"
        echo "Waiting for cluster state to update..."
        sleep 3
        echo "✓ Successfully removed worker node(s)."
        show_status
        ;;
    set)
        if [ -z "$2" ]; then
            echo "Error: 'set' requires target number of workers. e.g. $0 set 3"
            exit 1
        fi
        NEW_COUNT="$2"
        if [ "$NEW_COUNT" -lt 1 ]; then
            echo "Error: Minimum worker count is 1."
            exit 1
        fi
        echo "Setting Spark Workers to: ${NEW_COUNT} (was ${CURRENT})..."
        kubectl scale deployment "${DEPLOYMENT}" -n "${NAMESPACE}" --replicas="${NEW_COUNT}"
        if [ "$NEW_COUNT" -gt "$CURRENT" ]; then
            echo "Waiting for new worker pods to be ready..."
            kubectl wait --for=condition=ready pod -l app=spark-worker -n "${NAMESPACE}" --timeout=120s
        else
            sleep 3
        fi
        echo "✓ Successfully updated worker nodes."
        show_status
        ;;
    status)
        show_status
        ;;
    *)
        show_usage
        ;;
esac
