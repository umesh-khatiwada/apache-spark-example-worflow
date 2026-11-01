#!/usr/bin/env bash
set -euo pipefail

# setup_multipass_cluster.sh
# Provisions a 3-node Apache Spark cluster using Multipass
# Master: spark-master
# Workers: spark-worker-1, spark-worker-2

SPARK_VERSION="3.5.3"
HADOOP_VERSION="3"
SPARK_TAR="spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"
SPARK_URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_TAR}"

echo "=========================================================="
echo " Starting Multipass Apache Spark Cluster Provisioning     "
echo " Spark Version: ${SPARK_VERSION}                          "
echo "=========================================================="

NODES=("spark-master" "spark-worker-1" "spark-worker-2")

for node in "${NODES[@]}"; do
    if multipass list | grep -q "$node"; then
        STATE=$(multipass info "$node" | grep "State:" | awk '{print $2}')
        if [ "$STATE" != "Running" ]; then
            echo "Starting existing node: $node"
            multipass start "$node"
        else
            echo "Node $node is already running."
        fi
    else
        echo "Launching node: $node (2 CPU, 2G RAM, 5G Disk)"
        multipass launch --name "$node" --cpus 2 --memory 2G --disk 5G 24.04
    fi
done

MASTER_IP=$(multipass info spark-master | grep "IPv4:" | awk '{print $2}')
echo "Spark Master IP: ${MASTER_IP}"

for node in "${NODES[@]}"; do
    echo "Configuring Java and Spark on $node..."
    multipass exec "$node" -- bash -c "
        sudo apt-get update -qq && sudo apt-get install -y -qq openjdk-17-jre-headless python3 python3-pip curl wget tar
        if [ ! -d /opt/spark ]; then
            wget -q ${SPARK_URL} -O /tmp/spark.tgz
            sudo tar -xzf /tmp/spark.tgz -C /opt
            sudo mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark
            rm /tmp/spark.tgz
        fi
    "
done

echo "Starting Master on spark-master..."
multipass exec spark-master -- bash -c "/opt/spark/sbin/start-master.sh -h ${MASTER_IP} -p 7077 --webui-port 8080"

for worker in "spark-worker-1" "spark-worker-2"; do
    echo "Starting Worker on $worker connecting to spark://${MASTER_IP}:7077..."
    multipass exec "$worker" -- bash -c "/opt/spark/sbin/start-worker.sh spark://${MASTER_IP}:7077"
done

echo "=========================================================="
echo " Cluster successfully started!                            "
echo " Spark Master URL: spark://${MASTER_IP}:7077              "
echo " Spark Web UI:     http://${MASTER_IP}:8080               "
echo "=========================================================="
