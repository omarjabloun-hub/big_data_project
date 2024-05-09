#!/bin/bash
# Start Hadoop services
./start-hadoop.sh

# Create directories in HDFS
hdfs dfs -mkdir -p /user/root/input

# This command assumes the file is already in the container. See Dockerfile change below.
hdfs dfs -put /root/customer_data.csv /user/root/input/

# Keep the container running after the script finishes
tail -f /dev/null