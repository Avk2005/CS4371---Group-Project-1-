#!/usr/bin/env bash
# Create topic1 and topic2 in the Kafka container (safe to run twice).
set -euo pipefail
for t in topic1 topic2; do
  docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic "$t" --partitions 1 --replication-factor 1
done
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
