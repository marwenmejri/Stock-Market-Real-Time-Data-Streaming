#!/bin/bash

echo "Waiting for Kafka to be ready..."
sleep 10

echo "Creating topic 'stock-market' with 3 partitions and RF 3"
/opt/kafka/bin/kafka-topics.sh \
  --create \
  --if-not-exists \
  --topic stock-market \
  --bootstrap-server kafka1:9092,kafka2:9093,kafka3:9094 \
  --partitions 3 \
  --replication-factor 3

echo "Kafka topic 'stock-market' setup complete."
