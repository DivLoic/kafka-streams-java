#!/usr/bin/env bash

# create input topic
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 12 \
  --topic ratings

# create inout topic
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 4 \
  --topic products --config cleanup.policy=compact

# create output topic
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 6 \
  --topic score-details --config cleanup.policy=compact
