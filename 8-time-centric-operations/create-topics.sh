#!/usr/bin/env bash

# create input topic
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 8 \
  --topic departures

# create inout topic
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 8 \
  --topic arrivals

# create output topic
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 4 \
  --topic short-trip-counts --config cleanup.policy=compact
