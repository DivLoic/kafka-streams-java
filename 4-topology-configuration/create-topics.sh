#!/usr/bin/env bash

# create input topic
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 4 \
  --topic street-food-orders

# create error output topic
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 \
  --topic street-food-order-errors

# create output topic
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 \
  --topic street-food-notifications