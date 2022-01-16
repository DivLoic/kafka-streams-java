#!/usr/bin/env bash

kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 4 \
  --topic raw-temperature-measures


kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 \
  --topic quality-warnings