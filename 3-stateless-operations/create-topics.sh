#!/usr/bin/env bash

# create input topic with one partition to get full ordering
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 \
  --topic user-profile-updates

# create intermediary log compacted topic
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 \
  --topic users-favourite-meal --config cleanup.policy=compact

# create output log compacted topic
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 \
  --topic favourite-meals-output --config cleanup.policy=compact