#!/usr/bin/env bash

# create the input topic with one partition
kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic fruit-input

# create the output topic with one partition
kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic fruit-output-count
