#!/usr/bin/env bash

# create the input topic with one partitions
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic fruit-input

# create the output topic
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic fruit-output-count