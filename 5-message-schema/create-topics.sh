#!/usr/bin/env bash

kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 4 --topic onsite-user-commands-json
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 4 --topic user-orders-json
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 4 --topic user-payment-json

kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 4 --topic onsite-user-commands-proto
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 4 --topic user-orders-proto
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 4 --topic user-payment-proto

kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 4 --topic onsite-user-commands-avro
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 4 --topic user-orders-avro
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 4 --topic user-payment-avro
