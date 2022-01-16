#!/usr/bin/env bash

kafka-avro-console-consumer --bootstrap-server localhost:9092 --topic avro-user-purchases --property print.key=true