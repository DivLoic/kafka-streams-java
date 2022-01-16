#!/usr/bin/env bash

# start a consumer on the output topic
kafka-avro-console-consumer --bootstrap-server localhost:9092 \
    --topic short-trip-counts \
    --property print.key=true \
    --property print.value=true \
    --key-deserializer org.apache.kafka.common.serialization.StringDeserializer \
    --property schema.registry.url=http://localhost:8081
