#!/usr/bin/env bash

kafka-avro-console-consumer --bootstrap-server localhost:9092 \
    --topic generic-avro-purchases \
    --from-beginning \
    --property print.key=true \
    --property print.value=true

#kafka-avro-console-consumer --bootstrap-server localhost:9092 \
#    --topic specific-avro-purchases \
#    --from-beginning \
#    --property print.key=true \
#    --property print.value=true