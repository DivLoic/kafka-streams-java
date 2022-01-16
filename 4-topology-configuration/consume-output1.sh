#!/usr/bin/env bash

# start a consumer on the output topic
kafka-console-consumer --bootstrap-server localhost:9092 \
    --topic street-food-notifications \
    --from-beginning \
    --property print.key=false \
    --property print.value=true \
    --property print.topic=true
