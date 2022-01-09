#!/usr/bin/env bash

# start a consumer on the output topic
kafka-console-consumer --bootstrap-server localhost:9092 \
    --topic user-profile-updates \
    --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true \
    --property print.value=true