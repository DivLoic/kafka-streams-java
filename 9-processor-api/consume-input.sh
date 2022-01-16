#!/usr/bin/env bash

kafka-avro-console-consumer --bootstrap-server localhost:9092 --topic processor-victories --property print.key=true | grep --color -E '(XXX|$)'