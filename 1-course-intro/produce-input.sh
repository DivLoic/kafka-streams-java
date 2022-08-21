#!/usr/bin/env bash

kafka-console-producer.sh --broker-list localhost:9092 --topic fruit-input

# input examples
# apples grapes bananas
# pineapples apples pears
# kiwis peaches apples
