#!/usr/bin/env bash

kafka-console-producer --broker-list localhost:9092 --topic fruit-input

# or # confluent local services kafka produce fruit-input

# input examples
# apples grapes bananas
# pineapples apples pears
# kiwis peaches app
