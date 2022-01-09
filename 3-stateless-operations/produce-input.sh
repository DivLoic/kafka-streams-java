#!/usr/bin/env bash

produce() {
  echo "${1}" | kafka-console-producer --broker-list localhost:9092 --topic user-profile-updates
}

pipe1="echo \"<user_id>,goto-meal:<meal_name>\""
pipe2="kafka-console-producer --broker-list localhost:9092 --topic user-profile-updates"
echo "Running: ${pipe1} | ${pipe2}  "


produce 'ldivad,goto-meal:entrecote 🇫🇷 '
produce 'emmab,goto-meal:🇸🇪 meatballs'
produce 'lucas,goto-meal:entrecote 🇫🇷 '
produce 'guilbia,goto-meal:entrecote 🇫🇷 '
produce 'mattp94,photo:cGhvdG8K'
