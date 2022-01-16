#!/usr/bin/env bash

if [[ "${1}" == "--json" ]]; then
  topic="onsite-user-commands-json"
  converter="io.confluent.connect.json.JsonSchemaConverter"
elif [[ "${1}" == "--proto" ]]; then
  topic="onsite-user-commands-proto"
  converter="io.confluent.connect.protobuf.ProtobufConverter"
elif [[ "${1}" == "--avro" ]]; then
  topic="onsite-user-commands-avro"
  converter="io.confluent.connect.avro.AvroConverter"
else
  echo "Use the generator with the arg --json, --proto or --avro"
  exit 1
fi

module="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

export topic
export module
export converter

PAYLOAD=$(envsubst <"${module}/connect-generator.json")

echo "Starting the generator with:"
echo $PAYLOAD | jq
curl -X POST -H "Content-Type: application/json" --url http://localhost:8083/connectors --data "${PAYLOAD}"
