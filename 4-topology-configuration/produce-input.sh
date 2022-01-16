#!/usr/bin/env bash

module="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
package="io.conducktor.course.streams.generator"
jar="4-topology-configuration-3.0-SNAPSHOT-jar-with-dependencies.jar"

if [ ! -f "${module}/target/${jar}" ]; then
    echo "The archive ${jar} does not exist, run : mvn package"
    exit 1
fi

input="${1:-1}"

item_num() {
   [[ "${input}" -eq "error" ]] && echo "0" || echo "${input}"
}

order_number() {
  order=$(cat /dev/urandom | LC_ALL=C tr -dc '0-9' | fold -w 8 | head -n 1)
  [[ "${input}" -eq "error" ]] && echo "-1" || echo "${order}"
}
echo order="$(order_number)" items="$(item_num)"

java -cp "${module}/target/${jar}" "${package}.OrderProducer" "$(order_number)" "$(item_num)" >> /dev/null
