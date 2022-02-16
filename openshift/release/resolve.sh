#!/usr/bin/env bash

function resolve_resources() {
  echo $@

  local dir=$1
  local resolved_file_name=$2
  local image_prefix=$3
  local image_tag=${4-""}

  echo "Writing resolved yaml to $resolved_file_name ${image_tag}"

  for yaml in "$dir"/*.yaml; do
    echo "Resolving ${yaml}"

    echo "---" >>$resolved_file_name
    # 1. Prefix test image references with test-
    # 2. Rewrite image references
    # 3. Remove comment lines
    # 4. Remove empty lines
    sed -e "s+\(.* image: \)\(knative.dev\)\(.*/\)\(test/\)\(.*\)+\1\2 \3\4test-\5+g" \
      -e "s+ko://++" \
      -e "s+kafka.eventing.knative.dev/release: devel+kafka.eventing.knative.dev/release: ${release}+" \
      -e "s+\${KNATIVE_KAFKA_DISPATCHER_IMAGE}+${image_prefix}broker-dispatcher${image_tag}+" \
      -e "s+\${KNATIVE_KAFKA_RECEIVER_IMAGE}+${image_prefix}broker-receiver${image_tag}+" \
      -e "s+\(.* image: \)\(knative.dev\)\(.*/\)\(.*\)+\1${image_prefix}broker-\4${image_tag}+g" \
      -e '/^[ \t]*#/d' \
      -e '/^[ \t]*$/d' \
      "$yaml" >>$resolved_file_name
  done
}
