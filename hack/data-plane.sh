#!/usr/bin/env bash

# Copyright 2020 The Knative Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# variables used:
# - KO_DOCKER_REPO (required)

source "$(dirname "$(realpath "${BASH_SOURCE[0]}")")"/label.sh

readonly DATA_PLANE_DIR=data-plane
readonly DATA_PLANE_CONFIG_DIR=${DATA_PLANE_DIR}/config

readonly SOURCE_DATA_PLANE_CONFIG_DIR=${DATA_PLANE_CONFIG_DIR}/source
# Broker config
readonly BROKER_DATA_PLANE_CONFIG_DIR=${DATA_PLANE_CONFIG_DIR}/broker
readonly BROKER_TLS_CONFIG_DIR=${DATA_PLANE_CONFIG_DIR}/broker-tls
readonly SINK_DATA_PLANE_CONFIG_DIR=${DATA_PLANE_CONFIG_DIR}/sink
readonly SINK_TLS_CONFIG_DIR=${DATA_PLANE_CONFIG_DIR}/sink-tls
readonly CHANNEL_DATA_PLANE_CONFIG_DIR=${DATA_PLANE_CONFIG_DIR}/channel
readonly CHANNEL_TLS_CONFIG_DIR=${DATA_PLANE_CONFIG_DIR}/channel-tls

readonly RECEIVER_LOOM_DIRECTORY=receiver-loom
readonly DISPATCHER_LOOM_DIRECTORY=dispatcher-loom

USE_LOOM=${USE_LOOM:-"true"}

if [ $USE_LOOM == "true" ]; then
  echo "Using loom modules ${USE_LOOM}"
fi

# Checks whether the given function exists.
function function_exists() {
  [[ "$(type -t $1)" == "function" ]]
}

if ! function_exists header; then
  function header() {
    echo "$@"
  }
fi

# Use GNU tools on macOS. Requires the 'grep' Homebrew formulae.
if [ "$(uname)" == "Darwin" ]; then
  grep=ggrep
fi

function receiver_build_push() {
  header "Building receiver ..."

  local receiver_sha=""

  local receiver="${KNATIVE_KAFKA_BROKER_RECEIVER:-${KO_DOCKER_REPO}/knative-kafka-broker-receiver-loom}"
  ./mvnw clean package jib:build -pl "${RECEIVER_LOOM_DIRECTORY}" -DskipTests || return $?
  receiver_sha=$(cat "${RECEIVER_LOOM_DIRECTORY}/target/jib-image.digest")

  export KNATIVE_KAFKA_RECEIVER_IMAGE="${receiver}@${receiver_sha}"

  echo "Receiver image ${KNATIVE_KAFKA_RECEIVER_IMAGE}"
}

function dispatcher_build_push() {
  header "Building dispatcher ..."

  local dispatcher_sha=""

  local dispatcher="${KNATIVE_KAFKA_BROKER_DISPATCHER:-${KO_DOCKER_REPO}/knative-kafka-broker-dispatcher-loom}"
  ./mvnw clean package jib:build -pl "${DISPATCHER_LOOM_DIRECTORY}" -DskipTests || return $?
  dispatcher_sha=$(cat "${DISPATCHER_LOOM_DIRECTORY}/target/jib-image.digest")

  export KNATIVE_KAFKA_DISPATCHER_IMAGE="${dispatcher}@${dispatcher_sha}"

  echo "Dispatcher image ${KNATIVE_KAFKA_DISPATCHER_IMAGE}"
}

function data_plane_build_push() {
  pushd "${DATA_PLANE_DIR}" || return $?
  ./mvnw install -DskipTests || fail_test "failed to install data plane"
  receiver_build_push || receiver_build_push || fail_test "failed to build receiver"
  dispatcher_build_push || dispatcher_build_push || fail_test "failed to build dispatcher"
  popd || return $?

  if [ "$KO_DOCKER_REPO" = "kind.local" ]; then
    kind load docker-image "${KNATIVE_KAFKA_RECEIVER_IMAGE}"
    kind load docker-image "${KNATIVE_KAFKA_DISPATCHER_IMAGE}"
  fi
}

function replace_images() {
  local file=$1

  sed -i "s|\${KNATIVE_KAFKA_DISPATCHER_IMAGE}|${KNATIVE_KAFKA_DISPATCHER_IMAGE}|g" "${file}" &&
    sed -i "s|\${KNATIVE_KAFKA_RECEIVER_IMAGE}|${KNATIVE_KAFKA_RECEIVER_IMAGE}|g" "${file}"

  # spefying 'runAsUser: 1001' in the source config for loom images
  if [ "$USE_LOOM" == "true" ]; then
    sed -i "s|runAsNonRoot: true|runAsUser: 1001|g" "${file}"
  fi

  return $?
}

function k8s() {
  header "Creating artifacts"

  echo "Dispatcher image ---> ${KNATIVE_KAFKA_DISPATCHER_IMAGE}"
  echo "Receiver image   ---> ${KNATIVE_KAFKA_RECEIVER_IMAGE}"

  ko resolve ${KO_FLAGS} -Rf ${SOURCE_DATA_PLANE_CONFIG_DIR} | "${LABEL_YAML_CMD[@]}" >>"${EVENTING_KAFKA_SOURCE_ARTIFACT}"
  ko resolve ${KO_FLAGS} -Rf ${BROKER_DATA_PLANE_CONFIG_DIR} | "${LABEL_YAML_CMD[@]}" >>"${EVENTING_KAFKA_BROKER_ARTIFACT}"
  ko resolve ${KO_FLAGS} -Rf ${SINK_DATA_PLANE_CONFIG_DIR} | "${LABEL_YAML_CMD[@]}" >>"${EVENTING_KAFKA_SINK_ARTIFACT}"
  ko resolve ${KO_FLAGS} -Rf ${CHANNEL_DATA_PLANE_CONFIG_DIR} | "${LABEL_YAML_CMD[@]}" >>"${EVENTING_KAFKA_CHANNEL_ARTIFACT}"
  ko resolve ${KO_FLAGS} -Rf ${BROKER_TLS_CONFIG_DIR} \
    -Rf ${SINK_TLS_CONFIG_DIR} \
    -Rf ${CHANNEL_TLS_CONFIG_DIR} \
    | "${LABEL_YAML_CMD[@]}" >"${EVENTING_KAFKA_TLS_NETWORK_ARTIFACT}"

  replace_images "${EVENTING_KAFKA_SOURCE_ARTIFACT}" &&
    replace_images "${EVENTING_KAFKA_BROKER_ARTIFACT}" &&
    replace_images "${EVENTING_KAFKA_SINK_ARTIFACT}" &&
    replace_images "${EVENTING_KAFKA_CHANNEL_ARTIFACT}" &&
    return $?
}

function data_plane_unit_tests() {
  local optsstate mvn_output
  pushd ${DATA_PLANE_DIR} || return $?
  # store state of Bash options.
  optsstate="$(set +o)"
  set +eE
  ./mvnw clean verify \
    -Dmaven.wagon.http.retryHandler.count=6 \
    --no-transfer-progress
  mvn_output=$?
  # restore state of Bash options.
  set +vx; eval "$optsstate"

  echo "Copy test reports to ${ARTIFACTS}"
  # shellcheck disable=SC2038
  local report_filepath report_file
  while read -r report_filepath; do
    report_file="junit_$(basename "${report_filepath}")"
    cp "${report_filepath}" "${ARTIFACTS}/${report_file}"
  done < <(find . -type f -regextype posix-extended -regex ".*/TEST-.*.xml$")

  popd || return $?

  return $mvn_output
}

# Note: do not change this function name, it's used during releases.
function data_plane_setup() {
  data_plane_build_push && k8s
  return $?
}

function data_plane_source_setup() {
  pushd "${DATA_PLANE_DIR}" || return $?
  ./mvnw install -DskipTests || fail_test "failed to install data plane"
  dispatcher_build_push || dispatcher_build_push || fail_test "failed to build dispatcher"
  popd || return $?

  if [ "$KO_DOCKER_REPO" = "kind.local" ]; then
    kind load docker-image "${KNATIVE_KAFKA_DISPATCHER_IMAGE}"
  fi

  header "Creating artifacts"

  echo "Dispatcher image ---> ${KNATIVE_KAFKA_DISPATCHER_IMAGE}"

  ko resolve ${KO_FLAGS} -Rf ${SOURCE_DATA_PLANE_CONFIG_DIR} | "${LABEL_YAML_CMD[@]}" >>"${EVENTING_KAFKA_SOURCE_BUNDLE_ARTIFACT}"

  sed -i "s|\${KNATIVE_KAFKA_DISPATCHER_IMAGE}|${KNATIVE_KAFKA_DISPATCHER_IMAGE}|g" "${EVENTING_KAFKA_SOURCE_BUNDLE_ARTIFACT}"

  return $?
}
