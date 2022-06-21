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

source "$(pwd)"/hack/label.sh

readonly DATA_PLANE_DIR=data-plane
readonly DATA_PLANE_CONFIG_DIR=${DATA_PLANE_DIR}/config

readonly SOURCE_DATA_PLANE_CONFIG_DIR=${DATA_PLANE_CONFIG_DIR}/source
# Broker config
readonly BROKER_DATA_PLANE_CONFIG_DIR=${DATA_PLANE_CONFIG_DIR}/broker
readonly SINK_DATA_PLANE_CONFIG_DIR=${DATA_PLANE_CONFIG_DIR}/sink
readonly CHANNEL_DATA_PLANE_CONFIG_DIR=${DATA_PLANE_CONFIG_DIR}/channel

readonly RECEIVER_DIRECTORY=receiver
readonly DISPATCHER_DIRECTORY=dispatcher

# Checks whether the given function exists.
function function_exists() {
  [[ "$(type -t $1)" == "function" ]]
}

if ! function_exists header; then
  function header() {
    echo "$@"
  }
fi

# Use GNU tools on macOS. Requires the 'grep' and 'gnu-sed' Homebrew formulae.
if [ "$(uname)" == "Darwin" ]; then
  sed=gsed
  grep=ggrep
fi

function receiver_build_push() {
  header "Building receiver ..."

  local receiver="${KNATIVE_KAFKA_BROKER_RECEIVER:-${KO_DOCKER_REPO}/knative-kafka-broker-receiver}"
  export KNATIVE_KAFKA_RECEIVER_IMAGE="${receiver}:${TAG}"

  ./mvnw package jib:build -pl ${RECEIVER_DIRECTORY} -DskipTests || return $?
}

function dispatcher_build_push() {
  header "Building dispatcher ..."

  local dispatcher="${KNATIVE_KAFKA_BROKER_DISPATCHER:-${KO_DOCKER_REPO}/knative-kafka-broker-dispatcher}"
  export KNATIVE_KAFKA_DISPATCHER_IMAGE="${dispatcher}:${TAG}"

  ./mvnw package jib:build -pl "${DISPATCHER_DIRECTORY}" -DskipTests || return $?
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

  replace_images "${EVENTING_KAFKA_SOURCE_ARTIFACT}" &&
    replace_images "${EVENTING_KAFKA_BROKER_ARTIFACT}" &&
    replace_images "${EVENTING_KAFKA_SINK_ARTIFACT}" &&
    replace_images "${EVENTING_KAFKA_CHANNEL_ARTIFACT}" &&
    return $?
}

function data_plane_unit_tests() {
  pushd ${DATA_PLANE_DIR} || return $?
  ./mvnw clean verify -Dmaven.wagon.http.retryHandler.count=6 --no-transfer-progress
  mvn_output=$?

  echo "Copy test reports in ${ARTIFACTS}"
  # shellcheck disable=SC2038
  find . -type f -regextype posix-extended -regex ".*/TEST-.*.xml$" | xargs -I '{}' cp {} "${ARTIFACTS}/junit_"{}

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
