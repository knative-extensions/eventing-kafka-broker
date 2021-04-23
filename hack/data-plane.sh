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
# - UUID (default: latest)
# - SKIP_PUSH (default: false) --> images will not be pushed to remote registry
# - WITH_KIND (default: false) --> images will be loaded in KinD

source $(pwd)/hack/label.sh

readonly WITH_KIND=${WITH_KIND:-false}
readonly SKIP_PUSH=${SKIP_PUSH:-false}
readonly UUID=${UUID:-${TAG:-latest}}

readonly DATA_PLANE_DIR=data-plane
readonly DATA_PLANE_CONFIG_DIR=${DATA_PLANE_DIR}/config

# Broker config
readonly BROKER_DATA_PLANE_CONFIG_DIR=${DATA_PLANE_CONFIG_DIR}/broker
readonly KAFKA_BROKER_DATA_PLANE_CONFIG_TEMPLATE_DIR=${BROKER_DATA_PLANE_CONFIG_DIR}/template # no trailing slash
readonly KAFKA_BROKER_DISPATCHER_TEMPLATE_FILE=${KAFKA_BROKER_DATA_PLANE_CONFIG_TEMPLATE_DIR}/500-dispatcher.yaml
readonly KAFKA_BROKER_RECEIVER_TEMPLATE_FILE=${KAFKA_BROKER_DATA_PLANE_CONFIG_TEMPLATE_DIR}/500-receiver.yaml

# Sink config
readonly SINK_DATA_PLANE_CONFIG_DIR=${DATA_PLANE_CONFIG_DIR}/sink
readonly KAFKA_SINK_DATA_PLANE_CONFIG_TEMPLATE_DIR=${SINK_DATA_PLANE_CONFIG_DIR}/template
readonly KAFKA_SINK_RECEIVER_TEMPLATE_FILE=${KAFKA_SINK_DATA_PLANE_CONFIG_TEMPLATE_DIR}/500-receiver.yaml

readonly receiver="${KNATIVE_KAFKA_BROKER_RECEIVER:-knative-kafka-broker-receiver}"
readonly dispatcher="${KNATIVE_KAFKA_BROKER_DISPATCHER:-knative-kafka-broker-dispatcher}"
readonly control_protocol_conformance_server="${KNATIVE_KAFKA_BROKER_DISPATCHER:-control-protocol-conformance-java-server}"

# The BASE_IMAGE must have system libraries (libc, zlib, etc) compatible with the JAVA_IMAGE because
# Jlink generates a jdk linked to the same system libraries available on the base images.
readonly BASE_IMAGE=gcr.io/distroless/java-debian10:base-nonroot # Based on debian:buster
readonly JAVA_IMAGE=docker.io/adoptopenjdk/openjdk15:debian      # Based on debian:buster

readonly RECEIVER_JAR="receiver-1.0-SNAPSHOT.jar"
readonly RECEIVER_DIRECTORY=receiver

readonly DISPATCHER_JAR="dispatcher-1.0-SNAPSHOT.jar"
readonly DISPATCHER_DIRECTORY=dispatcher

readonly CONTROL_PROTOCOL_CONFORMANCE_SERVER_JAR="control-protocol-conformance-server-1.0-SNAPSHOT.jar"
readonly CONTROL_PROTOCOL_CONFORMANCE_SERVER_DIRECTORY=control-protocol-conformance-server

# Checks whether the given function exists.
function function_exists() {
  [[ "$(type -t $1)" == "function" ]]
}

if ! function_exists header; then
  function header() {
    echo "$@"
  }
fi

function docker_push() {
  if ! ${SKIP_PUSH}; then
    docker push "$1"
  fi
}

function with_kind() {
  if ${WITH_KIND}; then
    kind load docker-image "$1"
  fi
}

function receiver_build_push() {
  header "Building receiver ..."

  docker build \
    -f ${DATA_PLANE_DIR}/docker/Dockerfile \
    --build-arg JAVA_IMAGE=${JAVA_IMAGE} \
    --build-arg BASE_IMAGE=${BASE_IMAGE} \
    --build-arg APP_JAR=${RECEIVER_JAR} \
    --build-arg APP_DIR=${RECEIVER_DIRECTORY} \
    -t "${KNATIVE_KAFKA_BROKER_RECEIVER_IMAGE}" ${DATA_PLANE_DIR} &&
    docker_push "${KNATIVE_KAFKA_BROKER_RECEIVER_IMAGE}" &&
    with_kind "${KNATIVE_KAFKA_BROKER_RECEIVER_IMAGE}"

  return $?
}

function dispatcher_build_push() {
  header "Building dispatcher ..."

  docker build \
    -f ${DATA_PLANE_DIR}/docker/Dockerfile \
    --build-arg JAVA_IMAGE=${JAVA_IMAGE} \
    --build-arg BASE_IMAGE=${BASE_IMAGE} \
    --build-arg APP_JAR=${DISPATCHER_JAR} \
    --build-arg APP_DIR=${DISPATCHER_DIRECTORY} \
    -t "${KNATIVE_KAFKA_BROKER_DISPATCHER_IMAGE}" ${DATA_PLANE_DIR} &&
    docker_push "${KNATIVE_KAFKA_BROKER_DISPATCHER_IMAGE}" &&
    with_kind "${KNATIVE_KAFKA_BROKER_DISPATCHER_IMAGE}"

  return $?
}

function control_protocol_conformance_server_build_push() {
  local uuid=${UUID}
  if [ "${uuid}" = "latest" ]; then
    uuid="$(uuidgen --time)"
  fi

  export KNATIVE_CONTROL_PROTOCOL_CONFORMANCE_SERVER_IMAGE="${KO_DOCKER_REPO}"/"${control_protocol_conformance_server}":"${uuid}"

  header "Building control protocol conformance server ..."

  docker build \
    -f ${DATA_PLANE_DIR}/docker/Dockerfile \
    -f ${DATA_PLANE_DIR}/docker/Dockerfile \
    --build-arg JAVA_IMAGE=${JAVA_IMAGE} \
    --build-arg BASE_IMAGE=${BASE_IMAGE} \
    --build-arg APP_JAR=${CONTROL_PROTOCOL_CONFORMANCE_SERVER_JAR} \
    --build-arg APP_DIR=${CONTROL_PROTOCOL_CONFORMANCE_SERVER_DIRECTORY} \
    -t "${KNATIVE_CONTROL_PROTOCOL_CONFORMANCE_SERVER_IMAGE}" ${DATA_PLANE_DIR} &&
    docker_push "${KNATIVE_CONTROL_PROTOCOL_CONFORMANCE_SERVER_IMAGE}" &&
    with_kind "${KNATIVE_CONTROL_PROTOCOL_CONFORMANCE_SERVER_IMAGE}"

  return $?
}

function data_plane_build_push() {

  local uuid=${UUID}
  if [ "${uuid}" = "latest" ]; then
    uuid="$(uuidgen --time)"
  fi

  export KNATIVE_KAFKA_BROKER_RECEIVER_IMAGE="${KO_DOCKER_REPO}"/"${receiver}":"${uuid}"
  export KNATIVE_KAFKA_BROKER_DISPATCHER_IMAGE="${KO_DOCKER_REPO}"/"${dispatcher}":"${uuid}"
  export KNATIVE_KAFKA_SINK_RECEIVER_IMAGE="${KO_DOCKER_REPO}"/"${receiver}":"${uuid}"

  receiver_build_push || receiver_build_push || fail_test "failed to build receiver"
  dispatcher_build_push || dispatcher_build_push || fail_test "failed to build dispatcher"
}

function k8s() {
  echo "dispatcher image ---> ${KNATIVE_KAFKA_BROKER_DISPATCHER_IMAGE}"
  echo "receiver image   ---> ${KNATIVE_KAFKA_BROKER_RECEIVER_IMAGE}"
  echo "receiver image   ---> ${KNATIVE_KAFKA_SINK_RECEIVER_IMAGE}"

  ko resolve ${KO_FLAGS} -f ${BROKER_DATA_PLANE_CONFIG_DIR} | "${LABEL_YAML_CMD[@]}" >>"${EVENTING_KAFKA_BROKER_ARTIFACT}"
  ko resolve ${KO_FLAGS} -f ${SINK_DATA_PLANE_CONFIG_DIR} | "${LABEL_YAML_CMD[@]}" >>"${EVENTING_KAFKA_SINK_ARTIFACT}"

  sed "s|\${KNATIVE_KAFKA_BROKER_DISPATCHER_IMAGE}|${KNATIVE_KAFKA_BROKER_DISPATCHER_IMAGE}|g" ${KAFKA_BROKER_DISPATCHER_TEMPLATE_FILE} |
    "${LABEL_YAML_CMD[@]}" >>"${EVENTING_KAFKA_BROKER_ARTIFACT}" || fail_test "Failed to append ${KAFKA_BROKER_DISPATCHER_TEMPLATE_FILE}"

  sed "s|\${KNATIVE_KAFKA_BROKER_RECEIVER_IMAGE}|${KNATIVE_KAFKA_BROKER_RECEIVER_IMAGE}|g" ${KAFKA_BROKER_RECEIVER_TEMPLATE_FILE} |
    "${LABEL_YAML_CMD[@]}" >>"${EVENTING_KAFKA_BROKER_ARTIFACT}" || fail_test "Failed to append ${KAFKA_BROKER_RECEIVER_TEMPLATE_FILE}"

  sed "s|\${KNATIVE_KAFKA_SINK_RECEIVER_IMAGE}|${KNATIVE_KAFKA_SINK_RECEIVER_IMAGE}|g" ${KAFKA_SINK_RECEIVER_TEMPLATE_FILE} |
    "${LABEL_YAML_CMD[@]}" >>"${EVENTING_KAFKA_SINK_ARTIFACT}" || fail_test "Failed to append ${KAFKA_SINK_RECEIVER_TEMPLATE_FILE}"
}

function data_plane_unit_tests() {
  pushd ${DATA_PLANE_DIR} || fail_test
  mvn clean verify -Dhttp.keepAlive=false -Dmaven.wagon.http.pool=false -Dmaven.wagon.http.retryHandler.count=6 --no-transfer-progress
  mvn_output=$?

  echo "Copy test reports in ${ARTIFACTS}"
  find . -type f -regextype posix-extended -regex ".*/TEST-.*.xml$" | xargs -I '{}' cp {} ${ARTIFACTS}/
  pushd ${ARTIFACTS} || fail_test
  for f in * ; do
    mv -- "$f" "junit_$f" ;
  done
  popd || fail_test
  popd || fail_test

  return $mvn_output
}

# Note: do not change this function name, it's used during releases.
function data_plane_setup() {
  data_plane_build_push && k8s
  return $?
}
