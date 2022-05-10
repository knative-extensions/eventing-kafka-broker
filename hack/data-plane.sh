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
# - SKIP_PUSH (default: false) --> images will not be pushed to remote registry, nor to kind local registry

source "$(pwd)"/hack/label.sh

readonly SKIP_PUSH=${SKIP_PUSH:-false}

readonly DATA_PLANE_DIR=data-plane
readonly DATA_PLANE_CONFIG_DIR=${DATA_PLANE_DIR}/config

# Source config
readonly SOURCE_DATA_PLANE_CONFIG_DIR=${DATA_PLANE_CONFIG_DIR}/source
# Temporary Sourcev2 config
readonly SOURCEV2_DATA_PLANE_CONFIG_DIR=${DATA_PLANE_CONFIG_DIR}/sourcev2
# Broker config
readonly BROKER_DATA_PLANE_CONFIG_DIR=${DATA_PLANE_CONFIG_DIR}/broker
# Sink config
readonly SINK_DATA_PLANE_CONFIG_DIR=${DATA_PLANE_CONFIG_DIR}/sink
# Channel config
readonly CHANNEL_DATA_PLANE_CONFIG_DIR=${DATA_PLANE_CONFIG_DIR}/channel

# The BASE_IMAGE must have system libraries (libc, zlib, etc) compatible with the JAVA_IMAGE because
# Jlink generates a jdk linked to the same system libraries available on the base images.
readonly BASE_IMAGE=${BASE_IMAGE:-"gcr.io/distroless/java-debian11:base-nonroot"} # Based on debian:buster
readonly JAVA_IMAGE=${JAVA_IMAGE:-"docker.io/eclipse-temurin:17-jdk-centos7"}     # Based on centos7

readonly RECEIVER_JAR="receiver-1.0-SNAPSHOT.jar"
readonly RECEIVER_DIRECTORY=receiver

readonly DISPATCHER_JAR="dispatcher-1.0-SNAPSHOT.jar"
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

function image_push() {
  if ${SKIP_PUSH}; then
    return
  fi
  if [ "$KO_DOCKER_REPO" = "kind.local" ]; then
    kind load docker-image "$1"
  else
    docker push "$1"
  fi
}

function receiver_build_push() {
  header "Building receiver ..."

  readonly receiver="${KNATIVE_KAFKA_BROKER_RECEIVER:-${KO_DOCKER_REPO}/knative-kafka-broker-receiver}"

  local digest
  digest=$(docker build \
    -q \
    -f ${DATA_PLANE_DIR}/docker/Dockerfile \
    --build-arg JAVA_IMAGE="${JAVA_IMAGE}" \
    --build-arg BASE_IMAGE="${BASE_IMAGE}" \
    --build-arg APP_JAR=${RECEIVER_JAR} \
    --build-arg APP_DIR=${RECEIVER_DIRECTORY} \
    -t "${receiver}:latest" ${DATA_PLANE_DIR})

  header "Pushing receiver image, digest: $digest"

  # Remove sha256 prefix from digest "sha256:..." since that's not legal
  # for pushing the image.
  local tag=${digest/sha256/""}

  # Export variable that identifies the image.
  #
  # We cannot reference the image by digest since re-tagging
  # the image with docker or podman will produce a different
  # digest.
  export KNATIVE_KAFKA_RECEIVER_IMAGE="$receiver$tag"

  # Tag the built image with the latest tag with the tag based on the digest.
  docker tag "${receiver}:latest" "${KNATIVE_KAFKA_RECEIVER_IMAGE}" &&
    image_push "${KNATIVE_KAFKA_RECEIVER_IMAGE}" &&
    # Remove the tagged image after pushing it to avoid having
    # many images during local development.
    docker image rm "${KNATIVE_KAFKA_RECEIVER_IMAGE}"

  return $?
}

function dispatcher_build_push() {
  header "Building dispatcher ..."

  readonly dispatcher="${KNATIVE_KAFKA_BROKER_DISPATCHER:-${KO_DOCKER_REPO}/knative-kafka-broker-dispatcher}"

  local digest
  digest=$(docker build \
    -q \
    -f ${DATA_PLANE_DIR}/docker/Dockerfile \
    --build-arg JAVA_IMAGE=${JAVA_IMAGE} \
    --build-arg BASE_IMAGE=${BASE_IMAGE} \
    --build-arg APP_JAR=${DISPATCHER_JAR} \
    --build-arg APP_DIR=${DISPATCHER_DIRECTORY} \
    -t "${dispatcher}:latest" ${DATA_PLANE_DIR})

  header "Pushing dispatcher image digest: $digest"

  # Remove sha256 prefix from digest "sha256:..." since that's not legal
  # for pushing the image.
  local tag=${digest/sha256/""}

  # Export variable that identifies the image.
  #
  # We cannot reference the image by digest since re-tagging
  # the image with docker or podman will produce a different
  # digest.
  export KNATIVE_KAFKA_DISPATCHER_IMAGE="$dispatcher$tag"

  # Tag the built image with the latest tag with the tag based on the digest.
  docker tag "${dispatcher}:latest" "${KNATIVE_KAFKA_DISPATCHER_IMAGE}" &&
    image_push "${KNATIVE_KAFKA_DISPATCHER_IMAGE}" &&
    # Remove the tagged image after pushing it to avoid having
    # many images during local development.
    docker image rm "${KNATIVE_KAFKA_DISPATCHER_IMAGE}"

  return $?
}

function data_plane_build_push() {
  receiver_build_push || receiver_build_push || fail_test "failed to build receiver"
  dispatcher_build_push || dispatcher_build_push || fail_test "failed to build dispatcher"
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
  pushd ${DATA_PLANE_DIR} || fail_test
  ./mvnw clean verify -Dmaven.wagon.http.retryHandler.count=6 --no-transfer-progress
  mvn_output=$?

  echo "Copy test reports in ${ARTIFACTS}"
  find . -type f -regextype posix-extended -regex ".*/TEST-.*.xml$" | xargs -I '{}' cp {} ${ARTIFACTS}/
  pushd ${ARTIFACTS} || fail_test
  for f in *; do
    mv -- "$f" "junit_$f"
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

function data_plane_sourcev2_setup() {
  dispatcher_build_push || dispatcher_build_push || fail_test "failed to build dispatcher"
  header "Creating artifacts"

  echo "Dispatcher image ---> ${KNATIVE_KAFKA_DISPATCHER_IMAGE}"

  ko resolve ${KO_FLAGS} -Rf ${SOURCEV2_DATA_PLANE_CONFIG_DIR} | "${LABEL_YAML_CMD[@]}" >>"${EVENTING_KAFKA_SOURCE_BUNDLE_ARTIFACT}"

  sed -i "s|\${KNATIVE_KAFKA_DISPATCHER_IMAGE}|${KNATIVE_KAFKA_DISPATCHER_IMAGE}|g" "${EVENTING_KAFKA_SOURCE_BUNDLE_ARTIFACT}"

  return $?
}
