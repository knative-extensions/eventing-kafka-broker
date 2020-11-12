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
# - SKIP_INITIALIZE (default: false) - skip cluster creation

source $(pwd)/vendor/knative.dev/hack/e2e-tests.sh
source $(pwd)/test/data-plane/library.sh
source $(pwd)/test/control-plane/library.sh

SKIP_INITIALIZE=${SKIP_INITIALIZE:-false}

readonly EVENTING_CONFIG="./config"

# Vendored eventing test iamges.
readonly VENDOR_EVENTING_TEST_IMAGES="vendor/knative.dev/eventing/test/test_images/"

readonly CHAOS_CONFIG="test/pkg/config/chaos/chaosduck.yaml"
# Vendored pkg test iamges.
readonly VENDOR_PKG_TEST_IMAGES="vendor/knative.dev/pkg/leaderelection/chaosduck"

export EVENTING_KAFKA_CONTROL_PLANE_ARTIFACT="eventing-kafka-controller.yaml"
export EVENTING_KAFKA_BROKER_ARTIFACT="eventing-kafka-broker.yaml"
export EVENTING_KAFKA_SINK_ARTIFACT="eventing-kafka-sink.yaml"

# The number of control plane replicas to run.
readonly REPLICAS=${REPLICAS:-3}

export SYSTEM_NAMESPACE="knative-eventing"
export CLUSTER_SUFFIX=${CLUSTER_SUFFIX:-"cluster.local"}

function knative_setup() {
  knative_eventing "apply --strict"
  return $?
}

function knative_teardown() {
  if ! is_release_branch; then
    echo ">> Install Knative Eventing from HEAD"
    pushd .
    cd eventing || fail_test "Failed to set up Eventing"
    ko delete --ignore-not-found -f "${EVENTING_CONFIG}"
    popd || fail_test "Failed to set up Eventing"
  else
    echo ">> Install Knative Eventing from ${KNATIVE_EVENTING_RELEASE}"
    kubectl delete --ignore-not-found -f ${KNATIVE_EVENTING_RELEASE}
  fi
}

function knative_eventing() {
  if ! is_release_branch; then
    echo ">> Install Knative Eventing from HEAD"
    pushd .
    cd "${GOPATH}" && mkdir -p src/knative.dev && cd src/knative.dev || fail_test "Failed to set up Eventing"
    git clone https://github.com/knative/eventing
    cd eventing || fail_test "Failed to set up Eventing"
    ko apply --strict -f "${EVENTING_CONFIG}"
    popd || fail_test "Failed to set up Eventing"
  else
    echo ">> Install Knative Eventing from ${KNATIVE_EVENTING_RELEASE}"
    kubectl apply -f ${KNATIVE_EVENTING_RELEASE}
  fi

  # Publish test images.
  echo ">> Publishing test images from eventing"
  # We vendor test image code from eventing, in order to use ko to resolve them into Docker images, the
  # path has to be a GOPATH.
  sed -i 's@knative.dev/eventing/test/test_images@knative.dev/eventing-kafka-broker/vendor/knative.dev/eventing/test/test_images@g' "${VENDOR_EVENTING_TEST_IMAGES}"*/*.yaml
  ./test/upload-test-images.sh ${VENDOR_EVENTING_TEST_IMAGES} e2e || fail_test "Error uploading test images"
  sed -i 's@knative.dev/eventing-kafka-broker/vendor/knative.dev/eventing/test/test_images@knative.dev/eventing/test/test_images@g' "${VENDOR_EVENTING_TEST_IMAGES}"*/*.yaml

  # Publish test images from pkg.
  echo ">> Publishing test images from pkg"
  # We vendor test image code from pkg, in order to use ko to resolve them into Docker images, the
  # path has to be a GOPATH.
  sed -i 's@knative.dev/pkg/leaderelection/chaosduck@knative.dev/eventing-kafka-broker/vendor/knative.dev/pkg/leaderelection/chaosduck@g' "${CHAOS_CONFIG}"
  ./test/upload-test-images.sh ${VENDOR_PKG_TEST_IMAGES} e2e || fail_test "Error uploading test images"
  sed -i 's@knative.dev/eventing-kafka-broker/vendor/knative.dev/pkg/leaderelection/chaosduck@knative.dev/pkg/leaderelection/chaosduck@g' "${CHAOS_CONFIG}"

  ./test/upload-test-images.sh "test/test_images" e2e || fail_test "Error uploading test images"

  ./test/kafka/kafka_setup.sh || fail_test "Failed to set up Kafka cluster"
}

function test_setup() {

  [ -f "${EVENTING_KAFKA_CONTROL_PLANE_ARTIFACT}" ] && rm "${EVENTING_KAFKA_CONTROL_PLANE_ARTIFACT}"
  [ -f "${EVENTING_KAFKA_BROKER_ARTIFACT}" ] && rm "${EVENTING_KAFKA_BROKER_ARTIFACT}"
  [ -f "${EVENTING_KAFKA_SINK_ARTIFACT}" ] && rm "${EVENTING_KAFKA_SINK_ARTIFACT}"

  header "Data plane setup"
  data_plane_setup || fail_test "Failed to set up data plane components"

  header "Control plane setup"
  control_plane_setup || fail_test "Failed to set up control plane components"

  kubectl apply -f "${EVENTING_KAFKA_CONTROL_PLANE_ARTIFACT}" || fail_test "Failed to apply ${EVENTING_KAFKA_CONTROL_PLANE_ARTIFACT}"
  wait_until_pods_running knative-eventing || fail_test "Control plane did not come up"

  kubectl apply -f "${EVENTING_KAFKA_BROKER_ARTIFACT}" || fail_test "Failed to apply ${EVENTING_KAFKA_BROKER_ARTIFACT}"
  wait_until_pods_running knative-eventing || fail_test "Broker data plane did not come up"

  kubectl apply -f "${EVENTING_KAFKA_SINK_ARTIFACT}" || fail_test "Failed to apply ${EVENTING_KAFKA_SINK_ARTIFACT}"
  wait_until_pods_running knative-eventing || fail_test "Sink data plane did not come up"

  # Apply test configurations, and restart data plane components (we don't have hot reload)
  ko apply -f ./test/pkg/config/ || fail_test "Failed to apply test configurations"

  kubectl rollout restart deployment -n knative-eventing kafka-broker-receiver
  kubectl rollout restart deployment -n knative-eventing kafka-broker-dispatcher
  kubectl rollout restart deployment -n knative-eventing kafka-sink-receiver
}

function test_teardown() {
  kubectl delete -f "${EVENTING_KAFKA_CONTROL_PLANE_ARTIFACT}" || fail_test "Failed to tear down control plane"
  kubectl delete -f "${EVENTING_KAFKA_BROKER_ARTIFACT}" || fail_test "Failed to tear down kafka broker"
  kubectl delete -f "${EVENTING_KAFKA_SINK_ARTIFACT}" || fail_test "Failed to tear down kafka sink"
}

function scale_controlplane() {
  for deployment in "$@"; do
    # Make sure all pods run in leader-elected mode.
    kubectl -n knative-eventing scale deployment "$deployment" --replicas=0 || fail_test "Failed to scale down to 0 ${deployment}"
    # Give it time to kill the pods.
    sleep 5
    # Scale up components for HA tests
    kubectl -n knative-eventing scale deployment "$deployment" --replicas="${REPLICAS}" || fail_test "Failed to scale up to ${REPLICAS} ${deployment}"
  done
}

function apply_chaos() {
  ko apply -f ./test/pkg/config/chaos || return $?
}

function apply_sacura() {
  ko apply --strict -f ./test/pkg/config/sacura/0-namespace.yaml || return $?
  ko apply --strict -f ./test/pkg/config/sacura/101-broker.yaml || return $?

  kubectl wait --for=condition=ready --timeout=3m -n sacura broker/broker || return $?

  ko apply --strict -f ./test/pkg/config/sacura || return $?
}
