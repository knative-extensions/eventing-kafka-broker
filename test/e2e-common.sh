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

readonly SKIP_INITIALIZE=${SKIP_INITIALIZE:-false}
readonly LOCAL_DEVELOPMENT=${LOCAL_DEVELOPMENT:-false}
export REPLICAS=${REPLICAS:-3}

source $(pwd)/vendor/knative.dev/hack/e2e-tests.sh
source $(pwd)/hack/data-plane.sh
source $(pwd)/hack/control-plane.sh
source $(pwd)/hack/artifacts-env.sh

# If gcloud is not available make it a no-op, not an error.
which gcloud &>/dev/null || gcloud() { echo "[ignore-gcloud $*]" 1>&2; }

# Use GNU tools on macOS. Requires the 'grep' and 'gnu-sed' Homebrew formulae.
if [ "$(uname)" == "Darwin" ]; then
  sed=gsed
  grep=ggrep
fi

# Latest release. If user does not supply this as a flag, the latest tagged release on the current branch will be used.
readonly LATEST_RELEASE_VERSION=$(latest_version)
readonly PREVIOUS_RELEASE_URL="${PREVIOUS_RELEASE_URL:-"https://github.com/knative-sandbox/eventing-kafka-broker/releases/download/${LATEST_RELEASE_VERSION}"}"

readonly EVENTING_CONFIG=${EVENTING_CONFIG:-"./third_party/eventing-latest/"}

# Vendored eventing test images.
readonly VENDOR_EVENTING_TEST_IMAGES="vendor/knative.dev/eventing/test/test_images/"

export MONITORING_ARTIFACTS_PATH="manifests/monitoring/prometheus-operator"
export EVENTING_KAFKA_CONTROLLER_PROMETHEUS_OPERATOR_ARTIFACT_PATH="${MONITORING_ARTIFACTS_PATH}/controller"
export EVENTING_KAFKA_WEBHOOK_PROMETHEUS_OPERATOR_ARTIFACT_PATH="${MONITORING_ARTIFACTS_PATH}/webhook"
export EVENTING_KAFKA_SOURCE_PROMETHEUS_OPERATOR_ARTIFACT_PATH="${MONITORING_ARTIFACTS_PATH}/source"
export EVENTING_KAFKA_BROKER_PROMETHEUS_OPERATOR_ARTIFACT_PATH="${MONITORING_ARTIFACTS_PATH}/broker"
export EVENTING_KAFKA_SINK_PROMETHEUS_OPERATOR_ARTIFACT_PATH="${MONITORING_ARTIFACTS_PATH}/sink"
export EVENTING_KAFKA_CHANNEL_PROMETHEUS_OPERATOR_ARTIFACT_PATH="${MONITORING_ARTIFACTS_PATH}/channel"

# The number of control plane replicas to run.
readonly REPLICAS=${REPLICAS:-1}

export SYSTEM_NAMESPACE="knative-eventing"
export CLUSTER_SUFFIX=${CLUSTER_SUFFIX:-"cluster.local"}

function knative_setup() {
  knative_eventing
  return $?
}

function knative_teardown() {
  if ! is_release_branch; then
    echo ">> Delete Knative Eventing from HEAD"
    pushd .
    cd eventing || fail_test "Failed to set up Eventing"
    kubectl delete --ignore-not-found -f "${EVENTING_CONFIG}"
    popd || fail_test "Failed to set up Eventing"
  else
    echo ">> Delete Knative Eventing from ${KNATIVE_EVENTING_RELEASE}"
    kubectl delete --ignore-not-found -f "${KNATIVE_EVENTING_RELEASE}"
  fi
}

function knative_eventing() {
  if ! is_release_branch; then
    echo ">> Install Knative Eventing from latest - ${EVENTING_CONFIG}"
    kubectl apply -f "${EVENTING_CONFIG}/eventing-crds.yaml"
    kubectl apply -f "${EVENTING_CONFIG}/eventing-core.yaml"
  else
    echo ">> Install Knative Eventing from ${KNATIVE_EVENTING_RELEASE}"
    kubectl apply -f "${KNATIVE_EVENTING_RELEASE}"
  fi

  ! kubectl patch horizontalpodautoscalers.autoscaling -n knative-eventing eventing-webhook -p '{"spec": {"minReplicas": '${REPLICAS}'}}'

  # Publish test images.
  echo ">> Publishing test images from eventing"
  ./test/upload-test-images.sh ${VENDOR_EVENTING_TEST_IMAGES} e2e || fail_test "Error uploading test images"

  # Publish test images from pkg.
  echo ">> Publishing test images from pkg"
  ./test/upload-test-images.sh "test/test_images" e2e || fail_test "Error uploading test images"

  kafka_setup
}

function kafka_setup() {
  echo ">> Prepare to deploy Strimzi"
  ./test/kafka/kafka_setup.sh || fail_test "Failed to set up Kafka cluster"
}

function build_components_from_source() {

  [ -f "${EVENTING_KAFKA_CONTROL_PLANE_ARTIFACT}" ] && rm "${EVENTING_KAFKA_CONTROL_PLANE_ARTIFACT}"
  [ -f "${EVENTING_KAFKA_SOURCE_ARTIFACT}" ] && rm "${EVENTING_KAFKA_SOURCE_ARTIFACT}"
  [ -f "${EVENTING_KAFKA_BROKER_ARTIFACT}" ] && rm "${EVENTING_KAFKA_BROKER_ARTIFACT}"
  [ -f "${EVENTING_KAFKA_SINK_ARTIFACT}" ] && rm "${EVENTING_KAFKA_SINK_ARTIFACT}"
  [ -f "${EVENTING_KAFKA_CHANNEL_ARTIFACT}" ] && rm "${EVENTING_KAFKA_CHANNEL_ARTIFACT}"
  [ -f "${EVENTING_KAFKA_POST_INSTALL_ARTIFACT}" ] && rm "${EVENTING_KAFKA_POST_INSTALL_ARTIFACT}"

  header "Data plane setup"
  data_plane_setup || fail_test "Failed to set up data plane components"

  header "Control plane setup"
  control_plane_setup || fail_test "Failed to set up control plane components"

  header "Building Monitoring artifacts"
  build_monitoring_artifacts || fail_test "Failed to create monitoring artifacts"

  return $?
}

function install_latest_release() {
  echo "Installing latest release from ${PREVIOUS_RELEASE_URL}"

  ko apply -f ./test/config/ || fail_test "Failed to apply test configurations"

  kubectl apply -f "${PREVIOUS_RELEASE_URL}/${EVENTING_KAFKA_CONTROL_PLANE_ARTIFACT}" || return $?
  kubectl apply -f "${PREVIOUS_RELEASE_URL}/${EVENTING_KAFKA_BROKER_ARTIFACT}" || return $?
  kubectl apply -f "${PREVIOUS_RELEASE_URL}/${EVENTING_KAFKA_SINK_ARTIFACT}" || return $?
  kubectl apply -f "${PREVIOUS_RELEASE_URL}/${EVENTING_KAFKA_SOURCE_ARTIFACT}" || return $?
  kubectl apply -f "${PREVIOUS_RELEASE_URL}/${EVENTING_KAFKA_CHANNEL_ARTIFACT}" || return $?

  # Restore test config-tracing.
  kubectl replace -f ./test/config/100-config-tracing.yaml
}

function install_head() {
  echo "Installing head"

  kubectl apply -f "${EVENTING_KAFKA_CONTROL_PLANE_ARTIFACT}" || return $?
  kubectl apply -f "${EVENTING_KAFKA_SOURCE_ARTIFACT}" || return $?
  kubectl apply -f "${EVENTING_KAFKA_BROKER_ARTIFACT}" || return $?
  kubectl apply -f "${EVENTING_KAFKA_SINK_ARTIFACT}" || return $?
  kubectl apply -f "${EVENTING_KAFKA_CHANNEL_ARTIFACT}" || return $?
  kubectl apply -f "${EVENTING_KAFKA_POST_INSTALL_ARTIFACT}" || return $?

  # Restore test config-tracing.
  kubectl replace -f ./test/config/100-config-tracing.yaml
}

function test_setup() {

  build_components_from_source || return $?

  install_head || return $?

  wait_until_pods_running knative-eventing || fail_test "System did not come up"

  # Apply test configurations, and restart data plane components (we don't have hot reload)
  ko apply -f ./test/config/ || fail_test "Failed to apply test configurations"

  setup_kafka_channel_auth || fail_test "Failed to apply channel auth configuration ${EVENTING_KAFKA_BROKER_CHANNEL_AUTH_SCENARIO}"

  kubectl rollout restart statefulset -n knative-eventing kafka-source-dispatcher

  kubectl rollout restart deployment -n knative-eventing kafka-broker-receiver
  kubectl rollout restart deployment -n knative-eventing kafka-broker-dispatcher
  kubectl rollout restart deployment -n knative-eventing kafka-sink-receiver
  kubectl rollout restart deployment -n knative-eventing kafka-channel-receiver
  kubectl rollout restart deployment -n knative-eventing kafka-channel-dispatcher
}

function test_teardown() {
  kubectl delete --ignore-not-found -f "${EVENTING_KAFKA_CONTROL_PLANE_ARTIFACT}" || fail_test "Failed to tear down control plane"
  kubectl delete --ignore-not-found -f "${EVENTING_KAFKA_BROKER_ARTIFACT}" || fail_test "Failed to tear down kafka broker"
  kubectl delete --ignore-not-found -f "${EVENTING_KAFKA_SINK_ARTIFACT}" || fail_test "Failed to tear down kafka sink"
  kubectl delete --ignore-not-found -f "${EVENTING_KAFKA_CHANNEL_ARTIFACT}" || fail_test "Failed to tear down kafka channel"
  kubectl delete --ignore-not-found -f "${EVENTING_KAFKA_SOURCE_ARTIFACT}" || fail_test "Failed to tear down kafka source"
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
  ko apply -f ./test/config/chaos || return $?
}

function delete_chaos() {
  kubectl delete --ignore-not-found -f ./test/config/chaos || return $?
}

function apply_sacura() {
  ko apply -f ./test/config/sacura/0-namespace.yaml || return $?
  ko apply -f ./test/config/sacura/100-broker-config.yaml || return $?
  ko apply -f ./test/config/sacura/101-broker.yaml || return $?

  kubectl wait --for=condition=ready --timeout=3m -n sacura broker/broker || {
    local ret=$?
    kubectl get -n sacura broker/broker -oyaml
    return ${ret}
  }

  ko apply -f ./test/config/sacura || return $?
}

function delete_sacura() {
  kubectl delete --ignore-not-found -f ./test/config/sacura/101-broker.yaml || return $?
  kubectl delete --ignore-not-found -f ./test/config/sacura/100-broker-config.yaml || return $?
  kubectl delete --ignore-not-found -f ./test/config/sacura/0-namespace.yaml || return $?
}

function export_logs_continuously() {

  labels=("kafka-broker-dispatcher" "kafka-broker-receiver" "kafka-sink-receiver" "kafka-channel-receiver" "kafka-channel-dispatcher" "kafka-source-dispatcher" "kafka-webhook-eventing" "kafka-controller")

  mkdir -p "$ARTIFACTS/${SYSTEM_NAMESPACE}"

  for deployment in "${labels[@]}"; do
    kubectl logs -n ${SYSTEM_NAMESPACE} -f -l=app="$deployment" >"$ARTIFACTS/${SYSTEM_NAMESPACE}/$deployment" 2>&1 &
  done
}

function save_release_artifacts() {
  # Copy our release artifacts into artifacts, so that release artifacts of a PR can be tested and reviewed without
  # building the project from source.
  cp "${EVENTING_KAFKA_BROKER_ARTIFACT}" "${ARTIFACTS}/${EVENTING_KAFKA_BROKER_ARTIFACT}" || return $?
  cp "${EVENTING_KAFKA_BROKER_PROMETHEUS_OPERATOR_ARTIFACT}" "${ARTIFACTS}/${EVENTING_KAFKA_BROKER_PROMETHEUS_OPERATOR_ARTIFACT}" || return $?

  cp "${EVENTING_KAFKA_SOURCE_ARTIFACT}" "${ARTIFACTS}/${EVENTING_KAFKA_SOURCE_ARTIFACT}" || return $?
  cp "${EVENTING_KAFKA_SOURCE_PROMETHEUS_OPERATOR_ARTIFACT}" "${ARTIFACTS}/${EVENTING_KAFKA_SOURCE_PROMETHEUS_OPERATOR_ARTIFACT}" || return $?

  cp "${EVENTING_KAFKA_SINK_ARTIFACT}" "${ARTIFACTS}/${EVENTING_KAFKA_SINK_ARTIFACT}" || return $?
  cp "${EVENTING_KAFKA_SINK_PROMETHEUS_OPERATOR_ARTIFACT}" "${ARTIFACTS}/${EVENTING_KAFKA_SINK_PROMETHEUS_OPERATOR_ARTIFACT}" || return $?

  cp "${EVENTING_KAFKA_CHANNEL_ARTIFACT}" "${ARTIFACTS}/${EVENTING_KAFKA_CHANNEL_ARTIFACT}" || return $?
  cp "${EVENTING_KAFKA_CHANNEL_PROMETHEUS_OPERATOR_ARTIFACT}" "${ARTIFACTS}/${EVENTING_KAFKA_CHANNEL_PROMETHEUS_OPERATOR_ARTIFACT}" || return $?

  cp "${EVENTING_KAFKA_CONTROL_PLANE_ARTIFACT}" "${ARTIFACTS}/${EVENTING_KAFKA_CONTROL_PLANE_ARTIFACT}" || return $?
  cp "${EVENTING_KAFKA_CONTROL_PLANE_PROMETHEUS_OPERATOR_ARTIFACT}" "${ARTIFACTS}/${EVENTING_KAFKA_CONTROL_PLANE_PROMETHEUS_OPERATOR_ARTIFACT}" || return $?
}

function build_monitoring_artifacts() {

  ko resolve ${KO_FLAGS} \
    -Rf "${EVENTING_KAFKA_CONTROLLER_PROMETHEUS_OPERATOR_ARTIFACT_PATH}" \
    -Rf "${EVENTING_KAFKA_WEBHOOK_PROMETHEUS_OPERATOR_ARTIFACT_PATH}" |
    "${LABEL_YAML_CMD[@]}" >"${EVENTING_KAFKA_CONTROL_PLANE_PROMETHEUS_OPERATOR_ARTIFACT}" || return $?

  ko resolve ${KO_FLAGS} -Rf "${EVENTING_KAFKA_BROKER_PROMETHEUS_OPERATOR_ARTIFACT_PATH}" |
    "${LABEL_YAML_CMD[@]}" >"${EVENTING_KAFKA_BROKER_PROMETHEUS_OPERATOR_ARTIFACT}" || return $?

  ko resolve ${KO_FLAGS} -Rf "${EVENTING_KAFKA_SOURCE_PROMETHEUS_OPERATOR_ARTIFACT_PATH}" |
    "${LABEL_YAML_CMD[@]}" >"${EVENTING_KAFKA_SOURCE_PROMETHEUS_OPERATOR_ARTIFACT}" || return $?

  ko resolve ${KO_FLAGS} -Rf "${EVENTING_KAFKA_SINK_PROMETHEUS_OPERATOR_ARTIFACT_PATH}" |
    "${LABEL_YAML_CMD[@]}" >"${EVENTING_KAFKA_SINK_PROMETHEUS_OPERATOR_ARTIFACT}" || return $?

  ko resolve ${KO_FLAGS} -Rf "${EVENTING_KAFKA_CHANNEL_PROMETHEUS_OPERATOR_ARTIFACT_PATH}" |
    "${LABEL_YAML_CMD[@]}" >"${EVENTING_KAFKA_CHANNEL_PROMETHEUS_OPERATOR_ARTIFACT}" || return $?
}

function setup_kafka_channel_auth() {
  echo "Apply channel auth config ${EVENTING_KAFKA_BROKER_CHANNEL_AUTH_SCENARIO}"

  if [ "$EVENTING_KAFKA_BROKER_CHANNEL_AUTH_SCENARIO" == "SSL" ]; then
    echo "Setting up SSL configuration for KafkaChannel"
    kubectl patch configmap/kafka-channel-config \
      -n knative-eventing \
      --type merge \
      -p '{"data":{"bootstrap.servers":"my-cluster-kafka-bootstrap.kafka:9093", "auth.secret.ref.name": "strimzi-tls-secret", "auth.secret.ref.namespace": "knative-eventing"}}'
  elif [ "$EVENTING_KAFKA_BROKER_CHANNEL_AUTH_SCENARIO" == "SASL_SSL" ]; then
    echo "Setting up SASL_SSL configuration for KafkaChannel"
    kubectl patch configmap/kafka-channel-config \
      -n knative-eventing \
      --type merge \
      -p '{"data":{"bootstrap.servers":"my-cluster-kafka-bootstrap.kafka:9094", "auth.secret.ref.name": "strimzi-sasl-secret", "auth.secret.ref.namespace": "knative-eventing"}}'
  elif [ "$EVENTING_KAFKA_BROKER_CHANNEL_AUTH_SCENARIO" == "SASL_PLAIN" ]; then
    echo "Setting up SASL_PLAIN configuration for KafkaChannel"
    kubectl patch configmap/kafka-channel-config \
      -n knative-eventing \
      --type merge \
      -p '{"data":{"bootstrap.servers":"my-cluster-kafka-bootstrap.kafka:9095", "auth.secret.ref.name": "strimzi-sasl-plain-secret", "auth.secret.ref.namespace": "knative-eventing"}}'
  else
    echo "Setting up no auth configuration for KafkaChannel"
    kubectl patch configmap/kafka-channel-config \
      -n knative-eventing \
      --type merge \
      -p '{"data":{"bootstrap.servers":"my-cluster-kafka-bootstrap.kafka:9092"}}'
    kubectl patch configmap/kafka-channel-config \
      -n knative-eventing \
      --type=json \
      -p='[{"op": "remove", "path": "/data/auth.secret.ref.name"}, {"op": "remove", "path": "/data/auth.secret.ref.namespace"}]' || true
  fi
}
