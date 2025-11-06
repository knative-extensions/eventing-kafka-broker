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
export KO_FLAGS="${KO_FLAGS:-}"
export TAG="$(git rev-parse HEAD)"

repo_root_dir=$(dirname "$(realpath "${BASH_SOURCE[0]}")")/..

source "${repo_root_dir}"/vendor/knative.dev/hack/e2e-tests.sh
source "${repo_root_dir}"/hack/data-plane.sh
source "${repo_root_dir}"/hack/control-plane.sh
source "${repo_root_dir}"/hack/artifacts-env.sh

# If gcloud is not available make it a no-op, not an error.
which gcloud &>/dev/null || gcloud() { echo "[ignore-gcloud $*]" 1>&2; }

# Use GNU tools on macOS. Requires the 'grep' Homebrew formulae.
if [ "$(uname)" == "Darwin" ]; then
  grep=ggrep
fi

# Latest release. If user does not supply this as a flag, the latest tagged release on the current branch will be used.
readonly LATEST_RELEASE_VERSION=$(latest_version)
readonly PREVIOUS_RELEASE_URL="${PREVIOUS_RELEASE_URL:-"https://github.com/knative-extensions/eventing-kafka-broker/releases/download/${LATEST_RELEASE_VERSION}"}"

readonly EVENTING_CONFIG=${EVENTING_CONFIG:-"./third_party/eventing-latest/"}
readonly CERTMANAGER_CONFIG=${CERTMANAGER_CONFIG:-"./third_party/cert-manager"}

# Vendored eventing test images.
readonly VENDOR_EVENTING_TEST_IMAGES="vendor/knative.dev/eventing/test/test_images/"

export MONITORING_ARTIFACTS_PATH="manifests/monitoring/prometheus-operator"
export EVENTING_KAFKA_CONTROLLER_PROMETHEUS_OPERATOR_ARTIFACT_PATH="${MONITORING_ARTIFACTS_PATH}/controller"
export EVENTING_KAFKA_CONTROLLER_SOURCE_PROMETHEUS_OPERATOR_ARTIFACT_PATH="${MONITORING_ARTIFACTS_PATH}/controller-source"
export EVENTING_KAFKA_WEBHOOK_PROMETHEUS_OPERATOR_ARTIFACT_PATH="${MONITORING_ARTIFACTS_PATH}/webhook"
export EVENTING_KAFKA_SOURCE_PROMETHEUS_OPERATOR_ARTIFACT_PATH="${MONITORING_ARTIFACTS_PATH}/source"
export EVENTING_KAFKA_BROKER_PROMETHEUS_OPERATOR_ARTIFACT_PATH="${MONITORING_ARTIFACTS_PATH}/broker"
export EVENTING_KAFKA_SINK_PROMETHEUS_OPERATOR_ARTIFACT_PATH="${MONITORING_ARTIFACTS_PATH}/sink"
export EVENTING_KAFKA_CHANNEL_PROMETHEUS_OPERATOR_ARTIFACT_PATH="${MONITORING_ARTIFACTS_PATH}/channel"

# The number of control plane replicas to run.
readonly REPLICAS=${REPLICAS:-1}

# Whether to turn chaosduck off (via deployment scaling).
# This is mainly used by the test automation via prow.
readonly SCALE_CHAOSDUCK_TO_ZERO="${SCALE_CHAOSDUCK_TO_ZERO:-0}"

export BROKER_TEMPLATES="${repo_root_dir}"/test/e2e_new/templates/kafka-broker
export CHANNEL_GROUP_KIND=KafkaChannel.messaging.knative.dev
export CHANNEL_VERSION=v1beta1

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
    kubectl delete --ignore-not-found -f "${EVENTING_CONFIG}"
    popd || fail_test "Failed to set up Eventing"
  else
    echo ">> Delete Knative Eventing from ${KNATIVE_EVENTING_RELEASE}"
    kubectl delete --ignore-not-found -f "${KNATIVE_EVENTING_RELEASE}"
  fi
}

function install_eventing_core() {
  if ! is_release_branch; then
    echo ">> Install Knative Eventing from latest - ${EVENTING_CONFIG}"
    kubectl apply -f "${EVENTING_CONFIG}/eventing-crds.yaml"
    kubectl apply -f "${EVENTING_CONFIG}/eventing-core.yaml"
    kubectl apply -f "${EVENTING_CONFIG}/eventing-tls-networking.yaml"
  else
    echo ">> Install Knative Eventing from ${KNATIVE_EVENTING_RELEASE} and ${KNATIVE_EVENTING_RELEASE_TLS}"
    kubectl apply -f "${KNATIVE_EVENTING_RELEASE}"
    kubectl apply -f "${KNATIVE_EVENTING_RELEASE_TLS}"
  fi

  install_eventing_core_test_tls_resources || return $?

  kubectl patch horizontalpodautoscalers.autoscaling -n knative-eventing eventing-webhook -p '{"spec": {"minReplicas": '${REPLICAS}'}}'
}

function knative_eventing() {
  # we need cert-manager installed to be able to create the issuers
  kubectl apply -f "${CERTMANAGER_CONFIG}/00-namespace.yaml"

  timeout 600 bash -c "until kubectl apply -f ${CERTMANAGER_CONFIG}/01-cert-manager.yaml; do sleep 5; done"
  wait_until_pods_running "cert-manager" || fail_test "Failed to install cert manager"

  timeout 600 bash -c "until kubectl apply -f ${CERTMANAGER_CONFIG}/02-trust-manager.yaml; do sleep 5; done"
  wait_until_pods_running "cert-manager" || fail_test "Failed to install trust manager"

  install_eventing_core

  # Publish test images.
  echo ">> Publishing test images from eventing"
  ./test/upload-test-images.sh ${VENDOR_EVENTING_TEST_IMAGES} e2e || fail_test "Error uploading test images"

  # Publish test images from pkg.
  echo ">> Publishing test images from pkg"
  ./test/upload-test-images.sh "test/test_images" e2e || fail_test "Error uploading test images"

  kafka_setup
  keda_setup
}

function kafka_setup() {
  echo ">> Prepare to deploy Strimzi"
  ./test/kafka/kafka_setup.sh || fail_test "Failed to set up Kafka cluster"
}

function keda_setup() {
  echo ">> Prepare to deploy KEDA"
  ./test/keda/keda_setup.sh || fail_test "Failed to set up KEDA"
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

function build_control_plane_from_source() {

  # Remove existing control plane artifacts if they exist
  [ -f "${EVENTING_KAFKA_CONTROL_PLANE_ARTIFACT}" ] && rm "${EVENTING_KAFKA_CONTROL_PLANE_ARTIFACT}"
  [ -f "${EVENTING_KAFKA_POST_INSTALL_ARTIFACT}" ] && rm "${EVENTING_KAFKA_POST_INSTALL_ARTIFACT}"

  header "Control plane setup"
  control_plane_setup || fail_test "Failed to set up control plane components"

  header "Building Monitoring artifacts"
  build_monitoring_artifacts || fail_test "Failed to create monitoring artifacts"

  return $?
}

function build_source_components_from_source() {

  [ -f "${EVENTING_KAFKA_SOURCE_BUNDLE_ARTIFACT}" ] && rm "${EVENTING_KAFKA_SOURCE_BUNDLE_ARTIFACT}"
  [ -f "${EVENTING_KAFKA_POST_INSTALL_ARTIFACT}" ] && rm "${EVENTING_KAFKA_POST_INSTALL_ARTIFACT}"

  header "Data plane source setup"
  data_plane_source_setup || fail_test "Failed to set up source data plane dispatcher"

  header "Control plane source setup"
  control_plane_source_setup || fail_test "Failed to set up source control plane components"

  header "Building source Monitoring artifacts"
  build_monitoring_artifacts_source || fail_test "Failed to create monitoring artifacts"

  return $?
}

function install_latest_release() {
  echo "Installing previous eventing core release"

  eventing_version=$(echo ${LATEST_RELEASE_VERSION} | sed 's/knative-v\(.*\)/\1/')

  # Hack - need to find a way to find the patch version for eventing core, for now default to 0
  start_release_knative_eventing "$(major_version ${eventing_version}).$(minor_version ${eventing_version}).0"

  echo "Installing latest release from ${PREVIOUS_RELEASE_URL}"

  ko apply ${KO_FLAGS} -f ./test/config/ || fail_test "Failed to apply test configurations"

  kubectl apply -f "${PREVIOUS_RELEASE_URL}/${EVENTING_KAFKA_CONTROL_PLANE_ARTIFACT}" || return $?
  # The next apply needs the webhook rules to be populated to properly validate/mutate the resources
  kubectl wait --for=jsonpath='{.webhooks[0].rules[0]}' mutatingwebhookconfiguration/pods.defaulting.webhook.kafka.eventing.knative.dev --timeout=60s
  kubectl apply -f "${PREVIOUS_RELEASE_URL}/${EVENTING_KAFKA_BROKER_ARTIFACT}" || return $?
  kubectl apply -f "${PREVIOUS_RELEASE_URL}/${EVENTING_KAFKA_SINK_ARTIFACT}" || return $?
  kubectl apply -f "${PREVIOUS_RELEASE_URL}/${EVENTING_KAFKA_SOURCE_ARTIFACT}" || return $?
  kubectl apply -f "${PREVIOUS_RELEASE_URL}/${EVENTING_KAFKA_CHANNEL_ARTIFACT}" || return $?

  # Restore test config.
  kubectl apply -f ./test/config/100-config-kafka-features.yaml
}

function install_head() {
  echo "Installing head"

  kubectl apply -f "${EVENTING_KAFKA_CONTROL_PLANE_ARTIFACT}" || return $?
  # The next apply needs the webhook rules to be populated to properly validate/mutate the resources
  kubectl wait --for=jsonpath='{.webhooks[0].rules[0]}' mutatingwebhookconfiguration/pods.defaulting.webhook.kafka.eventing.knative.dev --timeout=60s
  kubectl apply -f "${EVENTING_KAFKA_SOURCE_ARTIFACT}" || return $?
  kubectl apply -f "${EVENTING_KAFKA_BROKER_ARTIFACT}" || return $?
  kubectl apply -f "${EVENTING_KAFKA_SINK_ARTIFACT}" || return $?
  kubectl apply -f "${EVENTING_KAFKA_CHANNEL_ARTIFACT}" || return $?
  kubectl apply -f "${EVENTING_KAFKA_TLS_NETWORK_ARTIFACT}" || return $?
  kubectl apply -f "${EVENTING_KAFKA_POST_INSTALL_ARTIFACT}" || return $?

  # Restore test config.
  kubectl apply -f ./test/config/100-config-kafka-features.yaml
}

function install_control_plane_from_source() {
  echo "Installing control plane from source"

  kubectl apply -f "${EVENTING_KAFKA_CONTROL_PLANE_ARTIFACT}" || return $?
  kubectl apply -f "${EVENTING_KAFKA_POST_INSTALL_ARTIFACT}" || return $?
  kubectl apply -f "${EVENTING_KAFKA_TLS_NETWORK_ARTIFACT}" || return $?

  # Restore test config.
  kubectl replace -f ./test/config/100-config-kafka-features.yaml

}

function install_latest_release_source() {
  echo "Installing latest release from ${PREVIOUS_RELEASE_URL}"

  ko apply ${KO_FLAGS} -f ./test/config/ || fail_test "Failed to apply test configurations"

  kubectl apply -f "${PREVIOUS_RELEASE_URL}/${EVENTING_KAFKA_SOURCE_BUNDLE_ARTIFACT}" || return $?

  # Restore test config.
  kubectl apply -f ./test/config/100-config-kafka-features.yaml
}

function install_head_source() {
  echo "Installing head"

  kubectl apply -f "${EVENTING_KAFKA_SOURCE_BUNDLE_ARTIFACT}" || return $?
  kubectl apply -f "${EVENTING_KAFKA_POST_INSTALL_ARTIFACT}" || return $?

  # Restore test config.
  kubectl apply -f ./test/config/100-config-kafka-features.yaml
}

function test_setup() {

  build_components_from_source || return $?

  install_head || return $?

  wait_until_pods_running knative-eventing || fail_test "System did not come up"

  # Apply test configurations, and restart data plane components (we don't have hot reload)
  ko apply ${KO_FLAGS} -f ./test/config/ || fail_test "Failed to apply test configurations"

  create_sasl_secrets || fail_test "Failed to create SASL secrets"
  create_tls_secrets || fail_test "Failed to create TLS secrets"
  setup_kafka_channel_auth || fail_test "Failed to apply channel auth configuration ${EVENTING_KAFKA_BROKER_CHANNEL_AUTH_SCENARIO}"

  kubectl rollout restart statefulset -n knative-eventing kafka-source-dispatcher
  kubectl rollout restart deployment -n knative-eventing kafka-broker-receiver
  kubectl rollout restart statefulset -n knative-eventing kafka-broker-dispatcher
  kubectl rollout restart deployment -n knative-eventing kafka-sink-receiver
  kubectl rollout restart deployment -n knative-eventing kafka-channel-receiver
  kubectl rollout restart statefulset -n knative-eventing kafka-channel-dispatcher
}

function test_teardown() {
  kubectl delete --ignore-not-found -f "${EVENTING_KAFKA_CONTROL_PLANE_ARTIFACT}" || fail_test "Failed to tear down control plane"
  kubectl delete --ignore-not-found -f "${EVENTING_KAFKA_BROKER_ARTIFACT}" || fail_test "Failed to tear down kafka broker"
  kubectl delete --ignore-not-found -f "${EVENTING_KAFKA_SINK_ARTIFACT}" || fail_test "Failed to tear down kafka sink"
  kubectl delete --ignore-not-found -f "${EVENTING_KAFKA_CHANNEL_ARTIFACT}" || fail_test "Failed to tear down kafka channel"
  kubectl delete --ignore-not-found -f "${EVENTING_KAFKA_SOURCE_ARTIFACT}" || fail_test "Failed to tear down kafka source"
}

function test_source_setup() {

  build_source_components_from_source || return $?

  install_head_source || return $?

  wait_until_pods_running knative-eventing || fail_test "System did not come up"

  kubectl rollout restart statefulset -n knative-eventing kafka-source-dispatcher
}

function test_source_teardown() {
  kubectl delete --ignore-not-found -f "${EVENTING_KAFKA_SOURCE_BUNDLE_ARTIFACT}" || fail_test "Failed to tear down kafka source"
}

function scale_controlplane() {
  for deployment in "$@"; do
    # Make sure all pods run in leader-elected mode.
    kubectl -n knative-eventing scale deployment "$deployment" --replicas=0 || fail_test "Failed to scale down to 0 ${deployment}"
    # Give it time to kill the pods.
    sleep 5
    # Scale up components for HA tests
    kubectl -n knative-eventing scale deployment "$deployment" --replicas="${REPLICAS}" || fail_test "Failed to scale up to ${REPLICAS} ${deployment}"
    # Wait for the deployment to be available
    kubectl -n knative-eventing wait deployment "$deployment" --for condition=Available=True --timeout=600s || fail_test "Failed to scale up to ${REPLICAS} ${deployment}"
  done
}

function apply_chaos() {
  ko apply ${KO_FLAGS} -f ./test/config/chaos || return $?
  if (( SCALE_CHAOSDUCK_TO_ZERO )); then
    echo "Scaling chaosduck replicas down to zero"
    kubectl -n knative-eventing scale deployment/chaosduck --replicas=0
  fi
}

function delete_chaos() {
  kubectl delete --ignore-not-found -f ./test/config/chaos || return $?
}

function apply_sacura() {
  kubectl apply -Rf ./test/config/sacura/resources || return $?

  kubectl wait --for=condition=ready --timeout=3m -n sacura broker/broker || {
    kubectl describe -n sacura broker/broker
    return $?
  }
  kubectl wait --for=condition=ready --timeout=3m -n sacura trigger/trigger || {
    kubectl describe -n sacura trigger/trigger
    return $?
  }

  kubectl apply -Rf ./test/config/sacura || return $?
}

function apply_sacura_sink_source() {
  kubectl apply -Rf ./test/config/sacura-sink-source/resources || return $?

  kubectl wait --for=condition=ready --timeout=3m -n sacura-sink-source kafkasink/sink || {
    kubectl describe -n sacura-sink-source kafkasink/sink
    return $?
  }
  kubectl wait --for=condition=ready --timeout=3m -n sacura-sink-source kafkasource/source || {
    kubectl describe -n sacura-sink-source kafkasource/source
    return $?
  }

  kubectl apply -Rf ./test/config/sacura-sink-source || return $?
}

function delete_sacura() {
  kubectl delete --ignore-not-found ns sacura || return $?
  kubectl delete --ignore-not-found ns sacura-sink-source || return $?
}

function export_logs_continuously() {

  labels=("kafka-broker-dispatcher" "kafka-broker-receiver" "kafka-sink-receiver" "kafka-channel-receiver" "kafka-channel-dispatcher" "kafka-source-dispatcher" "kafka-webhook-eventing" "kafka-controller" "kafka-source-controller")

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
    LABEL_YAML_CMD >"${EVENTING_KAFKA_CONTROL_PLANE_PROMETHEUS_OPERATOR_ARTIFACT}" || return $?

  ko resolve ${KO_FLAGS} -Rf "${EVENTING_KAFKA_BROKER_PROMETHEUS_OPERATOR_ARTIFACT_PATH}" |
    LABEL_YAML_CMD >"${EVENTING_KAFKA_BROKER_PROMETHEUS_OPERATOR_ARTIFACT}" || return $?

  ko resolve ${KO_FLAGS} -Rf "${EVENTING_KAFKA_SOURCE_PROMETHEUS_OPERATOR_ARTIFACT_PATH}" |
    LABEL_YAML_CMD >"${EVENTING_KAFKA_SOURCE_PROMETHEUS_OPERATOR_ARTIFACT}" || return $?

  ko resolve ${KO_FLAGS} -Rf "${EVENTING_KAFKA_SINK_PROMETHEUS_OPERATOR_ARTIFACT_PATH}" |
    LABEL_YAML_CMD >"${EVENTING_KAFKA_SINK_PROMETHEUS_OPERATOR_ARTIFACT}" || return $?

  ko resolve ${KO_FLAGS} -Rf "${EVENTING_KAFKA_CHANNEL_PROMETHEUS_OPERATOR_ARTIFACT_PATH}" |
    LABEL_YAML_CMD >"${EVENTING_KAFKA_CHANNEL_PROMETHEUS_OPERATOR_ARTIFACT}" || return $?
}

function build_monitoring_artifacts_source() {

  ko resolve ${KO_FLAGS} \
    -Rf "${EVENTING_KAFKA_CONTROLLER_SOURCE_PROMETHEUS_OPERATOR_ARTIFACT_PATH}" \
    -Rf "${EVENTING_KAFKA_WEBHOOK_PROMETHEUS_OPERATOR_ARTIFACT_PATH}" |
    LABEL_YAML_CMD >"${EVENTING_KAFKA_CONTROL_PLANE_PROMETHEUS_OPERATOR_ARTIFACT}" || return $?

  ko resolve ${KO_FLAGS} -Rf "${EVENTING_KAFKA_SOURCE_PROMETHEUS_OPERATOR_ARTIFACT_PATH}" |
    LABEL_YAML_CMD >"${EVENTING_KAFKA_SOURCE_PROMETHEUS_OPERATOR_ARTIFACT}" || return $?
}

function create_tls_secrets() {
  ca_cert_secret="my-cluster-cluster-ca-cert"
  tls_user="my-tls-user"

  echo "Waiting until secrets: ${ca_cert_secret}, ${tls_user} exist"
  wait_until_object_exists secret ${ca_cert_secret} kafka
  wait_until_object_exists secret ${tls_user} kafka

  echo "Creating TLS Kafka secret"

  STRIMZI_CRT=$(kubectl -n kafka get secret "${ca_cert_secret}" --template='{{index .data "ca.crt"}}' | base64 --decode )
  TLSUSER_CRT=$(kubectl -n kafka get secret "${tls_user}" --template='{{index .data "user.crt"}}' | base64 --decode )
  TLSUSER_KEY=$(kubectl -n kafka get secret "${tls_user}" --template='{{index .data "user.key"}}' | base64 --decode )

  kubectl create secret --namespace "${SYSTEM_NAMESPACE}" generic strimzi-tls-secret \
    --from-literal=ca.crt="$STRIMZI_CRT" \
    --from-literal=user.crt="$TLSUSER_CRT" \
    --from-literal=user.key="$TLSUSER_KEY" \
    --from-literal=protocol="SSL" \
    --dry-run=client -o yaml | kubectl apply -n "${SYSTEM_NAMESPACE}" -f -
}

function create_sasl_secrets() {
  ca_cert_secret="my-cluster-cluster-ca-cert"
  sasl_user="my-sasl-user"

  echo "Waiting until secrets: ${ca_cert_secret}, ${sasl_user} exist"
  wait_until_object_exists secret ${ca_cert_secret} kafka
  wait_until_object_exists secret ${sasl_user} kafka

  echo "Creating SASL_SSL and SASL_PLAINTEXT Kafka secrets"

  STRIMZI_CRT=$(kubectl -n kafka get secret ${ca_cert_secret} --template='{{index .data "ca.crt"}}' | base64 --decode )
  SASL_PASSWD=$(kubectl -n kafka get secret ${sasl_user} --template='{{index .data "password"}}' | base64 --decode )

  kubectl create secret --namespace "${SYSTEM_NAMESPACE}" generic strimzi-sasl-secret \
    --from-literal=ca.crt="$STRIMZI_CRT" \
    --from-literal=password="$SASL_PASSWD" \
    --from-literal=user="my-sasl-user" \
    --from-literal=protocol="SASL_SSL" \
    --from-literal=sasl.mechanism="SCRAM-SHA-512" \
    --dry-run=client -o yaml | kubectl apply -n "${SYSTEM_NAMESPACE}" -f -

  kubectl create secret --namespace "${SYSTEM_NAMESPACE}" generic strimzi-sasl-secret-legacy \
    --from-literal=ca.crt="$STRIMZI_CRT" \
    --from-literal=password="$SASL_PASSWD" \
    --from-literal=user="my-sasl-user" \
    --from-literal=saslType="SCRAM-SHA-512" \
    --dry-run=client -o yaml | kubectl apply -n "${SYSTEM_NAMESPACE}" -f -

  kubectl create secret --namespace "${SYSTEM_NAMESPACE}" generic strimzi-sasl-plain-secret \
    --from-literal=password="$SASL_PASSWD" \
    --from-literal=user="my-sasl-user" \
    --from-literal=protocol="SASL_PLAINTEXT" \
    --from-literal=sasl.mechanism="SCRAM-SHA-512" \
    --dry-run=client -o yaml | kubectl apply -n "${SYSTEM_NAMESPACE}" -f -

  kubectl create secret --namespace "${SYSTEM_NAMESPACE}" generic strimzi-sasl-plain-secret-legacy \
    --from-literal=password="$SASL_PASSWD" \
    --from-literal=username="my-sasl-user" \
    --from-literal=saslType="SCRAM-SHA-512" \
    --dry-run=client -o yaml | kubectl apply -n "${SYSTEM_NAMESPACE}" -f -
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
      -p '{"data":{"bootstrap.servers":"my-cluster-kafka-bootstrap.kafka:9094", "auth.secret.ref.name": "strimzi-sasl-secret-legacy", "auth.secret.ref.namespace": "knative-eventing"}}'
  elif [ "$EVENTING_KAFKA_BROKER_CHANNEL_AUTH_SCENARIO" == "SASL_PLAIN" ]; then
    echo "Setting up SASL_PLAIN configuration for KafkaChannel"
    kubectl patch configmap/kafka-channel-config \
      -n knative-eventing \
      --type merge \
      -p '{"data":{"bootstrap.servers":"my-cluster-kafka-bootstrap.kafka:9095", "auth.secret.ref.name": "strimzi-sasl-plain-secret-legacy", "auth.secret.ref.namespace": "knative-eventing"}}'
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

function install_eventing_core_test_tls_resources() {
    ko apply -Rf "${repo_root_dir}/vendor/knative.dev/eventing/test/config/tls" || return $?
}

function mount_knative_eventing_bundle() {
  echo "Mounting knative-eventing-bundle ConfigMap to data-plane components"

  for receiver in kafka-broker-receiver kafka-channel-receiver kafka-sink-receiver
  do
    kubectl get deployment -n knative-eventing "$receiver" -o json | \
      jq '.spec.template.spec.volumes |= . + [{"name":"knative-eventing-bundle","configMap":{"defaultMode":420,"name":"knative-eventing-bundle"}}]' | \
      jq '(.spec.template.spec.containers[] | select(.name=="'"$receiver"'") | .volumeMounts)  |= . + [{"mountPath":"/etc/knative-eventing-bundle/knative-eventing-bundle.jks","name":"knative-eventing-bundle","readOnly":true,"subPath":"knative-eventing-bundle.jks"}]' | \
      jq '(.spec.template.spec.containers[] | select(.name=="'"$receiver"'") | .env[] | select(.name=="JAVA_TOOL_OPTIONS") | .value) |= . + " -Djavax.net.ssl.trustStore=/etc/knative-eventing-bundle/knative-eventing-bundle.jks"' | \
      kubectl apply -f -
  done

  for dispatcher in kafka-broker-dispatcher kafka-channel-dispatcher kafka-source-dispatcher
  do
    kubectl get statefulset -n knative-eventing "$dispatcher" -o json | \
      jq '.spec.template.spec.volumes |= . + [{"name":"knative-eventing-bundle","configMap":{"defaultMode":420,"name":"knative-eventing-bundle"}}]' | \
      jq '(.spec.template.spec.containers[] | select(.name=="'"$dispatcher"'") | .volumeMounts)  |= . + [{"mountPath":"/etc/knative-eventing-bundle/knative-eventing-bundle.jks","name":"knative-eventing-bundle","readOnly":true,"subPath":"knative-eventing-bundle.jks"}]' | \
      jq '(.spec.template.spec.containers[] | select(.name=="'"$dispatcher"'") | .env[] | select(.name=="JAVA_TOOL_OPTIONS") | .value) |= . + " -Djavax.net.ssl.trustStore=/etc/knative-eventing-bundle/knative-eventing-bundle.jks"' | \
      kubectl apply -f -
  done

  echo "Mounting knative-eventing-bundle ConfigMap to kafka-controller"

  kubectl get deployment -n knative-eventing kafka-controller -o json | \
    jq '.spec.template.spec.volumes |= . + [{"name":"knative-eventing-bundle","configMap":{"defaultMode":420,"name":"knative-eventing-bundle"}}]' | \
    jq '(.spec.template.spec.containers[] | select(.name=="controller") | .volumeMounts) |= . + [{"mountPath":"/etc/knative-eventing-bundle/knative-eventing-bundle.pem","name":"knative-eventing-bundle","readOnly":true,"subPath":"knative-eventing-bundle.pem"}]' | \
    jq '(.spec.template.spec.containers[] | select(.name=="controller") | .env) |= . + [{"name":"SSL_CERT_DIR","value":"/etc/knative-eventing-bundle:/etc/ssl/certs"}]' | \
    kubectl apply -f -
}

function unmount_knative_eventing_bundle() {
  echo "Unmounting knative-eventing-bundle ConfigMap from data-plane components"

  for receiver in kafka-broker-receiver kafka-channel-receiver kafka-sink-receiver
  do
    kubectl get deployment -n knative-eventing "$receiver" -o json | \
      jq '(.spec.template.spec.volumes[] | select(.name=="knative-eventing-bundle")) |= empty' | \
      jq '(.spec.template.spec.containers[] | select(.name=="'"$receiver"'") | .volumeMounts[] | select(.name=="knative-eventing-bundle")) |= empty' | \
      jq '(.spec.template.spec.containers[] | select(.name=="'"$receiver"'") | .env[] | select(.name=="JAVA_TOOL_OPTIONS") | .value) |= (. | sub(" -Djavax.net.ssl.trustStore=/etc/knative-eventing-bundle/knative-eventing-bundle.jks";""))' | \
      kubectl apply -f -
  done

  for dispatcher in kafka-broker-dispatcher kafka-channel-dispatcher kafka-source-dispatcher
  do
    kubectl get statefulset -n knative-eventing "$dispatcher" -o json | \
      jq '(.spec.template.spec.volumes[] | select(.name=="knative-eventing-bundle")) |= empty' | \
      jq '(.spec.template.spec.containers[] | select(.name=="'"$dispatcher"'") | .volumeMounts[] | select(.name=="knative-eventing-bundle")) |= empty' | \
      jq '(.spec.template.spec.containers[] | select(.name=="'"$dispatcher"'") | .env[] | select(.name=="JAVA_TOOL_OPTIONS") | .value) |= (. | sub(" -Djavax.net.ssl.trustStore=/etc/knative-eventing-bundle/knative-eventing-bundle.jks";""))' | \
      kubectl apply -f -
  done

  echo "Unmounting knative-eventing-bundle ConfigMap from kafka-controller"

  kubectl get deployment -n knative-eventing kafka-controller -o json | \
    jq '(.spec.template.spec.volumes[] | select(.name=="knative-eventing-bundle")) |= empty' | \
    jq '(.spec.template.spec.containers[] | select(.name=="controller") | .volumeMounts[] | select(.name=="knative-eventing-bundle")) |= empty' | \
    jq '(.spec.template.spec.containers[] | select(.name=="controller") | .env[] | select(.name=="SSL_CERT_DIR")) |= empty' | \
    kubectl apply -f -
}
