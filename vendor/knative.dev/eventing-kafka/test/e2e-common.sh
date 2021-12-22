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

# This script runs the end-to-end tests against eventing-contrib built
# from source.

# If you already have the *_OVERRIDE environment variables set, call
# this script with the --run-tests arguments and it will use the cluster
# and run the tests.
# Note that local clusters often do not have the resources to run 12 parallel
# tests (the default) as the tests each tend to create their own namespaces and
# dispatchers.  For example, a local Docker cluster with 4 CPUs and 8 GB RAM will
# probably be able to handle 6 at maximum.  Be sure to adequately set the
# MAX_PARALLEL_TESTS variable before running this script, with the caveat that
# lowering it too much might make the tests run over the timeout that
# the go_test_e2e commands are using below.

# This script includes common functions for testing setup and teardown.

TEST_PARALLEL=${MAX_PARALLEL_TESTS:-12}

source "$(dirname "$(dirname "${BASH_SOURCE[0]}")")/vendor/knative.dev/hack/e2e-tests.sh"

# If gcloud is not available make it a no-op, not an error.
which gcloud &> /dev/null || gcloud() { echo "[ignore-gcloud $*]" 1>&2; }

# Use GNU Tools on MacOS (Requires the 'grep' and 'gnu-sed' Homebrew formulae)
if [ "$(uname)" == "Darwin" ]; then
  sed=gsed
  grep=ggrep
fi

# Eventing main config path from HEAD.
readonly EVENTING_KAFKA_REPO="${EVENTING_KAFKA_REPO:-https://github.com/knative-sandbox/eventing-kafka}"
readonly EVENTING_CONFIG="./config/"
readonly EVENTING_MT_CHANNEL_BROKER_CONFIG="./config/brokers/mt-channel-broker"
readonly EVENTING_IN_MEMORY_CHANNEL_CONFIG="./config/channels/in-memory-channel"

# Vendored Eventing Test Images.
readonly VENDOR_EVENTING_TEST_IMAGES="vendor/knative.dev/eventing/test/test_images/"
# HEAD eventing test images.
readonly HEAD_EVENTING_TEST_IMAGES="${GOPATH}/src/knative.dev/eventing/test/test_images/"

# Config tracing config.
readonly CONFIG_TRACING_CONFIG="test/config/config-tracing.yaml"

# Strimzi Kafka Cluster Brokers URL (base64 encoded value for k8s secret)
readonly STRIMZI_KAFKA_NAMESPACE="kafka" # Installation Namespace
readonly STRIMZI_KAFKA_CLUSTER_BROKERS="my-cluster-kafka-bootstrap.kafka.svc:9092"

# Eventing Kafka main config path from HEAD.
readonly KAFKA_CRD_CONFIG_TEMPLATE_DIR="./config/channel"
readonly DISTRIBUTED_TEMPLATE_DIR="${KAFKA_CRD_CONFIG_TEMPLATE_DIR}/distributed"
readonly CONSOLIDATED_TEMPLATE_DIR="${KAFKA_CRD_CONFIG_TEMPLATE_DIR}/consolidated"

# Eventing Kafka Channel CRD Secret (Will be modified with Strimzi Cluster Brokers - No Authentication)
readonly EVENTING_KAFKA_SECRET_TEMPLATE="300-kafka-secret.yaml"

# Eventing Kafka Channel CRD Config Map (Will be modified with SASL/TLS disabled)
readonly EVENTING_KAFKA_CONFIG_TEMPLATE="300-eventing-kafka-configmap.yaml"

# Strimzi installation config template used for starting up Kafka clusters.
readonly STRIMZI_INSTALLATION_CONFIG_TEMPLATE="test/config/100-strimzi-cluster-operator-0.26.1.yaml"
# Strimzi installation config.
readonly STRIMZI_INSTALLATION_CONFIG="$(mktemp "${ARTIFACTS}/strimzi-XXXXX.yaml")"
# Kafka cluster CR config file.
readonly KAFKA_INSTALLATION_CONFIG="test/config/100-kafka-ephemeral-triple-3.0.0.yaml"
# Kafka TLS ConfigMap.
readonly KAFKA_TLS_CONFIG="test/config/config-kafka-tls.yaml"
# Kafka SASL ConfigMap.
readonly KAFKA_SASL_CONFIG="test/config/config-kafka-sasl.yaml"
# Kafka Users CR config file.
readonly KAFKA_USERS_CONFIG="test/config/100-strimzi-users.yaml"
# Kafka PLAIN cluster URL
readonly KAFKA_PLAIN_CLUSTER_URL="my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092"
# Kafka TLS cluster URL
readonly KAFKA_TLS_CLUSTER_URL="my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9093"
# Kafka SASL cluster URL
readonly KAFKA_SASL_CLUSTER_URL="my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9094"
# Kafka cluster URL for our installation, during tests
KAFKA_CLUSTER_URL=${KAFKA_PLAIN_CLUSTER_URL}
# Kafka channel CRD config template file. It needs to be modified to be the real config file.
readonly KAFKA_CRD_CONFIG_TEMPLATE="400-kafka-config.yaml"

# Real Kafka channel CRD config, generated from the template directory and
# modified template file.
readonly KAFKA_CRD_CONFIG_DIR="$(mktemp -d "${ARTIFACTS}/channel-crd-XXXXX")"
# Real Kafka Source CRD config, generated from the template directory and
# modified template file.
readonly KAFKA_SOURCE_CRD_CONFIG_DIR="$(mktemp -d "${ARTIFACTS}/source-crd-XXXXX")"
# A target directory where post install scripts are gets created.
readonly KAFKA_POST_INSTALL_DIR="$(mktemp -d "${ARTIFACTS}/post-install-XXXXX")"
readonly KAFKA_POST_INSTALL_TEMPLATE_DIR="config/post-install"

# Kafka ST and MT Source CRD config template directory
readonly KAFKA_SOURCE_TEMPLATE_DIR="config/source/single"
readonly KAFKA_MT_SOURCE_TEMPLATE_DIR="config/source/multi"

# Namespaces where we install Eventing components
# This is the namespace of knative-eventing itself
export EVENTING_NAMESPACE="knative-eventing"

# Namespace where we install eventing-kafka components (may be different than EVENTING_NAMESPACE)
readonly SYSTEM_NAMESPACE="knative-eventing"
export SYSTEM_NAMESPACE

# Zipkin setup
readonly KNATIVE_EVENTING_MONITORING_YAML="test/config/monitoring.yaml"

# Latest release. If user does not supply this as a flag, the latest
# tagged release on the current branch will be used.
LATEST_RELEASE_VERSION="${LATEST_RELEASE_VERSION:-$(latest_version)}"

#
# TODO - Consider adding this function to the test-infra library.sh utilities ?
#
# Add The kn-eventing-test-pull-secret To Specified ServiceAccount & Restart Pods
#
# If the default namespace contains a Secret named 'kn-eventing-test-pull-secret',
# then copy it into the specified Namespace and add it to the specified ServiceAccount,
# and restart the specified Pods.
#
# This utility function exists to support local cluster testing with a private Docker
# repository, and is based on the CopySecret() functionality in eventing/pkg/utils.
#
function add_kn_eventing_test_pull_secret() {

  # Local Constants
  local secret="kn-eventing-test-pull-secret"

  # Get The Function Arguments
  local namespace="$1"
  local account="$2"
  local deployment="$3"

  # If The Eventing Test Pull Secret Is Present & The Namespace Was Specified
  if [[ $(kubectl get secret $secret -n default --ignore-not-found --no-headers=true | wc -l) -eq 1 && -n "$namespace" ]]; then

      # If The Secret Is Not Already In The Specified Namespace Then Copy It In
      if [[ $(kubectl get secret $secret -n "$namespace" --ignore-not-found --no-headers=true | wc -l) -lt 1 ]]; then
        kubectl get secret $secret -n default -o yaml | sed "s/namespace: default/namespace: $namespace/" | kubectl create -f -
      fi

      # If Specified Then Patch The ServiceAccount To Include The Image Pull Secret
      if [[ -n "$account" ]]; then
        kubectl patch serviceaccount -n "$namespace" "$account" -p "{\"imagePullSecrets\": [{\"name\": \"$secret\"}]}"
      fi

      # If Specified Then Restart The Pods Of The Deployment
      if [[ -n "$deployment" ]]; then
        kubectl rollout restart -n "$namespace" deployment "$deployment"
      fi
  fi
}

function knative_setup() {
  install_knative_eventing
}

function install_knative_eventing {
  if is_release_branch; then
    echo ">> Install Knative Eventing from ${KNATIVE_EVENTING_RELEASE}"
    kubectl apply -f ${KNATIVE_EVENTING_RELEASE}
  else
    echo ">> Install Knative Eventing from HEAD"
    pushd .
    cd ${GOPATH} && mkdir -p src/knative.dev && cd src/knative.dev
    git clone https://github.com/knative/eventing
    cd eventing
    ko apply -f "${EVENTING_CONFIG}"
    # Install MT Channel Based Broker
    ko apply -f "${EVENTING_MT_CHANNEL_BROKER_CONFIG}"
    # Install IMC
    ko apply -Rf "${EVENTING_IN_MEMORY_CHANNEL_CONFIG}"
    popd
  fi
   wait_until_pods_running "${EVENTING_NAMESPACE}" || fail_test "Knative Eventing did not come up"

  install_zipkin
}

# Setup zipkin
function install_zipkin() {
  echo "Installing Zipkin..."
  sed "s/\${SYSTEM_NAMESPACE}/${SYSTEM_NAMESPACE}/g" < "${KNATIVE_EVENTING_MONITORING_YAML}" | kubectl apply -f -
  wait_until_pods_running "${SYSTEM_NAMESPACE}" || fail_test "Zipkin inside eventing did not come up"
  # Setup config tracing for tracing tests
  sed "s/\${SYSTEM_NAMESPACE}/${SYSTEM_NAMESPACE}/g" <  "${CONFIG_TRACING_CONFIG}" | kubectl apply -f -
}

# Remove zipkin
function uninstall_zipkin() {
  echo "Uninstalling Zipkin..."
  sed "s/\${SYSTEM_NAMESPACE}/${SYSTEM_NAMESPACE}/g" <  "${KNATIVE_EVENTING_MONITORING_YAML}" | kubectl delete -f -
  wait_until_object_does_not_exist deployment zipkin "${SYSTEM_NAMESPACE}" || fail_test "Zipkin deployment was unable to be deleted"
  kubectl delete -n "${SYSTEM_NAMESPACE}" configmap config-tracing
}

function knative_teardown() {
  echo ">> Stopping Knative Eventing"
  if is_release_branch; then
    echo ">> Uninstalling Knative Eventing from ${KNATIVE_EVENTING_RELEASE}"
    kubectl delete -f "${KNATIVE_EVENTING_RELEASE}"
  else
    echo ">> Uninstalling Knative Eventing from HEAD"
    pushd .
    cd ${GOPATH}/src/knative.dev/eventing
    # Remove IMC
    ko delete -Rf "${EVENTING_IN_MEMORY_CHANNEL_CONFIG}"
    # Remove MT Channel Based Broker
    ko delete -f "${EVENTING_MT_CHANNEL_BROKER_CONFIG}"
    # Remove eventing
    ko delete -f "${EVENTING_CONFIG}"
    popd
  fi
  wait_until_object_does_not_exist namespaces "${EVENTING_NAMESPACE}"
}

# Add function call to trap
# Parameters: $1 - Function to call
#             $2...$n - Signals for trap
function add_trap() {
  local current_trap new_cmd cmd
  cmd=$1
  shift
  for trap_signal in "$@"; do
    current_trap="$(trap -p $trap_signal | cut -d\' -f2)"
    new_cmd="($cmd)"
    [[ -n "${current_trap}" ]] && new_cmd="${current_trap};${new_cmd}"
    trap -- "${new_cmd}" $trap_signal
  done
}

function test_setup() {
  kafka_setup || return 1

  # Install kail if needed.
  if ! which kail > /dev/null; then
    bash <( curl -sfL https://raw.githubusercontent.com/boz/kail/master/godownloader.sh) -b "$GOPATH/bin"
  fi

  # Capture all logs.
  kail > "${ARTIFACTS}/k8s.log.txt" &
  local kail_pid=$!
  # Clean up kail so it doesn't interfere with job shutting down
  add_trap "kill $kail_pid || true" EXIT

  # Publish test images.
  echo ">> Publishing test images from eventing"
  $(dirname $0)/upload-test-images.sh "${VENDOR_EVENTING_TEST_IMAGES}" e2e || fail_test "Error uploading test images"
  $(dirname $0)/upload-test-images.sh "test/test_images" e2e || fail_test "Error uploading test images"
}

function test_teardown() {
  kafka_teardown
}

function install_released_consolidated_channel {
  install_consolidated_channel_crds latest-release
}

function install_head_consolidated_channel {
  install_consolidated_channel_crds HEAD
}

function install_consolidated_channel_crds {
  local source url ver release_yaml
  source="${1:-HEAD}"
  if [[ "${source}" == 'HEAD' ]]; then
    echo "Installing consolidated Kafka Channel CRD (from HEAD)"
    rm -rf "${KAFKA_CRD_CONFIG_DIR}" && mkdir -p "${KAFKA_CRD_CONFIG_DIR}"
    cp "${CONSOLIDATED_TEMPLATE_DIR}/"*yaml "${KAFKA_CRD_CONFIG_DIR}"
    sed -i "s/namespace: knative-eventing/namespace: ${SYSTEM_NAMESPACE}/g" \
      "${KAFKA_CRD_CONFIG_DIR}/"*yaml
    sed -i "s/REPLACE_WITH_CLUSTER_URL/${KAFKA_CLUSTER_URL}/" \
      "${KAFKA_CRD_CONFIG_DIR}/${KAFKA_CRD_CONFIG_TEMPLATE}"
    ko apply -f "${KAFKA_CRD_CONFIG_DIR}"
    run_postinstall_jobs
  elif [[ "${source}" == 'latest-release' ]]; then
    ver="${LATEST_RELEASE_VERSION}"
    echo "Installing consolidated Kafka Channel CRD (from latest release: ${ver})"
    # Download the latest release of Knative Eventing Kafka.
    url="${EVENTING_KAFKA_REPO}/releases/download/${ver}/channel-consolidated.yaml"
    release_yaml="${ARTIFACTS}/channel-consolidated-${ver}.yaml"

    curl -Lo "${release_yaml}" "${url}"
    sed -i "s/namespace: knative-eventing/namespace: ${SYSTEM_NAMESPACE}/g" \
      "${release_yaml}"
    sed -i "s/REPLACE_WITH_CLUSTER_URL/${KAFKA_CLUSTER_URL}/" \
      "${release_yaml}"
    echo "Applying: ${release_yaml}"
    kubectl apply -f "${release_yaml}"
  else
    fail_test "Unsupported source of installation: ${source}"
    return 55
  fi
  sleep 1 # Wait until something gets deployed
  wait_until_pods_running "${SYSTEM_NAMESPACE}"
}

function install_released_consolidated_source {
  install_consolidated_sources_crds latest-release
}

function install_head_consolidated_source {
  install_consolidated_sources_crds HEAD
}

function install_consolidated_sources_crds() {
  local source url ver release_yaml
  source="${1:-HEAD}"
  if [[ "${source}" == 'HEAD' ]]; then
    echo "Installing consolidated Kafka Source CRD (from HEAD)"
    rm -rf "${KAFKA_SOURCE_CRD_CONFIG_DIR}" && mkdir -p "${KAFKA_SOURCE_CRD_CONFIG_DIR}"
    cp "${KAFKA_SOURCE_TEMPLATE_DIR}/"*yaml "${KAFKA_SOURCE_CRD_CONFIG_DIR}"
    sed -i "s/namespace: knative-eventing/namespace: ${SYSTEM_NAMESPACE}/g" "${KAFKA_SOURCE_CRD_CONFIG_DIR}/"*yaml
    ko apply -f "${KAFKA_SOURCE_CRD_CONFIG_DIR}" || return $?
  elif [[ "${source}" == 'latest-release' ]]; then
    ver="${LATEST_RELEASE_VERSION}"
    echo "Installing consolidated Kafka Source CRD (from latest release: ${ver})"
    # Download the latest release of Knative Eventing Kafka.
    url="${EVENTING_KAFKA_REPO}/releases/download/${ver}/source.yaml"
    release_yaml="${ARTIFACTS}/source-${ver}.yaml"

    curl -Lo "${release_yaml}" "${url}"
    sed -i "s/namespace: knative-eventing/namespace: ${SYSTEM_NAMESPACE}/g" \
      "${release_yaml}"
    echo "Applying: ${release_yaml}"
    kubectl apply -f "${release_yaml}"
  else
    fail_test "Unsupported source of installation: ${source}"
    return 56
  fi

  run_postinstall_jobs
  wait_until_pods_running "${EVENTING_NAMESPACE}" || fail_test "Failed to install the consolidated Kafka Source CRD"
}

function run_postinstall_jobs() {
  # There are no post-install scripts today. This needs to be enabled if any post-install script is added

  #  echo "Running post-install jobs using ${KAFKA_POST_INSTALL_DIR}"
  #  rm -rf "${KAFKA_POST_INSTALL_DIR}" && mkdir -p "${KAFKA_POST_INSTALL_DIR}"
  #  cp "${KAFKA_POST_INSTALL_TEMPLATE_DIR}/"*yaml "${KAFKA_POST_INSTALL_DIR}"
  #  sed -i "s/namespace: knative-eventing/namespace: ${SYSTEM_NAMESPACE}/g" \
  #    "${KAFKA_POST_INSTALL_DIR}/"*yaml
  #  ko apply -f "${KAFKA_POST_INSTALL_DIR}"

  echo "No postinstall jobs to run"
}

# Uninstall The eventing-kafka KafkaChannel Implementation Via Ko
function uninstall_channel_crds() {
  echo "Uninstalling Kafka Channel CRD"
  kubectl delete secret -n "${SYSTEM_NAMESPACE}" kafka-cluster

  # The distributed channel controller no longer actively monitors the kafka-cluster secret,
  # so the receiver will only be torn down if there is a change to the kafkachannels or the
  # config-kafka configmap.  However, if the secret is missing, the global resync does not happen
  # (the controller presumes this missing secret is an error and reacts accordingly), so this
  # manual deletion of the receiver deployment is the easiest solution here.
  if [[ $1 == "distributed" ]]; then
    # Create the same value as GenerateHash() in distributed/controller/util/hash.go
    hash=$(md5 -qs "kafka-cluster" | cut -c 1-8)
    kubectl delete deployment -n "${SYSTEM_NAMESPACE}" kafka-cluster-${hash}-receiver
    kubectl delete service -n "${SYSTEM_NAMESPACE}" kafka-cluster-${hash}-receiver
  fi

  echo "Current namespaces:"
  kubectl get namespaces
  echo "Current kafkachannels:"
  kubectl get kafkachannel -A
  ko delete --ignore-not-found=true --now --timeout 120s -f "${KAFKA_CRD_CONFIG_DIR}"
}

function uninstall_sources_crds() {
  echo "Uninstalling Kafka Source CRD"
  ko delete --ignore-not-found=true --now --timeout 180s -f "${KAFKA_SOURCE_CRD_CONFIG_DIR}"
}

function install_distributed_channel_crds() {
  echo "Installing distributed Kafka Channel CRD"
  rm -rf "${KAFKA_CRD_CONFIG_DIR}" && mkdir -p "${KAFKA_CRD_CONFIG_DIR}"
  cp "${DISTRIBUTED_TEMPLATE_DIR}/"*yaml "${KAFKA_CRD_CONFIG_DIR}"
  sed -i "s/namespace: knative-eventing/namespace: ${SYSTEM_NAMESPACE}/g" "${KAFKA_CRD_CONFIG_DIR}/"*yaml

  # Update The ConfigMap With Strimzi Kafka Cluster Brokers (No Authentication)
  sed -i "s/REPLACE_WITH_CLUSTER_URL/${KAFKA_CLUSTER_URL}/" ${KAFKA_CRD_CONFIG_DIR}/${EVENTING_KAFKA_CONFIG_TEMPLATE}

  # Update the config-kafka configmap to disable SASL/TLS for the tests
  sed -i '/^ *TLS:/{n;s/true/false/};/^ *SASL:/{n;s/true/false/}' "${KAFKA_CRD_CONFIG_DIR}/${EVENTING_KAFKA_CONFIG_TEMPLATE}"

  # Install The eventing-kafka KafkaChannel Implementation
  ko apply -f "${KAFKA_CRD_CONFIG_DIR}" || return 1

   # Add The kn-eventing-test-pull-secret (If Present) To ServiceAccount & Restart eventing-kafka Deployment
  add_kn_eventing_test_pull_secret "${SYSTEM_NAMESPACE}" eventing-kafka-channel-controller eventing-kafka-channel-controller

  run_postinstall_jobs

  wait_until_pods_running "${SYSTEM_NAMESPACE}" || fail_test "Failed to install the distributed Kafka Channel CRD"
}

function install_mt_source() {
  echo "Installing multi-tenant Kafka Source"
  rm -rf "${KAFKA_SOURCE_CRD_CONFIG_DIR}" && mkdir -p "${KAFKA_SOURCE_CRD_CONFIG_DIR}"
  cp "${KAFKA_MT_SOURCE_TEMPLATE_DIR}/"*yaml "${KAFKA_SOURCE_CRD_CONFIG_DIR}"
  sed -i "s/namespace: knative-eventing/namespace: ${SYSTEM_NAMESPACE}/g" "${KAFKA_SOURCE_CRD_CONFIG_DIR}/"*yaml
  ko apply -f "${KAFKA_SOURCE_CRD_CONFIG_DIR}" || return 1

  # FIXME: integration-test-mt-source fails if post-install scripts are executed (knative-sandbox/eventing-kafka#495)
  # run_postinstall_jobs

  wait_until_pods_running "${EVENTING_NAMESPACE}" || fail_test "Failed to install the multi-tenant Kafka Source"
}

function uninstall_mt_source() {
  echo "Uninstalling  multi-tenant Kafka Source CRD"
  ko delete --ignore-not-found=true --now --timeout 180s -f "${KAFKA_SOURCE_CRD_CONFIG_DIR}"
}

function kafka_setup() {
  # Create The Namespace Where Strimzi Kafka Will Be Installed
  echo "Installing Kafka Cluster"
  kubectl get -o name namespace ${STRIMZI_KAFKA_NAMESPACE} || kubectl create namespace ${STRIMZI_KAFKA_NAMESPACE} || return 1

  # Install Strimzi Into The Desired Namespace (Dynamically Changing The Namespace)
  sed "s/namespace: .*/namespace: ${STRIMZI_KAFKA_NAMESPACE}/" ${STRIMZI_INSTALLATION_CONFIG_TEMPLATE} > "${STRIMZI_INSTALLATION_CONFIG}"

  echo "Create The Actual Kafka Cluster Instance For The Cluster Operator To Setup using: ${STRIMZI_INSTALLATION_CONFIG}"
  kubectl apply -f "${STRIMZI_INSTALLATION_CONFIG}" -n "${STRIMZI_KAFKA_NAMESPACE}" -l strimzi.io/crd-install=true
  kubectl apply -f "${STRIMZI_INSTALLATION_CONFIG}" -n "${STRIMZI_KAFKA_NAMESPACE}"
  kubectl apply -f "${KAFKA_INSTALLATION_CONFIG}" -n "${STRIMZI_KAFKA_NAMESPACE}"

  # Delay Pod Running Check Until All Pods Are Created To Prevent Race Condition (Strimzi Kafka Instance Can Take A Bit To Spin Up)
  local iterations=0
  local progress="Waiting for Kafka Pods to be created..."
  while [[ $(kubectl get pods --no-headers=true -n ${STRIMZI_KAFKA_NAMESPACE} | wc -l) -lt 6 && $iterations -lt 60 ]] # 1 ClusterOperator, 3 Zookeeper, 1 Kafka, 1 EntityOperator
  do
    echo -ne "${progress}\r"
    progress="${progress}."
    iterations=$((iterations + 1))
    sleep 3
  done
  echo "${progress}"

  # Wait For The Strimzi Kafka Cluster Operator To Be Ready (Forcing Delay To Ensure CRDs Are Installed To Prevent Race Condition)
  wait_until_pods_running "${STRIMZI_KAFKA_NAMESPACE}" || fail_test "Failed to start up a Strimzi Kafka Instance"

  # Create some Strimzi Kafka Users
  kubectl apply -f "${KAFKA_USERS_CONFIG}" -n "${STRIMZI_KAFKA_NAMESPACE}"
}

function kafka_teardown() {
  echo "Uninstalling Kafka cluster"
  kubectl delete -f ${KAFKA_INSTALLATION_CONFIG} -n "${STRIMZI_KAFKA_NAMESPACE}"
  kubectl delete -f "${STRIMZI_INSTALLATION_CONFIG}" -n "${STRIMZI_KAFKA_NAMESPACE}"
  kubectl delete namespace "${STRIMZI_KAFKA_NAMESPACE}"
}

function create_tls_secrets() {
  echo "Creating TLS Kafka secret"
  STRIMZI_CRT=$(kubectl -n ${STRIMZI_KAFKA_NAMESPACE} get secret my-cluster-cluster-ca-cert --template='{{index .data "ca.crt"}}' | base64 --decode )
  TLSUSER_CRT=$(kubectl -n ${STRIMZI_KAFKA_NAMESPACE} get secret my-tls-user --template='{{index .data "user.crt"}}' | base64 --decode )
  TLSUSER_KEY=$(kubectl -n ${STRIMZI_KAFKA_NAMESPACE} get secret my-tls-user --template='{{index .data "user.key"}}' | base64 --decode )

  kubectl create secret --namespace "${SYSTEM_NAMESPACE}" generic strimzi-tls-secret \
    --from-literal=ca.crt="$STRIMZI_CRT" \
    --from-literal=user.crt="$TLSUSER_CRT" \
    --from-literal=user.key="$TLSUSER_KEY"
}

function create_sasl_secrets() {
  echo "Creating SASL Kafka secret"
  STRIMZI_CRT=$(kubectl -n ${STRIMZI_KAFKA_NAMESPACE} get secret my-cluster-cluster-ca-cert --template='{{index .data "ca.crt"}}' | base64 --decode )
  SASL_PASSWD=$(kubectl -n ${STRIMZI_KAFKA_NAMESPACE} get secret my-sasl-user --template='{{index .data "password"}}' | base64 --decode )

  kubectl create secret --namespace "${SYSTEM_NAMESPACE}" generic strimzi-sasl-secret \
    --from-literal=ca.crt="$STRIMZI_CRT" \
    --from-literal=password="$SASL_PASSWD" \
    --from-literal=saslType="SCRAM-SHA-512" \
    --from-literal=user="my-sasl-user"
}

# Installs the resources necessary to test the consolidated channel, runs those tests, and then cleans up those resources
function test_consolidated_channel_plain() {
  # Test the consolidated channel with no auth
  echo "Testing the consolidated channel and source"
  install_consolidated_channel_crds || return 1
  install_consolidated_sources_crds || return 1

  echo "Run rekt tests"
  go_test_e2e -tags=e2e -timeout=20m -test.parallel=${TEST_PARALLEL} -run "^TestKafkaChannelReadiness$" ./test/rekt/... || fail_test

  echo "Run classic tests"
  go_test_e2e -tags=e2e,consolidated,source -timeout=40m -test.parallel=${TEST_PARALLEL} ./test/e2e -channels=messaging.knative.dev/v1beta1:KafkaChannel  || fail_test
  go_test_e2e -tags=e2e,consolidated,source -timeout=5m -test.parallel=${TEST_PARALLEL} ./test/conformance -channels=messaging.knative.dev/v1beta1:KafkaChannel -sources=sources.knative.dev/v1beta1:KafkaSource || fail_test

  uninstall_sources_crds || return 1
  uninstall_channel_crds || return 1
}

function test_consolidated_channel_tls() {
  # Test the consolidated channel with TLS
  echo "Testing the consolidated channel with TLS"
  # Set the URL to the TLS listeners config
  sed -i "s/\${SYSTEM_NAMESPACE}/${SYSTEM_NAMESPACE}/g" ${KAFKA_TLS_CONFIG}
  cp "${KAFKA_TLS_CONFIG}" "${CONSOLIDATED_TEMPLATE_DIR}/configmaps/kafka-config.yaml"
  KAFKA_CLUSTER_URL=${KAFKA_TLS_CLUSTER_URL}

  install_consolidated_channel_crds || return 1

  go_test_e2e -tags=e2e,consolidated -timeout=40m -test.parallel=${TEST_PARALLEL} ./test/e2e -channels=messaging.knative.dev/v1beta1:KafkaChannel  || fail_test

  uninstall_channel_crds || return 1
}

function test_consolidated_channel_sasl() {
  # Test the consolidated channel with SASL
  echo "Testing the consolidated channel with SASL"
  # Set the URL to the SASL listeners config
  sed -i "s/\${SYSTEM_NAMESPACE}/${SYSTEM_NAMESPACE}/g" ${KAFKA_SASL_CONFIG}
  cp "${KAFKA_SASL_CONFIG}" "${CONSOLIDATED_TEMPLATE_DIR}/configmaps/kafka-config.yaml"
  KAFKA_CLUSTER_URL=${KAFKA_SASL_CLUSTER_URL}

  install_consolidated_channel_crds || return 1

  go_test_e2e -tags=e2e,consolidated -timeout=40m -test.parallel=${TEST_PARALLEL} ./test/e2e -channels=messaging.knative.dev/v1beta1:KafkaChannel  || fail_test

  uninstall_channel_crds || return 1
}

# Installs the resources necessary to test the distributed channel, runs those tests, and then cleans up those resources
function test_distributed_channel() {
  # Test the distributed channel
  echo "Testing the distributed channel"
  install_distributed_channel_crds || return 1

  echo "Run rekt tests"
  go_test_e2e -tags=e2e -timeout=20m -test.parallel=${TEST_PARALLEL} -run "^TestKafkaChannel*" ./test/rekt/... || fail_test

  echo "Run classic tests"
  go_test_e2e -tags=e2e -timeout=40m -test.parallel=${TEST_PARALLEL} ./test/e2e -channels=messaging.knative.dev/v1beta1:KafkaChannel  || fail_test
  go_test_e2e -tags=e2e -timeout=5m -test.parallel=${TEST_PARALLEL} ./test/conformance -channels=messaging.knative.dev/v1beta1:KafkaChannel || fail_test

  uninstall_channel_crds distributed || return 1
}

# Installs the resources necessary to test the multi-tenant source, runs those tests, and then cleans up those resources
function test_mt_source() {
  echo "Testing the multi-tenant source"
  install_mt_source || return 1

  export TEST_MT_SOURCE

  echo "Run rekt tests"
  go_test_e2e -tags=e2e -timeout=20m -test.parallel=${TEST_PARALLEL} -run "^TestKafkaSource*" ./test/rekt/... || fail_test

  # still run those since some test cases are still missing
  echo "Run classic tests"
  go_test_e2e -tags=source,mtsource -timeout=20m -test.parallel=${TEST_PARALLEL} ./test/e2e/...  || fail_test

  # wait for all KafkaSources to be deleted
  local iterations=0
  local progress="Waiting for KafkaSources to be deleted..."
  while [[ "$(kubectl get kafkasources --all-namespaces)" != "" && $iterations -lt 60 ]]
  do
    echo -ne "${progress}\r"
    progress="${progress}."
    iterations=$((iterations + 1))
    kubectl get kafkasources --all-namespaces -oyaml
    sleep 5
  done

  uninstall_mt_source || return 1
}


function parse_flags() {
  # This function will be called repeatedly by initialize() with one fewer
  # argument each time and expects a return value of "the number of arguments to skip"
  # so we can just check the first argument and return 1 (to have it redirected to the
  # test container) or 0 (to have initialize() parse it normally).
  case $1 in
    --distributed)
      TEST_DISTRIBUTED_CHANNEL=1
      return 1
      ;;
    --consolidated)
      TEST_CONSOLIDATED_CHANNEL=1
      return 1
      ;;
    --consolidated-tls)
      TEST_CONSOLIDATED_CHANNEL_TLS=1
      return 1
      ;;
    --consolidated-sasl)
      TEST_CONSOLIDATED_CHANNEL_SASL=1
      return 1
      ;;
    --mt-source)
      TEST_MT_SOURCE=1
      return 1
      ;;
  esac
  return 0
}
