#!/usr/bin/env bash

# Setup env vars to override the default settings
export PROJECT_NAME="knative-eventing-performance"
export BENCHMARK_ROOT_PATH="test/performance/benchmarks"

source vendor/knative.dev/test-infra/scripts/performance-tests.sh
source test/e2e-common.sh

export EVENTING_KAFKA_BROKER_ARTIFACT="eventing-kafka-broker.yaml"
readonly TEST_CONFIG_VARIANT="continuous"
readonly TEST_NAMESPACE="default"

function update_knative() {

  knative_eventing

  [ -f "${EVENTING_KAFKA_BROKER_ARTIFACT}" ] && rm "${EVENTING_KAFKA_BROKER_ARTIFACT}"

  control_plane_setup
  if [[ $? -ne 0 ]]; then
    return 1
  fi

  data_plane_setup
  if [[ $? -ne 0 ]]; then
    return 1
  fi

  kubectl apply -f "${EVENTING_KAFKA_BROKER_ARTIFACT}" || abort "failed to apply Apache Kafka Broker"

  wait_until_pods_running knative-eventing || abort "Knative Eventing did not come up"
}

function update_benchmark() {

  local benchmark_path="${BENCHMARK_ROOT_PATH}/$1"

  # TODO(chizhg): add update_environment function in test-infra/scripts/performance-tests.sh and move the below code there
  echo ">> Updating configmap"
  kubectl delete configmap config-mako -n "${TEST_NAMESPACE}" --ignore-not-found=true
  kubectl create configmap config-mako -n "${TEST_NAMESPACE}" --from-file="${benchmark_path}/prod.config" || abort "failed to create config-mako configmap"
  kubectl patch configmap config-mako -n "${TEST_NAMESPACE}" -p '{"data":{"environment":"prod"}}' || abort "failed to patch config-mako configmap"

  echo ">> Updating benchmark $1"
  ko delete -f "${benchmark_path}"/${TEST_CONFIG_VARIANT} --ignore-not-found=true --wait=false
  sleep 30

  # Add Git info in kodata so the benchmark can show which commit it's running on.
  local kodata_path="vendor/knative.dev/eventing/test/test_images/performance/kodata"
  mkdir "${kodata_path}"
  ln -s "${REPO_ROOT_DIR}/.git/HEAD" "${kodata_path}"
  ln -s "${REPO_ROOT_DIR}/.git/refs" "${kodata_path}"
  ko apply --strict -f "${benchmark_path}"/${TEST_CONFIG_VARIANT} || abort "failed to apply benchmark $1"

  wait_until_pods_running knative-eventing || abort "Knative Eventing did not come up"
}

main $@
