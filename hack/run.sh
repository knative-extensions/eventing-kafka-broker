#!/usr/bin/env bash

set -e

ROOT_DIR=$(dirname $0)/..

action="$1"

function usage() {
  cmd="$1"

  echo ""
  echo "Usage                                                      for more information read DEVELOPMENT.md"
  echo "$cmd <command>"
  echo ""
  echo "command:"
  echo "   deploy-infra                                            Deploy eventing, Kafka (Strimzi), publish test images"
  echo "   teardown-infra                                          Remove eventing, Kafka (Strimzi)"
  echo "   deploy-kafka                                            Deploy Kafka (Strimzi)"
  echo "   deploy                                                  Deploy eventing-kafka-broker"
  echo "   teardown                                                Remove eventing-kafka-broker"
  echo "   unit-tests, unit-test                                   Run unit tests"
  echo "   unit-tests-data-plane, unit-test-data-plane             Run data-plane unit tests"
  echo "   unit-tests-control-plane, unit-test-control-plane       Run control-plane unit tests"
  echo "   build-tests-control-plane, build-test-control-plane     Run build tests"
  echo "   deploy-sacura                                           Deploy sacura job"
  echo "   sacura-test                                             Run sacura tests"
  echo "   teardown-sacura                                         Remove sacura job"
  echo "   deploy-chaos                                            Deploy chaosduck"
  echo "   teardown-chaos                                          Remove chaosduck"
  echo "   profiler                                                Run profiling tests"
  echo "   generate                                                Run code generators"
  echo "   build-from-source                                       Build artifacts from source"
  echo ""
}

if [[ "$action" == "deploy-infra" ]]; then
  source "${ROOT_DIR}"/test/e2e-common.sh && knative_setup
elif [[ "${action}" == "teardown-infra" ]]; then
  source "${ROOT_DIR}"/test/e2e-common.sh && knative_teardown
elif [[ "${action}" == "deploy-kafka" ]]; then
  source "${ROOT_DIR}"/test/e2e-common.sh && kafka_setup
elif [[ "${action}" == "deploy" ]]; then
  source "${ROOT_DIR}"/test/e2e-common.sh && test_setup
elif [[ "${action}" == "build-from-source" ]]; then
  source "${ROOT_DIR}"/test/e2e-common.sh && build_components_from_source
elif [[ "${action}" == "teardown" ]]; then
  source "${ROOT_DIR}"/test/e2e-common.sh && test_teardown
elif [[ "${action}" == "unit-test" || "${action}" == "unit-tests" ]]; then
  "${ROOT_DIR}"/test/presubmit-tests.sh --unit-tests
elif [[ "${action}" == "unit-test-data-plane" || "${action}" == "unit-tests-data-plane" ]]; then
  cd "${ROOT_DIR}"/data-plane && ./mvnw verify -B -U && cd -
elif [[ "${action}" == "unit-test-control-plane" || "${action}" == "unit-tests-control-plane" ]]; then
  go test -race ./...
elif [[ "${action}" == "build-test" || "${action}" == "build-tests" ]]; then
  "${ROOT_DIR}"/test/presubmit-tests.sh --build-tests
elif [[ "${action}" == "deploy-sacura" ]]; then
  source "${ROOT_DIR}"/test/e2e-common.sh && apply_sacura
elif [[ "${action}" == "sacura-test" || "${action}" == "sacura-tests" ]]; then
  source "${ROOT_DIR}"/test/e2e-common.sh && go_test_e2e -tags=sacura -timeout=40m ./test/e2e/...
elif [[ "${action}" == "teardown-sacura" ]]; then
  source "${ROOT_DIR}"/test/e2e-common.sh && delete_sacura
elif [[ "${action}" == "deploy-chaos" ]]; then
  export REPLICAS="3"
  source "${ROOT_DIR}"/test/e2e-common.sh && scale_controlplane kafka-controller kafka-webhook-eventing eventing-webhook eventing-controller
  source "${ROOT_DIR}"/test/e2e-common.sh && wait_until_pods_running knative-eventing
  source "${ROOT_DIR}"/test/e2e-common.sh && apply_chaos
elif [[ "${action}" == "teardown-chaos" ]]; then
  export REPLICAS="1"
  source "${ROOT_DIR}"/test/e2e-common.sh && scale_controlplane kafka-controller kafka-webhook-eventing eventing-webhook eventing-controller
  source "${ROOT_DIR}"/test/e2e-common.sh && delete_chaos
elif [[ "${action}" == "profiler" ]]; then
  "${ROOT_DIR}"/data-plane/profiler/run.sh
elif [[ "${action}" == "generate" ]]; then
  "${ROOT_DIR}"/hack/generate-proto.sh
  "${ROOT_DIR}"/hack/update-codegen.sh
else
  echo "Unrecognized action ${action}"
  usage "$0"
  exit 1
fi
