#!/usr/bin/env bash

source $(dirname $0)/e2e-common.sh
export BROKER_TEMPLATES=./templates/kafka-broker

if ! ${SKIP_INITIALIZE}; then
  initialize $@ --skip-istio-addon --min-nodes=4 --max-nodes=4
  save_release_artifacts || fail_test "Failed to save release artifacts"
fi

if ! ${LOCAL_DEVELOPMENT}; then
  scale_controlplane kafka-controller kafka-webhook-eventing eventing-webhook eventing-controller
  apply_sacura                            || fail_test "Failed to apply Sacura"
  apply_sacura_sink_source || fail_test "Failed to apply Sacura (Source, Sink, Broker, Channel)"
  apply_chaos                             || fail_test "Failed to apply chaos"
fi

header "Waiting Knative eventing to come up"

wait_until_pods_running knative-eventing || fail_test "Pods in knative-eventing didn't come up"

export_logs_continuously

header "Running tests"

if [ "${EVENTING_KAFKA_BROKER_CHANNEL_AUTH_SCENARIO:-""}" != "" ]; then
  # if this flag exists, only test Kafka channel scenarios with auth
  $(dirname $0)/channel-reconciler-tests.sh || fail_test "Failed to execute KafkaChannel tests"
  success
fi

if [[ -z "${BROKER_CLASS}" ]]; then
  fail_test "Broker class is not defined. Specify it with 'BROKER_CLASS' env var."
else
  echo "BROKER_CLASS is set to '${BROKER_CLASS}'. Running tests for that broker class."
fi

if [ "${BROKER_CLASS}" == "KafkaNamespaced" ]; then
  # if flag exists, only test tests that are relevant to namespaced KafkaBroker
  echo "BROKER_CLASS is set to 'KafkaNamespaced'. Only running the relevant tests."
  go_test_e2e -timeout=1h ./test/e2e_new/... || fail_test "E2E (new) suite failed"
  success
fi

go_test_e2e -timeout=1h ./test/e2e_new/... || fail_test "E2E (new) suite failed"

go_test_e2e -tags=e2e,cloudevents -timeout=1h ./test/e2e_new_channel/... || fail_test "E2E (new - KafkaChannel) suite failed"

go_test_e2e -tags=deletecm ./test/e2e_new/... || fail_test "E2E (new deletecm) suite failed"

echo "Running E2E Reconciler Tests with strict transport encryption"

kubectl apply -Rf "$(dirname "$0")/config-transport-encryption"

go_test_e2e -timeout=1h ./test/e2e_new -run TLS || fail_test

if ! ${LOCAL_DEVELOPMENT}; then
  go_test_e2e -tags=sacura -timeout=40m ./test/e2e/... || fail_test "E2E (sacura) suite failed"
fi

success
