#!/usr/bin/env bash

source $(dirname $0)/e2e-common.sh

if ! ${SKIP_INITIALIZE}; then
  initialize $@ --skip-istio-addon
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
  $(dirname $0)/channel-e2e-tests.sh || fail_test "Failed to execute KafkaChannel tests"
  success
fi

export BROKER_CLASS="Kafka"

go_test_e2e -timeout=1h ./test/e2e/...        || fail_test "E2E suite failed (directory: ./e2e/...)"
go_test_e2e -timeout=1h ./test/e2e_broker/... || fail_test "E2E suite failed (directory: ./e2e_broker/...)"
go_test_e2e -timeout=1h ./test/e2e_sink/...   || fail_test "E2E suite failed (directory: ./e2e_sink/...)"
go_test_e2e -timeout=1h ./test/e2e_source/... || fail_test "E2E suite failed (directory: ./e2e_source/...)"

go_test_e2e -timeout=1h ./test/e2e_channel/... -channels=messaging.knative.dev/v1beta1:KafkaChannel || fail_test "E2E suite (KafkaChannel) failed (directory: ./e2e_channel/...)"

go_test_e2e -tags=deletecm ./test/e2e/... || fail_test "E2E (deletecm) suite failed (directory: ./e2e/...)"

if ! ${LOCAL_DEVELOPMENT}; then
  go_test_e2e -tags=sacura -timeout=40m ./test/e2e/... || fail_test "E2E (sacura) suite failed (directory: ./e2e/...)"
fi

success
