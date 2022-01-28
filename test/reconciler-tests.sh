#!/usr/bin/env bash

source $(dirname $0)/e2e-common.sh

if ! ${SKIP_INITIALIZE}; then
  initialize $@ --skip-istio-addon
  save_release_artifacts || fail_test "Failed to save release artifacts"
fi

if ! ${LOCAL_DEVELOPMENT}; then
  scale_controlplane kafka-controller kafka-webhook-eventing eventing-webhook eventing-controller
  apply_sacura || fail_test "Failed to apply Sacura"
  apply_chaos || fail_test "Failed to apply chaos"
fi

header "Waiting Knative eventing to come up"

wait_until_pods_running knative-eventing || fail_test "Pods in knative-eventing didn't come up"

if [ ! -z "${CHANNEL_AUTH_SCENARIO}" ]; then
  # if this flag exists, only test Kafka channel scenarios with auth

  # Setup auth config for KafkaChannel. Uses CHANNEL_AUTH_SCENARIO env var.
  header "Setting up auth config for KafkaChannel"
  setup_kafka_channel_auth

  header "Running KafkaChannel tests"

  export_logs_continuously

  go_test_e2e -tags=e2e,cloudevents -timeout=1h ./test/e2e_new_channel/... || fail_test "E2E (new - KafkaChannel) suite failed"

  # this exits the script
  success
fi

header "Running tests"

export_logs_continuously

go_test_e2e -timeout=1h ./test/e2e_new/... || fail_test "E2E (new) suite failed"

go_test_e2e -tags=e2e,cloudevents -timeout=1h ./test/e2e_new_channel/... || fail_test "E2E (new - KafkaChannel) suite failed"

go_test_e2e -tags=deletecm ./test/e2e_new/... || fail_test "E2E (new deletecm) suite failed"

if ! ${LOCAL_DEVELOPMENT}; then
  go_test_e2e -tags=sacura -timeout=40m ./test/e2e/... || fail_test "E2E (sacura) suite failed"
fi

success
