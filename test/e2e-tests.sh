#!/usr/bin/env bash

source $(dirname $0)/e2e-common.sh

setup_eventing_kafka_broker_test_cluster || fail_test "Failed to setup cluster"

go_test_e2e -timeout=30m ./test/e2e/... -channels=messaging.knative.dev/v1beta1:KafkaChannel || fail_test "E2E suite failed"

go_test_e2e -tags=deletecm ./test/e2e/... -channels=messaging.knative.dev/v1beta1:KafkaChannel || fail_test "E2E (deletecm) suite failed"

if ! ${LOCAL_DEVELOPMENT}; then
  go_test_e2e -tags=sacura -timeout=40m ./test/e2e/... || fail_test "E2E (sacura) suite failed"
fi

success
