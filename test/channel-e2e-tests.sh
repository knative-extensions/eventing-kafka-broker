#!/usr/bin/env bash

source $(dirname $0)/e2e-common.sh

header "Running first-event-delay.sh script ..."
$(dirname $0)/scripts/first-event-delay.sh || fail_test "Failed"

header "Running e2e_channel go tests ..."
go_test_e2e -timeout=1h ./test/e2e_channel/... -channels=messaging.knative.dev/v1beta1:KafkaChannel || fail_test "E2E suite (KafkaChannel) failed"
