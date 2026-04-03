#!/usr/bin/env bash

source $(dirname $0)/e2e-common.sh

# Enable parallel execution for first-event-delay test (100 channels in parallel vs sequential)
export PARALLEL=1
$(dirname $0)/scripts/first-event-delay.sh || fail_test "Failed"

go_test_e2e -timeout=1h -parallel=8 ./test/e2e_channel/... -channels=messaging.knative.dev/v1beta1:KafkaChannel || fail_test "E2E suite (KafkaChannel) failed"
