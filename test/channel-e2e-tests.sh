#!/usr/bin/env bash

export SCALE_CHAOSDUCK_TO_ZERO=1

source $(dirname $0)/e2e-common.sh

$(dirname $0)/scripts/first-event-delay.sh || fail_test "Failed"

go_test_e2e -timeout=1h ./test/e2e_channel/... -channels=messaging.knative.dev/v1beta1:KafkaChannel || fail_test "E2E suite (KafkaChannel) failed"
