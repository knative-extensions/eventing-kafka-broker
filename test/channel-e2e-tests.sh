#!/usr/bin/env bash

source $(dirname $0)/e2e-common.sh

$(dirname $0)/../reproducer.sh || fail_test "Reproducer failed"

go_test_e2e -timeout=1h ./test/e2e_channel/... -channels=messaging.knative.dev/v1beta1:KafkaChannel || fail_test "E2E suite (KafkaChannel) failed"
