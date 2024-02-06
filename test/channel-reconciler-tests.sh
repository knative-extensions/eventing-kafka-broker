#!/usr/bin/env bash

source $(dirname $0)/e2e-common.sh

go_test_e2e -tags=e2e,cloudevents -timeout=1h ./test/e2e_new_channel/... || fail_test "E2E (new - KafkaChannel) suite failed"

echo "Running E2E Channel Reconciler Tests with OIDC authentication enabled"

kubectl apply -Rf "$(dirname "$0")/config-oidc-authentication"

go_test_e2e -timeout=1h ./test/e2e_new_channel/... -run OIDC || fail_test
