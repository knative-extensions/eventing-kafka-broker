#!/usr/bin/env bash

# variables used:
# - SKIP_INITIALIZE (default: false) - skip cluster creation.
# - LOCAL_DEVELOPMENT (default: false) - skip heavy workloads installation like load and chaos generators.

readonly SKIP_INITIALIZE=${SKIP_INITIALIZE:-false}
readonly LOCAL_DEVELOPMENT=${LOCAL_DEVELOPMENT:-false}

source $(dirname $0)/e2e-common.sh

# If gcloud is not available make it a no-op, not an error.
which gcloud &>/dev/null || gcloud() { echo "[ignore-gcloud $*]" 1>&2; }

# Use GNU tools on macOS. Requires the 'grep' and 'gnu-sed' Homebrew formulae.
if [ "$(uname)" == "Darwin" ]; then
  sed=gsed
  grep=ggrep
fi

if ! ${SKIP_INITIALIZE}; then
  initialize $@ --skip-istio-addon
fi

if ! ${LOCAL_DEVELOPMENT}; then
  scale_controlplane kafka-controller kafka-webhook-eventing eventing-webhook eventing-controller kafka-broker-receiver kafka-broker-dispatcher kafka-sink-receiver
fi

if ! ${LOCAL_DEVELOPMENT}; then
  apply_chaos || fail_test "Failed to apply chaos"
fi

if ! ${LOCAL_DEVELOPMENT}; then
  apply_sacura || fail_test "Failed to apply Sacura"
fi

header "Waiting Knative eventing to come up"

wait_until_pods_running knative-eventing || fail_test "Pods in knative-eventing didn't come up"

header "Running tests"

go_test_e2e -timeout=30m ./test/... || fail_test "Integration tests failed"

if ! ${LOCAL_DEVELOPMENT}; then
  go_test_e2e -tags=sacura -timeout=20m ./test/... || fail_test "Sacura tests failed"
fi

success
