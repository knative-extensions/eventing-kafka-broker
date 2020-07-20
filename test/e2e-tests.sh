#!/usr/bin/env bash

# variables used:
# - SKIP_INITIALIZE (default: false) - skip cluster creation

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

header "Waiting Knative eventing to come up"

wait_until_pods_running knative-eventing || fail_test "Knative Eventing did not come up"

header "Running tests"

go_test_e2e -timeout=10m ./test/... || fail_test "Integration test failed"

success
