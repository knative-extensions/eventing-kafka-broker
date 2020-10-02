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

apply_chaos || fail_test "Failed to apply chaos"

header "Waiting Knative eventing to come up"

apply_sacura || fail_test "Failed to apply Sacura"

header "Running tests"

go_test_e2e -timeout=30m ./test/... || fail_test "Integration test failed"

kubectl wait --for=condition=complete --timeout=10m -n sacura job/sacura || job_completed=$?

# Export logs from pods in the namespace sacura
go run test/cmd/logs-exporter/main.go --logs-namespace sacura

if [[ "${job_completed}" -ne "0" ]]; then
  kubectl logs -n knative-eventing -l=app=sacura
  kubectl describe -n knative-eventing job sacura
  fail_test "Sacura Job not completed"
fi

success
