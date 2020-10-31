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
  apply_chaos || fail_test "Failed to apply chaos"
fi

if ! ${LOCAL_DEVELOPMENT}; then
  apply_sacura || fail_test "Failed to apply Sacura"
fi

header "Waiting Knative eventing to come up"

wait_until_pods_running knative-eventing || fail_test "Pods in knative-eventing didn't come up"

header "Running tests"

go_test_e2e -timeout=30m -parallel=12 ./test/... || fail_test "Integration test failed"

if ! ${LOCAL_DEVELOPMENT}; then

  kubectl wait --for=condition=complete --timeout=10m -n sacura job/sacura || job_completed=$?

  # Export logs from pods in the namespace sacura.
  go run test/cmd/logs-exporter/main.go --logs-namespace sacura

  if [[ "${job_completed}" -ne "0" ]]; then
    kubectl logs -n sacura -l=app=sacura
    kubectl describe -n sacura job sacura
    fail_test "Sacura Job not completed"
  fi

fi

success
