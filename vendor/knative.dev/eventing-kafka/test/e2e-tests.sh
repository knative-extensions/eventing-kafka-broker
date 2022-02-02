#!/usr/bin/env bash

# Copyright 2020 The Knative Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This script runs the end-to-end tests against eventing-contrib built
# from source.

# If you already have the *_OVERRIDE environment variables set, call
# this script with the --run-tests arguments and it will use the cluster
# and run the tests.
# Note that local clusters often do not have the resources to run 12 parallel
# tests (the default) as the tests each tend to create their own namespaces and
# dispatchers.  For example, a local Docker cluster with 4 CPUs and 8 GB RAM will
# probably be able to handle 6 at maximum.  Be sure to adequately set the
# MAX_PARALLEL_TESTS variable before running this script, with the caveat that
# lowering it too much might make the tests run over the timeout that
# the go_test_e2e commands are using below.

# Calling this script without arguments will create a new cluster in
# project $PROJECT_ID, start Knative eventing system, install resources
# in eventing-contrib, run all of the test suites and delete the cluster.

# To run an individual integration test suite, set one of these environment variables:
#   TEST_CONSOLIDATED_CHANNEL=1    To install and run the consolidated channel
#   TEST_DISTRIBUTED_CHANNEL=1     To install and run the distributed channel

# Variables supported by this script:
#   PROJECT_ID:  the GKR project in which to create the new cluster (unless using "--run-tests")
#   MAX_PARALLEL_TESTS:  The maximum number of go tests to run in parallel (via "-test.parallel", default 12)

TEST_PARALLEL=${MAX_PARALLEL_TESTS:-12}

source "$(dirname "$0")/e2e-common.sh"

# Create the system namespace if it doesn't already exist (may be the same as the EVENTING_NAMESPACE)
kubectl get namespace "${SYSTEM_NAMESPACE}" || kubectl create namespace "${SYSTEM_NAMESPACE}"

TEST_CONSOLIDATED_CHANNEL=${TEST_CONSOLIDATED_CHANNEL:-0}
TEST_CONSOLIDATED_CHANNEL_TLS=${TEST_CONSOLIDATED_CHANNEL_TLS:-0}
TEST_CONSOLIDATED_CHANNEL_SASL=${TEST_CONSOLIDATED_CHANNEL_SASL:-0}
TEST_DISTRIBUTED_CHANNEL=${TEST_DISTRIBUTED_CHANNEL:-0}
TEST_MT_SOURCE=${TEST_MT_SOURCE:-0}

echo "e2e-tests.sh command line: $@"

# If you wish to use this script just as test setup, *without* teardown, add "--skip-teardowns" to the initialize command
initialize $@ --skip-istio-addon

# Copy some resources if the SYSTEM_NAMESPACE is not the same as the EVENTING_NAMESPACE
if [[ $SYSTEM_NAMESPACE != $EVENTING_NAMESPACE ]]; then
  for configmap in config-leader-election config-logging config-observability config-kafka; do
    kubectl get configmap "${configmap}" "--namespace=${EVENTING_NAMESPACE}" -o yaml | sed "s/namespace: ${EVENTING_NAMESPACE}/namespace: ${SYSTEM_NAMESPACE}/" | kubectl create -f -
  done
fi

echo "e2e-tests.sh environment:"
echo "EVENTING_NAMESPACE: ${EVENTING_NAMESPACE}"
echo "SYSTEM_NAMESPACE: ${SYSTEM_NAMESPACE}"
echo "TEST_CONSOLIDATED_CHANNEL: ${TEST_CONSOLIDATED_CHANNEL}"
echo "TEST_DISTRIBUTED_CHANNEL: ${TEST_DISTRIBUTED_CHANNEL}"
echo "TEST_CONSOLIDATED_CHANNEL_TLS: ${TEST_CONSOLIDATED_CHANNEL_TLS}"
echo "TEST_CONSOLIDATED_CHANNEL_SASL: ${TEST_CONSOLIDATED_CHANNEL_SASL}"
echo "TEST_MT_SOURCE: ${TEST_MT_SOURCE}"

# If none of the tests were explicitly specified, run both plain tests
if [[ $TEST_CONSOLIDATED_CHANNEL != 1 ]] && [[ $TEST_CONSOLIDATED_CHANNEL_TLS != 1 ]] && [[ $TEST_CONSOLIDATED_CHANNEL_SASL != 1 ]] && [[ $TEST_DISTRIBUTED_CHANNEL != 1 ]] && [[ $TEST_MT_SOURCE != 1 ]]; then
  TEST_DISTRIBUTED_CHANNEL=1
  TEST_CONSOLIDATED_CHANNEL=1
fi

create_tls_secrets
create_sasl_secrets

if [[ $TEST_CONSOLIDATED_CHANNEL == 1 ]]; then
  echo "Launching the PLAIN TESTS:"
  test_consolidated_channel_plain || exit 1
fi

if [[ $TEST_CONSOLIDATED_CHANNEL_TLS == 1 ]]; then
  echo "Launching the TLS TESTS:"
  test_consolidated_channel_tls || exit 1
fi

if [[ $TEST_CONSOLIDATED_CHANNEL_SASL == 1 ]]; then
  echo "Launching the SASL TESTS:"
  test_consolidated_channel_sasl || exit 1
fi

if ! command -v ps &> /dev/null && command -v apt-get &> /dev/null; then
    # need to install ps
    echo "Couldn't find ps command. Installing it. "
    apt-get update && apt-get install -y procps
fi

# Terminate any zipkin port-forward processes that are still present on the system
ps -e -o pid,command | grep 'kubectl port-forward zipkin[^*]*9411:9411 -n' | sed 's/^ *\([0-9][0-9]*\) .*/\1/' | xargs kill

if [[ $TEST_DISTRIBUTED_CHANNEL == 1 ]]; then
  test_distributed_channel || exit 1
fi

if [[ $TEST_MT_SOURCE == 1 ]]; then
  echo "Launching the multi-tenant source TESTS:"
  test_mt_source || exit 1
fi

# If you wish to use this script just as test setup, *without* teardown, just uncomment this line and comment all go_test_e2e commands
# trap - SIGINT SIGQUIT SIGTSTP EXIT

success
