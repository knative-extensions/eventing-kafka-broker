#!/usr/bin/env bash

# Copyright 2023 The Knative Authors
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

source $(dirname "$0")/e2e-common.sh
export BROKER_TEMPLATES=./templates/kafka-broker

TESTNAME="$1"
TESTDIR="$2"

if [[ -z "${TESTNAME}" ]]; then
  fail_test "No testname provided"
fi

if [[ -z "${TESTDIR}" ]]; then
  fail_test "No testdir provided"
fi

header "Waiting Knative eventing to come up"

wait_until_pods_running knative-eventing || fail_test "Pods in knative-eventing didn't come up"

export_logs_continuously

header "Running tests"

if [[ -z "${BROKER_CLASS}" ]]; then
  echo "BROKER_CLASS is not set, setting it to Kafka as a default"
  BROKER_CLASS="Kafka"
else
  echo "BROKER_CLASS is set to '${BROKER_CLASS}'. Running tests for that broker class."
fi

go_test_e2e -timeout=30m -run="${TESTNAME}" "${TESTDIR}/..." || fail_test "Test(s) failed"

success
