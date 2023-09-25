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

if ! ${SKIP_INITIALIZE}; then
	initialize "$@" --num-nodes=4
	save_release_artifacts || fail_test "Failed to save release artifacts"
fi

if ! ${LOCAL_DEVELOPMENT}; then
	scale_controlplane kafka-controller kafka-webhook-eventing eventing-webhook eventing-controller
	apply_sacura || fail_test "Failed to apply Sacura"
	apply_sacura_sink_source || fail_test "Failed to apply Sacura (Source, Sink, Broker, Channel)"
	apply_chaos || fail_test "Failed to apply chaos"
fi

header "Waiting Knative eventing to come up"

wait_until_pods_running knative-eventing || fail_test "Pods in knative-eventing didn't come up"

kubectl apply -f $(dirname $0)/keda/enable-keda-autoscaling.yaml

export_logs_continuously

header "Running tests"

go_test_e2e -timeout=1h -run=KafkaSource ./test/e2e_new/... || fail_test "E2E (new) suite failed"

success
