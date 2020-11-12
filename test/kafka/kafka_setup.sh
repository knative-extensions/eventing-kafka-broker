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

set -e

source $(dirname $0)/../../vendor/knative.dev/hack/e2e-tests.sh


kubectl create namespace kafka --dry-run -o yaml | kubectl apply -f -

sleep 5

header "Applying Strimzi Cluster Operator file"
cat $(dirname $0)/strimzi-cluster-operator.yaml | sed "s/.cluster.local/${CLUSTER_SUFFIX}/g" | kubectl apply -f -

sleep 5

kubectl -n kafka apply -f $(dirname $0)/kafka-ephemeral-single.yaml

sleep 5

wait_until_pods_running kafka || fail_test "Failed to start up a Kafka cluster"
