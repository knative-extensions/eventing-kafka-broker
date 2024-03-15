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

set -euo pipefail

source $(dirname $0)/../../vendor/knative.dev/hack/e2e-tests.sh

CLUSTER_SUFFIX=${CLUSTER_SUFFIX:-"cluster.local"}
SYSTEM_NAMESPACE=${SYSTEM_NAMESPACE:-"knative-eventing"}

kubectl create namespace kafka --dry-run=client -o yaml | kubectl apply -f -

header "Applying Strimzi Cluster Operator file"
cat $(dirname $0)/strimzi-cluster-operator.yaml | sed 's/namespace: .*/namespace: kafka/' | sed "s/cluster.local/${CLUSTER_SUFFIX}/g" | kubectl apply -n kafka -f -

sleep 10

kubectl -n kafka apply -f $(dirname $0)/kafka-ephemeral.yaml || kubectl -n kafka apply -f $(dirname $0)/kafka-ephemeral.yaml

echo "Create TLS user"
kubectl apply -n kafka -f $(dirname $0)/user-tls.yaml

echo "Create SASL SCRAM 512 user"
kubectl apply -n kafka -f $(dirname $0)/user-sasl-scram-512.yaml

echo "Create SASL SCRAM 512 user with limited privileges"
kubectl apply -n kafka -f $(dirname $0)/user-sasl-scram-512-restricted.yaml

# Waits until the given object exists.
# Parameters: $1 - the kind of the object.
#             $2 - object's name.
#             $3 - namespace (optional).
function wait_until_object_exists() {
  local KUBECTL_ARGS="get $1 $2"
  local DESCRIPTION="$1 $2"

  if [[ -n $3 ]]; then
    KUBECTL_ARGS="get -n $3 $1 $2"
    DESCRIPTION="$1 $3/$2"
  fi
  echo -n "Waiting until ${DESCRIPTION} exists"
  for i in {1..150}; do  # timeout after 5 minutes
    if kubectl ${KUBECTL_ARGS} > /dev/null 2>&1; then
      echo -e "\n${DESCRIPTION} exists"
      return 0
    fi
    echo -n "."
    sleep 2
  done
  echo -e "\n\nERROR: timeout waiting for ${DESCRIPTION} to exist"
  kubectl ${KUBECTL_ARGS}
  return 1
}

wait_until_pods_running kafka || fail_test "Failed to start up a Kafka cluster"
