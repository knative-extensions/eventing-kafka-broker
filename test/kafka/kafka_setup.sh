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
cat $(dirname $0)/strimzi-cluster-operator.yaml | sed "s/cluster.local/${CLUSTER_SUFFIX}/g" | kubectl apply -n kafka -f -

sleep 10

kubectl -n kafka apply -f $(dirname $0)/kafka-ephemeral.yaml || kubectl -n kafka apply -f $(dirname $0)/kafka-ephemeral.yaml

echo "Create TLS user"
kubectl apply -n kafka -f $(dirname $0)/user-tls.yaml

echo "Create SASL SCRUM 512 user"
kubectl apply -n kafka -f $(dirname $0)/user-sasl-scram-512.yaml

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

function create_tls_secrets() {
  ca_cert_secret="my-cluster-cluster-ca-cert"
  tls_user="my-tls-user"

  echo "Waiting until secrets: ${ca_cert_secret}, ${tls_user} exist"
  wait_until_object_exists secret ${ca_cert_secret} kafka
  wait_until_object_exists secret ${tls_user} kafka

  echo "Creating TLS Kafka secret"

  STRIMZI_CRT=$(kubectl -n kafka get secret "${ca_cert_secret}" --template='{{index .data "ca.crt"}}' | base64 --decode )
  TLSUSER_CRT=$(kubectl -n kafka get secret "${tls_user}" --template='{{index .data "user.crt"}}' | base64 --decode )
  TLSUSER_KEY=$(kubectl -n kafka get secret "${tls_user}" --template='{{index .data "user.key"}}' | base64 --decode )

  kubectl create secret --namespace "${SYSTEM_NAMESPACE}" generic strimzi-tls-secret \
    --from-literal=ca.crt="$STRIMZI_CRT" \
    --from-literal=user.crt="$TLSUSER_CRT" \
    --from-literal=user.key="$TLSUSER_KEY" \
    --from-literal=protocol="SSL" \
    --dry-run=client -o yaml | kubectl apply -n "${SYSTEM_NAMESPACE}" -f -
}

function create_sasl_secrets() {
  ca_cert_secret="my-cluster-cluster-ca-cert"
  sasl_user="my-sasl-user"

  echo "Waiting until secrets: ${ca_cert_secret}, ${sasl_user} exist"
  wait_until_object_exists secret ${ca_cert_secret} kafka
  wait_until_object_exists secret ${sasl_user} kafka

  echo "Creating SASL_SSL and SASL_PLAINTEXT Kafka secrets"

  STRIMZI_CRT=$(kubectl -n kafka get secret ${ca_cert_secret} --template='{{index .data "ca.crt"}}' | base64 --decode )
  SASL_PASSWD=$(kubectl -n kafka get secret ${sasl_user} --template='{{index .data "password"}}' | base64 --decode )

  kubectl create secret --namespace "${SYSTEM_NAMESPACE}" generic strimzi-sasl-secret \
    --from-literal=ca.crt="$STRIMZI_CRT" \
    --from-literal=password="$SASL_PASSWD" \
    --from-literal=user="my-sasl-user" \
    --from-literal=protocol="SASL_SSL" \
    --from-literal=sasl.mechanism="SCRAM-SHA-512" \
    --from-literal=saslType="SCRAM-SHA-512" \
    --dry-run=client -o yaml | kubectl apply -n "${SYSTEM_NAMESPACE}" -f -

  kubectl create secret --namespace "${SYSTEM_NAMESPACE}" generic strimzi-sasl-plain-secret \
    --from-literal=password="$SASL_PASSWD" \
    --from-literal=user="my-sasl-user" \
    --from-literal=protocol="SASL_PLAINTEXT" \
    --from-literal=sasl.mechanism="SCRAM-SHA-512" \
    --from-literal=saslType="SCRAM-SHA-512" \
    --dry-run=client -o yaml | kubectl apply -n "${SYSTEM_NAMESPACE}" -f -
}

wait_until_pods_running kafka || fail_test "Failed to start up a Kafka cluster"

create_sasl_secrets || fail_test "Failed to create SASL secrets"
create_tls_secrets || fail_test "Failed to create TLS secrets"
