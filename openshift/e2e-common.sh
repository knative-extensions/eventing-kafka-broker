#!/usr/bin/env bash

export EVENTING_NAMESPACE="${EVENTING_NAMESPACE:-knative-eventing}"
export SYSTEM_NAMESPACE=$EVENTING_NAMESPACE
export ZIPKIN_NAMESPACE=$EVENTING_NAMESPACE
export KNATIVE_DEFAULT_NAMESPACE=$EVENTING_NAMESPACE
export EVENTING_KAFKA_BROKER_TEST_IMAGE_TEMPLATE=$(cat <<-END
{{- with .Name }}
{{- if eq . "event-sender"}}$KNATIVE_EVENTING_KAFKA_BROKER_TEST_EVENT_SENDER{{end -}}
{{- if eq . "heartbeats"}}$KNATIVE_EVENTING_KAFKA_BROKER_TEST_HEARTBEATS{{end -}}
{{- if eq . "recordevents"}}$KNATIVE_EVENTING_KAFKA_BROKER_TEST_RECORDEVENTS{{end -}}
{{- if eq . "print"}}$KNATIVE_EVENTING_KAFKA_BROKER_TEST_PRINT{{end -}}
{{- if eq . "performance"}}$KNATIVE_EVENTING_KAFKA_BROKER_TEST_PERFORMANCE{{end -}}
{{- if eq . "event-flaker"}}$KNATIVE_EVENTING_KAFKA_BROKER_TEST_EVENT_FLAKER{{end -}}
{{- if eq . "event-library"}}$KNATIVE_EVENTING_KAFKA_BROKER_TEST_EVENT_LIBRARY{{end -}}
{{- if eq . "committed-offset"}}$KNATIVE_EVENTING_KAFKA_BROKER_TEST_COMMITTED_OFFSET{{end -}}
{{- if eq . "consumer-group-lag-provider-test"}}$KNATIVE_EVENTING_KAFKA_BROKER_TEST_CONSUMER_GROUP_LAG_PROVIDER_TEST{{end -}}
{{- if eq . "kafka-consumer"}}$KNATIVE_EVENTING_KAFKA_BROKER_TEST_KAFKA_CONSUMER{{end -}}
{{- if eq . "partitions-replication-verifier"}}$KNATIVE_EVENTING_KAFKA_BROKER_TEST_PARTITIONS_REPLICATION_VERIFIER{{end -}}
{{- if eq . "request-sender"}}$KNATIVE_EVENTING_KAFKA_BROKER_TEST_REQUEST_SENDER{{end -}}
{{end -}}
END
)

function scale_up_workers(){
  local cluster_api_ns="openshift-machine-api"

  oc get machineset -n ${cluster_api_ns} --show-labels

  # Get the name of the first machineset that has at least 1 replica
  local machineset
  machineset=$(oc get machineset -n ${cluster_api_ns} -o custom-columns="name:{.metadata.name},replicas:{.spec.replicas}" | grep " 1" | head -n 1 | awk '{print $1}')
  # Bump the number of replicas to 6 (+ 1 + 1 == 8 workers)
  oc patch machineset -n ${cluster_api_ns} "${machineset}" -p '{"spec":{"replicas":6}}' --type=merge
  wait_until_machineset_scales_up ${cluster_api_ns} "${machineset}" 6
}

# Waits until the machineset in the given namespaces scales up to the
# desired number of replicas
# Parameters: $1 - namespace
#             $2 - machineset name
#             $3 - desired number of replicas
function wait_until_machineset_scales_up() {
  echo -n "Waiting until machineset $2 in namespace $1 scales up to $3 replicas"
  for _ in {1..150}; do  # timeout after 15 minutes
    local available
    available=$(oc get machineset -n "$1" "$2" -o jsonpath="{.status.availableReplicas}")
    if [[ ${available} -eq $3 ]]; then
      echo -e "\nMachineSet $2 in namespace $1 successfully scaled up to $3 replicas"
      return 0
    fi
    echo -n "."
    sleep 6
  done
  echo - "Error: timeout waiting for machineset $2 in namespace $1 to scale up to $3 replicas"
  return 1
}

# Loops until duration (car) is exceeded or command (cdr) returns non-zero
function timeout() {
  SECONDS=0; TIMEOUT=$1; shift
  while eval $*; do
    sleep 5
    [[ $SECONDS -gt $TIMEOUT ]] && echo "ERROR: Timed out" && return 1
  done
  return 0
}

function kafka_setup() {
  echo ">> Prepare to deploy Strimzi"
  ./test/kafka/kafka_setup.sh || fail_test "Failed to set up Kafka cluster"
}

function install_serverless(){
  header "Installing Serverless Operator"
  local operator_dir=/tmp/serverless-operator
  local failed=0
  git clone --branch main https://github.com/openshift-knative/serverless-operator.git $operator_dir || return 1
  # unset OPENSHIFT_BUILD_NAMESPACE (old CI) and OPENSHIFT_CI (new CI) as its used in serverless-operator's CI
  # environment as a switch to use CI built images, we want pre-built images of k-s-o and k-o-i
  unset OPENSHIFT_BUILD_NAMESPACE
  unset OPENSHIFT_CI
  pushd $operator_dir

  ./hack/install.sh && header "Serverless Operator installed successfully" || failed=1
  popd
  return $failed
}

function install_knative_kafka() {
  header "Set Kafka as default Broker"
  cat <<-EOF | oc apply -f -
apiVersion: operator.knative.dev/v1alpha1
kind: KnativeEventing
metadata:
  name: knative-eventing
  namespace: knative-eventing
spec:
  defaultBrokerClass: Kafka
  config:
    config-br-defaults:
      default-br-config: |
        clusterDefault:
          apiVersion: v1
          kind: ConfigMap
          name: kafka-broker-config
          namespace: knative-eventing
---
EOF

  header "Installing Knative Kafka Control Plane"

  CP_RELEASE_YAML="openshift/release/knative-eventing-kafka-broker-cp-ci.yaml"

  sed -i -e "s|registry.ci.openshift.org/openshift/knative-.*:knative-eventing-kafka-broker-kafka-controller|${KNATIVE_EVENTING_KAFKA_BROKER_KAFKA_CONTROLLER}|g" ${CP_RELEASE_YAML}
  sed -i -e "s|registry.ci.openshift.org/openshift/knative-.*:knative-eventing-kafka-broker-webhook-kafka|${KNATIVE_EVENTING_KAFKA_BROKER_WEBHOOK_KAFKA}|g"       ${CP_RELEASE_YAML}

  oc apply -f ${CP_RELEASE_YAML}
  wait_until_pods_running $EVENTING_NAMESPACE || return 1

  header "Installing Knative Kafka Data Plane"

  DP_RELEASE_YAML="openshift/release/knative-eventing-kafka-broker-dp-ci.yaml"

  sed -i -e "s|registry.ci.openshift.org/openshift/knative-.*:knative-eventing-kafka-broker-dispatcher|${KNATIVE_EVENTING_KAFKA_BROKER_DISPATCHER}|g" ${DP_RELEASE_YAML}
  sed -i -e "s|registry.ci.openshift.org/openshift/knative-.*:knative-eventing-kafka-broker-receiver|${KNATIVE_EVENTING_KAFKA_BROKER_RECEIVER}|g"     ${DP_RELEASE_YAML}
  sed -i -e "s|registry.ci.openshift.org/openshift/knative-.*:knative-eventing-kafka-broker-receiver|${KNATIVE_EVENTING_KAFKA_SINK_RECEIVER}|g"       ${DP_RELEASE_YAML}

  oc apply -f ${DP_RELEASE_YAML}
  wait_until_pods_running $EVENTING_NAMESPACE || return 1
}

function run_e2e_tests(){

  go_test_e2e -timeout=30m ./test/e2e/ \
    -imagetemplate "${TEST_IMAGE_TEMPLATE}" || fail_test "E2E suite failed"
}

function run_conformance_tests(){
  go_test_e2e -timeout=30m ./test/e2e/conformance \
    -imagetemplate "${TEST_IMAGE_TEMPLATE}" || fail_test "E2E conformance suite failed"
}

function run_e2e_new_tests(){
  go_test_e2e -timeout=30m ./test/e2e_new || fail_test "E2E (new) suite failed"
}
