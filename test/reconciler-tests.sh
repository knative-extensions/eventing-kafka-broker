#!/usr/bin/env bash

source $(dirname "$0")/e2e-common.sh
export BROKER_TEMPLATES=./templates/kafka-broker

if ! ${SKIP_INITIALIZE}; then
	initialize "$@" --num-nodes=5
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

export_logs_continuously

kubectl apply -f ./test/e2e_new/config/features.yaml

header "Running tests"

if [ "${EVENTING_KAFKA_BROKER_CHANNEL_AUTH_SCENARIO:-""}" != "" ]; then
	# if this flag exists, only test Kafka channel scenarios with auth
	$(dirname "$0")/channel-reconciler-tests.sh || fail_test "Failed to execute KafkaChannel tests"
	success
fi

if [[ -z "${BROKER_CLASS}" ]]; then
	fail_test "Broker class is not defined. Specify it with 'BROKER_CLASS' env var."
else
	echo "BROKER_CLASS is set to '${BROKER_CLASS}'. Running tests for that broker class."
fi

if [ "${BROKER_CLASS}" == "KafkaNamespaced" ]; then
	# if flag exists, only test tests that are relevant to namespaced KafkaBroker
	echo "BROKER_CLASS is set to 'KafkaNamespaced'. Only running the relevant tests."
	go_test_e2e -timeout=1h ./test/e2e_new/... || fail_test "E2E (new) suite failed"
	success
fi

go_test_e2e -timeout=1h ./test/e2e_new/... || fail_test "E2E (new) suite failed"

go_test_e2e -tags=e2e,cloudevents -timeout=1h ./test/e2e_new_channel/... || fail_test "E2E (new - KafkaChannel) suite failed"

go_test_e2e -tags=deletecm ./test/e2e_new/... || fail_test "E2E (new deletecm) suite failed"

echo "Running 'Implicit CA' E2E Reconciler tests with knative-eventing-bundle mounted"

mount_knative_eventing_bundle

go_test_e2e -tags=e2e,implicitca -timeout=5m ./test/e2e_new -run ImplicitCA || fail_test "E2E (new - implicitca) suite failed"

unmount_knative_eventing_bundle

echo "Running E2E Reconciler tests with consumergroup id template changed"

kubectl apply -f "$(dirname "$0")/config-kafka-features/new-cg-id.yaml"

go_test_e2e -tags=e2e -timeout=15m ./test/e2e_new -run TestTriggerUsesConsumerGroupIDFromTemplate

kubectl apply -f "$(dirname "$0")/config-kafka-features/restore-cg-id.yaml"

echo "Running E2E Reconciler Tests with strict transport encryption"

kubectl apply -Rf "$(dirname "$0")/config-transport-encryption"

go_test_e2e -timeout=1h ./test/e2e_new -run TLS || fail_test

echo "Running E2E Reconciler OIDC and AuthZ Tests"

kubectl apply -Rf "$(dirname "$0")/config-auth"

# Wait until we can actually create an EventPolicy (proof webhook has the right config)
echo "Waiting for webhook to reload OIDC config..."
for i in {1..60}; do
  if kubectl create -f - <<EOF 2>/dev/null
apiVersion: eventing.knative.dev/v1alpha1
kind: EventPolicy
metadata:
  name: config-readiness-test
  namespace: knative-eventing
spec:
  to:
    - ref:
        apiVersion: v1
        kind: Service
        name: test
EOF
  then
    kubectl delete eventpolicy config-readiness-test -n knative-eventing
    echo "Webhook ready with OIDC config enabled"
    break
  fi
  echo "Attempt $i: Webhook not ready yet, waiting..."
  sleep 2
done

go_test_e2e -timeout=1h ./test/e2e_new -run "OIDC|AuthZ" || fail_test

if ! ${LOCAL_DEVELOPMENT}; then
	go_test_e2e -tags=sacura -timeout=40m ./test/e2e/... || fail_test "E2E (sacura) suite failed"
fi

success
