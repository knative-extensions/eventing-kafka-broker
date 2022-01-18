#!/usr/bin/env bash

source $(dirname $0)/../test/e2e-common.sh

# Add helm repository.
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update

cat <<EOF | kubectl apply -f -
kind: Namespace
apiVersion: v1
metadata:
  name: knative-monitoring
  labels:
    app.kubernetes.io/name: knative-monitoring
EOF

# Install helm chart.
helm upgrade --install kube-prometheus-stack prometheus-community/kube-prometheus-stack \
  --values hack/artifacts/kube-prometheus-stack-values.yaml \
  --version 30.0.1 \
  --wait --wait-for-jobs

header "Building Monitoring artifacts"
build_monitoring_artifacts || fail_test "Failed to create monitoring artifacts"

wait_until_pods_running knative-monitoring || fail_test "Knative Monitoring did not come up"

kubectl apply -f "${EVENTING_KAFKA_CONTROL_PLANE_PROMETHEUS_OPERATOR_ARTIFACT}" || exit 1
kubectl apply -f "${EVENTING_KAFKA_SOURCE_PROMETHEUS_OPERATOR_ARTIFACT}" || exit 1
kubectl apply -f "${EVENTING_KAFKA_BROKER_PROMETHEUS_OPERATOR_ARTIFACT}" || exit 1
kubectl apply -f "${EVENTING_KAFKA_CHANNEL_PROMETHEUS_OPERATOR_ARTIFACT}" || exit 1
kubectl apply -f "${EVENTING_KAFKA_SINK_PROMETHEUS_OPERATOR_ARTIFACT}" || exit 1
