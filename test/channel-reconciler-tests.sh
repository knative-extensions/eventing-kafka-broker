#!/usr/bin/env bash

source $(dirname $0)/e2e-common.sh

go_test_e2e -tags=e2e,cloudevents -timeout=1h ./test/e2e_new_channel/... || fail_test "E2E (new - KafkaChannel) suite failed"

function app {
  local ns
  ns=$1
  cat <<EOF
---
apiVersion: v1
kind: Namespace
metadata:
  labels:
    bindings.knative.dev/include: "true"
  name: $ns
---
apiVersion: messaging.knative.dev/v1beta1
kind: KafkaChannel
metadata:
  name: channel
  namespace: $ns
spec:
  numPartitions: 32
  replicationFactor: 3
---
apiVersion: v1
kind: Service
metadata:
  name: event-display
  namespace: $ns
spec:
  ports:
  - port: 80
    protocol: TCP
    targetPort: 8080
  selector:
    app: event-display
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: event-display
  namespace: $ns
  labels:
   app: event-display
spec:
  selector:
    matchLabels:
      app: event-display
  template:
    metadata:
      labels:
        app: event-display
    spec:
      containers:
      - name: event-display
        image: quay.io/openshift-knative/knative-eventing-sources-event-display:v0.13.2
---
apiVersion: messaging.knative.dev/v1
kind: Subscription
metadata:
  name: event-display
  namespace: $ns
spec:
  channel:
    apiVersion: messaging.knative.dev/v1beta1
    kind: KafkaChannel
    name: channel
  delivery:
    backoffDelay: PT1S
    backoffPolicy: linear
    retry: 1000
  subscriber:
    ref:
      apiVersion: v1
      kind: Service
      name: event-display
---
apiVersion: sources.knative.dev/v1
kind: SinkBinding
metadata:
  name: bind-heartbeat
  namespace: $ns
spec:
  subject:
    apiVersion: apps/v1
    kind: Deployment
    selector:
      matchLabels:
        app: heartbeat
  sink:
    ref:
      apiVersion: messaging.knative.dev/v1beta1
      kind: KafkaChannel
      name: channel
  ceOverrides:
    extensions:
      sink: bound
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: hearbeat
  namespace: $ns
  labels:
   app: heartbeat
   bindings.knative.dev/include: "true"
spec:
  selector:
    matchLabels:
      app: heartbeat
  template:
    metadata:
      labels:
        app: heartbeat
    spec:
      containers:
      - name: single-heartbeat
        image: quay.io/openshift-knative/knative-eventing-sources-heartbeats:v0.13.2
        args:
        - --period=1
        env:
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: POD_NAMESPACE
          valueFrom:
            fieldRef:
              fieldPath: metadata.namespace
EOF
}

function create {
  kubectl create -f -
}

function wait_for_cloudevent {
  local ns count
  ns=$1

  count=0

  while true; do
    podname=$(kubectl get pod -n $ns -o name -l app=event-display 2>/dev/null)
    if [ -n $podname ]; then
      kubectl logs -n $ns $podname 2>/dev/null | grep -q "☁️  cloudevents.Event" && break
    fi
    sleep 1
    count=$((count + 1))

    if [ $count -gt 1 ]; then
      echo "Waiting for first events for $ns for ${count}s"

      if [ $count -gt 60 ]; then
        echo "takes too long to receive events"
        exit 1
      fi
    fi
  done
}

i=0

while [ $i -lt 100 ]; do
  i=$((i + 1))
  app foo$i | create
  wait_for_cloudevent foo$i

  kubectl delete namespace foo$i

done
