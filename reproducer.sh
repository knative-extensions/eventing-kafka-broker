#!/usr/bin/env bash

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

function wait_for_cloudevent {
  local ns count
  ns=$1

  count=0

  while true
  do
    podname=$(kubectl get pod -n $ns -o name -l app=event-display 2>/dev/null)
    if [ -n $podname ]
    then
      kubectl logs -n $ns $podname 2>/dev/null | grep -q "☁️  cloudevents.Event" && break
    fi
    sleep 1
    count=$((count + 1))

    if [ $count -gt 1 ]
    then
      echo "Waiting for first events for $ns for ${count}s"

      if [ $count -gt 60 ]
      then
        echo "takes too long to receive events"
        exit 1
      fi
    fi
  done
}

function run {
  i=$1

  app foo$i | kubectl apply -f -
  kubectl wait kafkachannel --timeout=60s -n foo$i channel --for=condition=Ready=True || exit 1
  kubectl wait subscription --timeout=60s -n foo$i event-display --for=condition=Ready=True || exit 1
  wait_for_cloudevent foo$i
  kubectl delete namespace foo$i
}

export -f run
export -f app
export -f wait_for_cloudevent

if [[ ${PARALLEL:-""} != "" ]]; then
  for i in {1..200}
  do
    timeout -k 60s 60s bash -c "run $i" &
    pids[${i}]=$!
  done

  wait "${pids[@]}" || exit $?
else
  for i in {1..200}
  do
    run "$i"
  done
fi
