#!/usr/bin/env bash
set -e

strimzi_version=$(curl https://github.com/strimzi/strimzi-kafka-operator/releases/latest | awk -F 'tag/' '{print $2}' | awk -F '"' '{print $1}' 2>/dev/null)

header "Using Strimzi Version: ${strimzi_version}"
header "Strimzi install"

kubectl create namespace kafka

curl -L "https://github.com/strimzi/strimzi-kafka-operator/releases/download/${strimzi_version}/strimzi-cluster-operator-${strimzi_version}.yaml" |
  sed 's/namespace: .*/namespace: kafka/' |
  kubectl -n kafka apply -f -

header "Applying Strimzi Cluster file"
kubectl -n kafka apply -f "https://raw.githubusercontent.com/strimzi/strimzi-kafka-operator/${strimzi_version}/examples/kafka/kafka-ephemeral-single.yaml"
