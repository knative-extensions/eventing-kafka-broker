#!/usr/bin/env bash

# After running this script you need to add the env variable KUBERNETES_SERVICE_DNS_DOMAIN
# to .cluster.local to the strimzi-cluster-operator Deployment in order to use a different
# DNS suffix.
# `.cluster.local` will be replaced to the generated one on GitHub Actions.

strimzi_version=$(curl https://github.com/strimzi/strimzi-kafka-operator/releases/latest | awk -F 'tag/' '{print $2}' | awk -F '"' '{print $1}' 2>/dev/null)

echo "Using Strimzi Version: ${strimzi_version}"

curl -L "https://github.com/strimzi/strimzi-kafka-operator/releases/download/${strimzi_version}/strimzi-cluster-operator-${strimzi_version}.yaml" |
  sed 's/namespace: .*/namespace: kafka/' > test/kafka/strimzi-cluster-operator.yaml
