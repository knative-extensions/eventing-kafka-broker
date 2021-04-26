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

# This script updates test/kafka/strimzi-cluster-operator.yaml
#
# After running this script you need to set the env variable KUBERNETES_SERVICE_DNS_DOMAIN
# to cluster.local to the strimzi-cluster-operator Deployment in order to use a different
# DNS suffix.
# `cluster.local` will be replaced to the generated one on GitHub Actions.

strimzi_version=$(curl https://github.com/strimzi/strimzi-kafka-operator/releases/latest | awk -F 'tag/' '{print $2}' | awk -F '"' '{print $1}' 2>/dev/null)

echo "Using Strimzi Version: ${strimzi_version}"

curl -L "https://github.com/strimzi/strimzi-kafka-operator/releases/download/${strimzi_version}/strimzi-cluster-operator-${strimzi_version}.yaml" |
  sed 's/namespace: .*/namespace: kafka/' > test/kafka/strimzi-cluster-operator.yaml
