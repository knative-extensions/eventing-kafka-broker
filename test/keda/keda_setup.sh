#!/usr/bin/env bash

# Copyright 2023 The Knative Authors
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

header "Applying KEDA Operator file"
kubectl apply --server-side --force-conflicts -f $(dirname $0)/../../third_party/keda/keda.yaml

# KEDA 2.21 defaults to 'enforce-audience' mode for service account tokens, which
# requires explicit audience mappings and breaks boundServiceAccountToken-based
# TriggerAuthentication used by our tests. Opt back into the legacy behaviour as a
# temporary compatibility measure until the integrations are migrated.
# See https://keda.sh/docs/2.21/migration/#service-account-token-audiences
header "Enabling KEDA legacy service account token mode"
kubectl patch deployment keda-operator -n keda --type=json \
  -p='[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--service-account-token-mode=legacy"}]'

wait_until_pods_running keda || fail_test "Failed to start up KEDA operator"
