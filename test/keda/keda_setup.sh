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
kubectl apply -f $(dirname $0)/../../third_party/keda/keda.yaml

wait_until_pods_running keda || fail_test "Failed to start up KEDA operator"
