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

set -o errexit
set -o nounset
set -o pipefail

source $(dirname $0)/../vendor/knative.dev/hack/codegen-library.sh

export PATH="$GOBIN:$PATH"

echo "=== Update Codegen for $MODULE_NAME"

# Compute _example hash for all configmaps.
group "Generating checksums for configmap _example keys"

${REPO_ROOT_DIR}/hack/update-checksums.sh

group "Kubernetes Codegen"

# generate the code with:
# --output-base    because this script should also be able to run inside the vendor dir of
#                  k8s.io/kubernetes. The output-base is needed for the generators to output into the vendor dir
#                  instead of the $GOPATH directly. For normal projects this can be dropped.
${CODEGEN_PKG}/generate-groups.sh "deepcopy,client,informer,lister" \
  knative.dev/eventing-kafka-broker/control-plane/pkg/client knative.dev/eventing-kafka-broker/control-plane/pkg/apis \
  "eventing:v1alpha1" \
  --go-header-file ${REPO_ROOT_DIR}/hack/boilerplate/boilerplate.go.txt

group "Knative Codegen"

# Knative Injection
${KNATIVE_CODEGEN_PKG}/hack/generate-knative.sh "injection" \
  knative.dev/eventing-kafka-broker/control-plane/pkg/client knative.dev/eventing-kafka-broker/control-plane/pkg/apis \
  "eventing:v1alpha1" \
  --go-header-file ${REPO_ROOT_DIR}/hack/boilerplate/boilerplate.go.txt

group "Update deps post-codegen"

# Our GH Actions env doesn't have protoc, nor Java.
# For more details: https://github.com/knative-sandbox/eventing-kafka-broker/pull/847#issuecomment-828562570
# Also: https://github.com/knative-sandbox/knobots/runs/2609020026?check_suite_focus=true#step:6:291
if ! ${GITHUB_ACTIONS:-false}; then
  ${REPO_ROOT_DIR}/hack/generate-proto.sh

  # Update Java third party file
  pushd data-plane
  ./mvnw -Dlicense.outputDirectory=. license:aggregate-add-third-party
  popd
fi

${REPO_ROOT_DIR}/hack/update-deps.sh
