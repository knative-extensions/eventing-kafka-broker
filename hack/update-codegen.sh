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

source "$(dirname $0)"/../vendor/knative.dev/hack/codegen-library.sh
source "${CODEGEN_PKG}/kube_codegen.sh"

export PATH="$GOBIN:$PATH"

echo "=== Update Codegen for $MODULE_NAME"

# Compute _example hash for all configmaps.
group "Generating checksums for configmap _example keys"

"${REPO_ROOT_DIR}"/hack/update-checksums.sh

group "Kubernetes Codegen"

kube::codegen::gen_helpers \
  --boilerplate "${REPO_ROOT_DIR}/hack/boilerplate/boilerplate.go.txt" \
  "${REPO_ROOT_DIR}/control-plane/pkg/apis"

kube::codegen::gen_client \
  --boilerplate "${REPO_ROOT_DIR}/hack/boilerplate/boilerplate.go.txt" \
  --output-dir "${REPO_ROOT_DIR}/control-plane/pkg/client" \
  --output-pkg "knative.dev/eventing-kafka-broker/control-plane/pkg/client" \
  --with-watch \
  "${REPO_ROOT_DIR}/control-plane/pkg/apis"

group "Knative Codegen"

# Knative Injection
"${KNATIVE_CODEGEN_PKG}"/hack/generate-knative.sh "injection" \
  knative.dev/eventing-kafka-broker/control-plane/pkg/client knative.dev/eventing-kafka-broker/control-plane/pkg/apis \
  "eventing:v1alpha1 messaging:v1 messaging:v1beta1 sources:v1 sources:v1beta1 bindings:v1beta1 internalskafkaeventing:v1alpha1" \
  --go-header-file "${REPO_ROOT_DIR}"/hack/boilerplate/boilerplate.go.txt

# KEDA ships its own generated clientset/informers/listers, so we neither copy its
# apis nor regenerate a clientset. Generate only the thin knative injection wrapper
# on top of KEDA's upstream client. --disable-informer-init keeps the KEDA informers
# opt-in (we only use the injection client). Note the KEDA apis package still imports
# controller-runtime (KEDA is on the kubebuilder v3 layout), so binaries importing
# this wrapper must call flags.DropControllerRuntimeKubeconfigFlag() before sharedmain.
OUTPUT_PKG="knative.dev/eventing-kafka-broker/third_party/pkg/client/injection" \
"${KNATIVE_CODEGEN_PKG}"/hack/generate-knative.sh "injection" \
  github.com/kedacore/keda/v2/pkg/generated \
  github.com/kedacore/keda/v2/apis \
  "keda:v1alpha1" \
  --disable-informer-init \
  --go-header-file "${REPO_ROOT_DIR}"/hack/boilerplate/boilerplate.go.txt

group "Update deps post-codegen"

# Our GH Actions env doesn't have protoc, nor Java.
# For more details: https://github.com/knative-extensions/eventing-kafka-broker/pull/847#issuecomment-828562570
# Also: https://github.com/knative-extensions/knobots/runs/2609020026?check_suite_focus=true#step:6:291
if ! ${GITHUB_ACTIONS:-false}; then
  "${REPO_ROOT_DIR}"/hack/generate-proto.sh

  # Update Java third party file
  pushd data-plane
  ./mvnw -Dlicense.outputDirectory=. license:aggregate-add-third-party

  # Run maven command to apply spotless formatting
  ./mvnw spotless:apply
  popd

fi

"${REPO_ROOT_DIR}"/hack/update-deps.sh

# Update cert-manager and trust-manager manifests under third_party/cert-manager.
# We source the update_cert_manager function from the vendored eventing script
# (skipping its hardcoded self-invocation) so the versions are controlled here,
# independent of whatever versions upstream eventing pins.
cert_manager_installer="${REPO_ROOT_DIR}/vendor/knative.dev/eventing/hack/update-cert-manager.sh"

# shellcheck source=/dev/null
source <(grep -v '^update_cert_manager ' "${cert_manager_installer}")

update_cert_manager "v1.21.3" "v0.26.0"
