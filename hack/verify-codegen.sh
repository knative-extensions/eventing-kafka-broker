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

export GO111MODULE=on

source $(dirname $0)/../vendor/knative.dev/hack/library.sh

readonly TMP_DIFFROOT="$(mktemp -d "${REPO_ROOT_DIR}/../tmpdiffroot.XXXXXX")"

cleanup() {
  rm -rf "${TMP_DIFFROOT}"
}

trap "cleanup" EXIT SIGINT

cleanup

# Save working tree state
mkdir -p "${TMP_DIFFROOT}"

DIRS=(
  "/go.mod"
  "/go.sum"
  "/data-plane/contract/src"
  "/data-plane/core/src"
  "/data-plane/dispatcher/src"
  "/data-plane/receiver/src"
  "/data-plane/tests/src"
  "/control-plane/pkg/core/config"
  "/control-plane/pkg/apis"
  "/control-plane/pkg/client"
  "/vendor"
  "/third_party"
)

for d in "${DIRS[@]}"; do
  if [[ -f "${REPO_ROOT_DIR}${d}" ]]; then
    mkdir -p "$(dirname "${TMP_DIFFROOT}${d}")"
    cp -aR "${REPO_ROOT_DIR}${d}" "${TMP_DIFFROOT}${d}"
  else
    mkdir -p "${TMP_DIFFROOT}${d}"
    cp -aR "${REPO_ROOT_DIR}${d}"/* "${TMP_DIFFROOT}${d}"
  fi
done

"${REPO_ROOT_DIR}/hack/update-codegen.sh"

echo "Diffing ${REPO_ROOT_DIR} against freshly generated codegen"
ret=0

for d in "${DIRS[@]}"; do
  diff -Naupr --no-dereference "${REPO_ROOT_DIR}${d}" "${TMP_DIFFROOT}${d}" || ret=1
done

# Restore working tree state
for d in "${DIRS[@]}"; do
  rm -r "${REPO_ROOT_DIR}${d}"
done

cp -aR "${TMP_DIFFROOT}"/* "${REPO_ROOT_DIR}"

if [[ $ret -eq 0 ]]; then
  echo "${REPO_ROOT_DIR} up to date."
else
  echo "${REPO_ROOT_DIR} is out of date. Please run hack/update-codegen.sh"
  exit 1
fi
