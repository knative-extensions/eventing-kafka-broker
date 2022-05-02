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

source $(dirname $0)/../vendor/knative.dev/hack/release.sh
source $(dirname $0)/data-plane.sh
source $(dirname $0)/control-plane.sh
source $(dirname $0)/artifacts-env.sh

function fail() {
  echo "$1"
  exit 1
}

function build_release() {

  [ -f "${EVENTING_KAFKA_SOURCE_BUNDLE_ARTIFACT}" ] && rm "${EVENTING_KAFKA_SOURCE_BUNDLE_ARTIFACT}"
  [ -f "${EVENTING_KAFKA_POST_INSTALL_ARTIFACT}" ] && rm "${EVENTING_KAFKA_POST_INSTALL_ARTIFACT}"

  header "Control plane sourcev2 setup"
  control_plane_sourcev2_setup
  if [[ $? -ne 0 ]]; then
    fail "failed to setup control plane artifact"
  fi

  header "Data plane sourcev2 setup"
  data_plane_sourcev2_setup
  if [[ $? -ne 0 ]]; then
    fail "failed to create data plane artifact"
  fi

  {
    cat ${EVENTING_KAFKA_SOURCE_BUNDLE_ARTIFACT}
  } >>${EVENTING_KAFKA_ARTIFACT}

  export ARTIFACTS_TO_PUBLISH=(
    "${EVENTING_KAFKA_ARTIFACT}"
    "${EVENTING_KAFKA_SOURCE_BUNDLE_ARTIFACT}"
    "${EVENTING_KAFKA_SOURCE_PROMETHEUS_OPERATOR_ARTIFACT}"
    "${EVENTING_KAFKA_POST_INSTALL_ARTIFACT}"
  )

  # ARTIFACTS_TO_PUBLISH has to be a string, not an array.
  # shellcheck disable=SC2178
  # shellcheck disable=SC2124
  export ARTIFACTS_TO_PUBLISH="${ARTIFACTS_TO_PUBLISH[@]}"
}

main $@
