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

# variables used:
# - SKIP_INITIALIZE (default: false) - skip cluster creation

source $(pwd)/vendor/knative.dev/test-infra/scripts/e2e-tests.sh
source $(pwd)/test/data-plane/library.sh
source $(pwd)/test/control-plane/library.sh

SKIP_INITIALIZE=${SKIP_INITIALIZE:-false}

EVENTING_CONFIG="./config"

function knative_setup() {
  knative_eventing "apply --strict"
  return $?
}

function knative_teardown() {
  knative_eventing "delete --ignore-not-found"
  return $?
}

function knative_eventing() {
  if ! is_release_branch; then
    echo ">> Install Knative Eventing from HEAD"
    pushd .
    cd "${GOPATH}" && mkdir -p src/knative.dev && cd src/knative.dev || fail_test "Failed to set up Eventing"
    git clone https://github.com/knative/eventing
    cd eventing || fail_test "Failed to set up Eventing"
    ko $1 -f "${EVENTING_CONFIG}"
    popd || fail_test "Failed to set up Eventing"
  else
    fail_test "Not ready for a release"
  fi
}

function test_setup() {
  ./test/kafka/kafka_setup.sh || fail_test "Failed to set up Kafka cluster"

  header "Data plane setup"
  data_plane_setup || fail_test "Failed to set up data plane components"

  header "Control plane setup"
  control_plane_setup || fail_test "Failed to set up control plane components"
}

function test_teardown() {
  header "Data plane teardown"
  data_plane_teardown || fail_test "Failed to tear down data plane components"

  header "Control plane teardown"
  control_plane_teardown || fail_test "Failed to tear down control plane components"
}
