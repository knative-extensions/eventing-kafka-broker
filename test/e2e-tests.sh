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

source $(dirname $0)/../vendor/knative.dev/test-infra/scripts/e2e-tests.sh
source $(dirname $0)/data-plane/library.sh

# If gcloud is not available make it a no-op, not an error.
which gcloud &>/dev/null || gcloud() { echo "[ignore-gcloud $*]" 1>&2; }

# Use GNU tools on macOS. Requires the 'grep' and 'gnu-sed' Homebrew formulae.
if [ "$(uname)" == "Darwin" ]; then
  sed=gsed
  grep=ggrep
fi

#function knative_setup() {
# set up eventing
#}

function test_setup() {
  ./test/kafka/kafka_setup.sh || fail_test "Failed to setup Kafka cluster"

  header "Data plane setup"
  data_plane_setup || fail_test "Failed to setup data-plane components"
}

function test_teardown() {
  header "Data plane teardown"
  data_plane_teardown
  return $?
}

initialize $@ --skip-istio-addon

data_plane_integration_tests

success
