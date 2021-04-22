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

# This script runs the presubmit tests; it is started by prow for each PR.
# For convenience, it can also be executed manually.
# Running the script without parameters, or with the --all-tests
# flag, causes all tests to be executed, in the right order.
# Use the flags --build-tests, --unit-tests and --integration-tests
# to run a specific set of tests.

export GO111MODULE=on
export DISABLE_MD_LINTING=1

source $(dirname $0)/../vendor/knative.dev/hack/presubmit-tests.sh
source $(dirname $0)/e2e-common.sh

function fail_test() {
  header "$1"
  exit 1
}

function build_tests() {
  header "Running control plane build tests"
  default_build_test_runner || fail_test "Control plane build tests failed"
}

function unit_tests() {
  header "Running control plane unit tests"
  #default_unit_test_runner || fail_test "Control plane unit tests failed"

  header "Running data plane unit tests"
  data_plane_unit_tests || fail_test "Data plane unit tests failed"
}

main $@
