#!/usr/bin/env bash

# Copyright 2021 The Knative Authors
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
# - SKIP_INITIALIZE (default: false) - skip cluster creation.

readonly SKIP_INITIALIZE=${SKIP_INITIALIZE:-false}

source $(dirname $0)/e2e-common.sh

# Override test_setup from e2e-common since we don't want to apply the latest release
# before running the upgrade test.
function test_setup() {
  build_components_from_source || return $?

  # Apply test configurations, and restart data plane components (we don't have hot reload)
  ko apply -f ./test/config/ || fail_test "Failed to apply test configurations"
}

if ! ${SKIP_INITIALIZE}; then
  initialize $@ --skip-istio-addon
  save_release_artifacts || fail_test "Failed to save release artifacts"
fi

set -Eeuo pipefail

TIMEOUT=${TIMEOUT:-60m}
GO_TEST_VERBOSITY="${GO_TEST_VERBOSITY:-standard-verbose}"

EVENTING_KAFKA_BROKER_UPGRADE_TESTS_FINISHEDSLEEP="5m" \
go_test_e2e -v \
  -tags=upgrade \
  -timeout="${TIMEOUT}" \
  ./test/upgrade \
  || fail_test

success
