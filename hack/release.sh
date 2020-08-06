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

source $(dirname $0)/../vendor/knative.dev/test-infra/scripts/release.sh
source $(dirname $0)/../test/data-plane/library.sh
source $(dirname $0)/../test/control-plane-plane/library.sh

export EVENTING_KAFKA_BROKER_ARTIFACT="eventing-kafka-broker.yaml"

function build_release() {
  data_plane_setup && control_plane_setup
  if [ -n $? ]; then
    export ARTIFACTS_TO_PUBLISH=(${EVENTING_KAFKA_BROKER_ARTIFACT})
  fi
}

main $@
