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
source $(dirname $0)/../test/data-plane/library.sh
source $(dirname $0)/../test/control-plane/library.sh

export EVENTING_KAFKA_CONTROL_PLANE_ARTIFACT="eventing-kafka-controller.yaml"
export EVENTING_KAFKA_BROKER_ARTIFACT="eventing-kafka-broker.yaml"
export EVENTING_KAFKA_SINK_ARTIFACT="eventing-kafka-sink.yaml"

function fail() {
  echo "$1"
  exit 1
}

function build_release() {

  [ -f "${EVENTING_KAFKA_CONTROL_PLANE_ARTIFACT}" ] && rm "${EVENTING_KAFKA_CONTROL_PLANE_ARTIFACT}"
  [ -f "${EVENTING_KAFKA_BROKER_ARTIFACT}" ] && rm "${EVENTING_KAFKA_BROKER_ARTIFACT}"
  [ -f "${EVENTING_KAFKA_SINK_ARTIFACT}" ] && rm "${EVENTING_KAFKA_SINK_ARTIFACT}"

  control_plane_setup
  if [[ $? -ne 0 ]]; then
    fail "failed to setup control plane artifact"
  fi

  data_plane_setup
  if [[ $? -ne 0 ]]; then
    fail "failed to create data plane artifact"
  fi

  export ARTIFACTS_TO_PUBLISH=("${EVENTING_KAFKA_CONTROL_PLANE_ARTIFACT} ${EVENTING_KAFKA_BROKER_ARTIFACT} ${EVENTING_KAFKA_SINK_ARTIFACT}")
}

main $@
