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

source $(pwd)/hack/label.sh

readonly CONTROL_PLANE_DIR=control-plane
readonly CONTROL_PLANE_CONFIG_DIR=${CONTROL_PLANE_DIR}/config
readonly KAFKA_SINK_CONFIG_DIR=${CONTROL_PLANE_CONFIG_DIR}/sink

readonly DATA_PLANE_LOGGING_CONFIG_DIR=data-plane/config

# Note: do not change this function name, it's used during releases.
function control_plane_setup() {
  ko resolve ${KO_FLAGS} -f "${CONTROL_PLANE_CONFIG_DIR}" | "${LABEL_YAML_CMD[@]}" >>"${EVENTING_KAFKA_CONTROL_PLANE_ARTIFACT}" &&
    ko resolve ${KO_FLAGS} -f "${KAFKA_SINK_CONFIG_DIR}" | "${LABEL_YAML_CMD[@]}" >>"${EVENTING_KAFKA_CONTROL_PLANE_ARTIFACT}"

  return $?
}
