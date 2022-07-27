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

readonly CONTROL_PLANE_CONFIG_DIR=control-plane/config/eventing-kafka-broker
readonly CONTROL_PLANE_POST_INSTALL_CONFIG_DIR=control-plane/config/post-install
readonly CONTROL_PLANE_SOURCE_CONFIG_DIR=control-plane/config/eventing-kafka-source

# Note: do not change this function name, it's used during releases.
function control_plane_setup() {
  ko resolve ${KO_FLAGS} -Rf "${CONTROL_PLANE_CONFIG_DIR}" | "${LABEL_YAML_CMD[@]}" >>"${EVENTING_KAFKA_CONTROL_PLANE_ARTIFACT}" || return $?
  ko resolve ${KO_FLAGS} -Rf "${CONTROL_PLANE_POST_INSTALL_CONFIG_DIR}" | "${LABEL_YAML_CMD[@]}" >>"${EVENTING_KAFKA_POST_INSTALL_ARTIFACT}" || return $?

  # Replace the references to dispatcher and receiver images in controller env vars
  replace_images "${EVENTING_KAFKA_CONTROL_PLANE_ARTIFACT}"
}

function control_plane_source_setup() {
  ko resolve ${KO_FLAGS} -Rf "${CONTROL_PLANE_POST_INSTALL_CONFIG_DIR}" | "${LABEL_YAML_CMD[@]}" >>"${EVENTING_KAFKA_POST_INSTALL_ARTIFACT}" || return $?
  ko resolve ${KO_FLAGS} -Rf "${CONTROL_PLANE_SOURCE_CONFIG_DIR}" | "${LABEL_YAML_CMD[@]}" >>"${EVENTING_KAFKA_SOURCE_BUNDLE_ARTIFACT}" || return $?
}
