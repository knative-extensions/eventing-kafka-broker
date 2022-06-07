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

if [ -z "${GOPATH:-}" ]; then
  export GOPATH=$(go env GOPATH)
fi

source $(dirname $0)/../vendor/knative.dev/hack/library.sh

go run "${REPO_ROOT_DIR}/vendor/knative.dev/pkg/configmap/hash-gen" "${REPO_ROOT_DIR}"/control-plane/config/eventing-kafka-broker/200-controller/100-config-tracing.yaml
go run "${REPO_ROOT_DIR}/vendor/knative.dev/pkg/configmap/hash-gen" "${REPO_ROOT_DIR}"/control-plane/config/eventing-kafka-broker/200-controller/100-config-logging.yaml
go run "${REPO_ROOT_DIR}/vendor/knative.dev/pkg/configmap/hash-gen" "${REPO_ROOT_DIR}"/control-plane/config/eventing-kafka-broker/200-controller/100-config-kafka-leader-election.yaml
go run "${REPO_ROOT_DIR}/vendor/knative.dev/pkg/configmap/hash-gen" "${REPO_ROOT_DIR}"/control-plane/config/eventing-kafka-broker/200-controller/100-config-kafka-features.yaml
go run "${REPO_ROOT_DIR}/vendor/knative.dev/pkg/configmap/hash-gen" "${REPO_ROOT_DIR}"/data-plane/config/broker/100-config-kafka-broker-data-plane.yaml
go run "${REPO_ROOT_DIR}/vendor/knative.dev/pkg/configmap/hash-gen" "${REPO_ROOT_DIR}"/data-plane/config/channel/100-config-kafka-channel-data-plane.yaml
go run "${REPO_ROOT_DIR}/vendor/knative.dev/pkg/configmap/hash-gen" "${REPO_ROOT_DIR}"/data-plane/config/sink/100-config-kafka-sink-data-plane.yaml
go run "${REPO_ROOT_DIR}/vendor/knative.dev/pkg/configmap/hash-gen" "${REPO_ROOT_DIR}"/data-plane/config/source/100-config-kafka-source-data-plane.yaml
