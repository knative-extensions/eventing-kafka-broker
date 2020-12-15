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

# Update release labels if this is a tagged release
if [[ -n "${TAG}" ]]; then
  echo "Tagged release, updating release labels to kafka.eventing.knative.dev/release: \"${TAG}\""
  export LABEL_YAML_CMD=(sed -e "s|kafka.eventing.knative.dev/release: devel|kafka.eventing.knative.dev/release: \"${TAG}\"|")
else
  echo "Untagged release, will NOT update release labels"
  export LABEL_YAML_CMD=(cat)
fi
