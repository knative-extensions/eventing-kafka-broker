#!/usr/bin/env bash

# Copyright 2023 The Knative Authors
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

# check that we have exactly one argument denoting which keda version to update to

if [[ "$#" -ne 1 ]]; then
	echo "Usage: $0 <keda_version>"
	exit 1
fi

keda_version=$1

echo "Using KEDA version: ${keda_version}"

curl -L "https://github.com/kedacore/keda/releases/download/v${keda_version}/keda-${keda_version}.yaml" \
	> $(dirname $0)/../third_party/keda/keda.yaml
