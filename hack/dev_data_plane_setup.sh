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
# create a docker-based development setup

# generate protobuf code
./proto/hack/generate_proto || exit 1

# start a docker container with maven installed.
# usage:
# 1. cd /app/data-plane
# 2. mvn <command-to-execute>
docker run -ti --rm \
  -v ~/.m2:/var/maven/.m2 \
  -v "$(pwd)":/app \
  --user "$(id -g)":"$(id -u)" \
  -e MAVEN_CONFIG=/var/maven/.m2 maven:3.6.3-openjdk-14 /bin/bash
