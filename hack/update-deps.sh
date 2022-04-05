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

source $(dirname "$0")/../vendor/knative.dev/hack/library.sh

version=$(echo $@ | grep -o "\-\-release \S*" | awk '{print $2}' || echo "")
upgrade=$(echo $@ | grep '\-\-upgrade' || echo "")

function update_submodule() {
  echo "Pulling branch main"
  branch=${1}
  git fetch origin "${branch}":"${branch}" || return $?
  git checkout "origin/${branch}" || return $?
}

function update_eventing_submodule() {
  pushd $(dirname "$0")/../third_party/eventing

  if [ "${version}" = "" ] || [ "${version}" = "v9000.1" ]; then
    if [ "${upgrade}" != "" ]; then
      update_submodule "main" || return $?
    fi
  else
    major_minor=${version:1} # Remove 'v' prefix
    update_submodule "release-${major_minor}" || return $?
  fi

  popd
}

git submodule update --init --recursive

update_eventing_submodule || exit $?

git submodule update --init --recursive

go_update_deps "$@"

# Apply Git patches
git apply $(dirname "$0")/patches/*
