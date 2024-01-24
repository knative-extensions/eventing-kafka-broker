#!/usr/bin/env bash

# Copyright © 2018 Knative Authors (knative-dev@googlegroups.com)
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

set -e

TIME=$(date +%s)

run_java_filter_benchmarks_for_class() {
  CLASS=$1

  echo "Running benchmarks for ${CLASS}"

  java -Dlogback.configurationFile=${SCRIPT_DIR}/resources/config-logging.xml \
    -jar "${SCRIPT_DIR}/target/benchmarks.jar" -prof gc $CLASS 2>&1 | tee "${SCRIPT_DIR}/output/${CLASS}.${TIME}.out.txt"

  echo "Successfully ran benchmarks for ${CLASS}!\n\nThe results can be found at ${SCRIPT_DIR}/output/${CLASS}.${TIME}.out.txt"
}

SCRIPT_DIR=$(cd -- "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

DATA_PLANE_DIR="${SCRIPT_DIR}/../"

# check if the benchmark class exists

if [[ ! -z $1 ]]; then
  FOUND=0
  while IFS="" read -r p || [[ -n "$p" ]]; do
    if [[ "$1" == "$p" ]]; then
      FOUND=1
      break
    fi
  done <"${SCRIPT_DIR}/resources/filter-class-list.txt"
  if [[ "$FOUND" != 1 ]]; then
    echo "Please provide a valid class name for a filter benchmark"
    exit 1
  fi
fi

pushd ${DATA_PLANE_DIR} || return $?

# build only benchmarks and it's dependents - skip aggregating licenses as it will be missing licenses due to only
# building some projects
./mvnw clean package -DskipTests -Dlicense.skipAggregateDownloadLicenses -Dlicense.skipAggregateAddThirdParty -P no-release -pl benchmarks -am

popd || return $?

mkdir -p "${SCRIPT_DIR}/output"

if [[ -z $1 ]]; then
  while IFS="" read -r p || [ -n "$p" ]; do
    run_java_filter_benchmarks_for_class "$p"
  done <"${SCRIPT_DIR}/resources/filter-class-list.txt"
else
  run_java_filter_benchmarks_for_class $1
fi
