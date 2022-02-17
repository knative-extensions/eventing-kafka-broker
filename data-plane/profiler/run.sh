#!/usr/bin/env bash
#
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
#

set -e

# Set runtime variables
# echo 1 > /proc/sys/kernel/perf_event_paranoid
# echo 0 > /proc/sys/kernel/kptr_restrict

EVENT=${EVENT:-alloc} # which event to trace (cpu, alloc, lock, cache-misses etc.)
SUFFIX=${SUFFIX:-""}  # file output suffix.

perf_event_paranoid_value=$(cat /proc/sys/kernel/perf_event_paranoid)
kptr_restrict_value=$(cat /proc/sys/kernel/kptr_restrict)
if [[ $perf_event_paranoid_value -ne "1" ]]; then
  echo "Execute: echo 1 > /proc/sys/kernel/perf_event_paranoid - value is: $perf_event_paranoid_value"
  sudo sh -c 'echo 1 > /proc/sys/kernel/perf_event_paranoid' || exit 1
fi

if [[ $kptr_restrict_value -ne "0" ]]; then
  echo "Execute: echo 0 > /proc/sys/kernel/kptr_restrict - value is $kptr_restrict_value"
  sudo sh -c 'echo 0 > /proc/sys/kernel/kptr_restrict' || exit 1
fi

echo "Profiling event ${EVENT}"

PROJECT_ROOT_DIR=$(dirname $0)/..
RESOURCES_DIR="$(dirname $0)"/resources
ASYNC_PROFILER_URL="https://github.com/jvm-profiling-tools/async-profiler/releases/download/v2.6/async-profiler-2.6-linux-x64.tar.gz"
KAFKA_URL="https://archive.apache.org/dist/kafka/2.6.0/kafka_2.13-2.6.0.tgz"
LOG_DIR=${LOG_DIR:-"/tmp/eventing-kafka-broker-logs/profiler"}

rm -rf "${LOG_DIR}" && mkdir -p "${LOG_DIR}"

echo "Project root dir: ${PROJECT_ROOT_DIR}"
echo "Resource dir: ${RESOURCES_DIR}"
echo "Async profiler URL: ${ASYNC_PROFILER_URL}"
echo "Kafka URL: ${KAFKA_URL}"

# Build the data plane.
cd "${PROJECT_ROOT_DIR}" && ./mvnw package -DskipTests -Dlicense.skip -Deditorconfig.skip -B -U --no-transfer-progress && cd - || exit 1

# Download async profiler.
rm -rf async-profiler
rm -rf async-profiler.tgz
mkdir async-profiler
wget -O - ${ASYNC_PROFILER_URL} >async-profiler.tgz
tar xzvf async-profiler.tgz -C async-profiler --strip-components=1

# Download Apache Kafka.
rm -rf kafka
rm -rf kafka.tgz
rm -rf /tmp/kafka-logs/
rm -rf /tmp/zookeeper/
mkdir kafka
wget -O - ${KAFKA_URL} >kafka.tgz
tar xzvf kafka.tgz -C kafka --strip-components=1

# Start Zookeeper and Kafka.
cd kafka || exit 1
./bin/zookeeper-server-start.sh config/zookeeper.properties >"${LOG_DIR}/zookeeper.log" &
zookeeper_pid=$!
./bin/kafka-server-start.sh config/server.properties >"${LOG_DIR}/kafka.log" &
kafka_pid=$!

# Create our Kafka topic.
./bin/kafka-topics.sh --create --topic attack-ingress-single --partitions 10 --bootstrap-server localhost:9092
# Back to our previous dir
cd ..

# Define expected env variables.
export PRODUCER_CONFIG_FILE_PATH=${RESOURCES_DIR}/config-kafka-broker-producer.properties
export CONSUMER_CONFIG_FILE_PATH=${RESOURCES_DIR}/config-kafka-broker-consumer.properties
export HTTPSERVER_CONFIG_FILE_PATH=${RESOURCES_DIR}/config-kafka-broker-httpserver.properties
export WEBCLIENT_CONFIG_FILE_PATH=${RESOURCES_DIR}/config-kafka-broker-webclient.properties
export DATA_PLANE_CONFIG_FILE_PATH=${RESOURCES_DIR}/ingress.json
export CONFIG_TRACING_PATH=${RESOURCES_DIR}/tracing
export LIVENESS_PROBE_PATH=/healthz
export READINESS_PROBE_PATH=/readyz
export METRICS_PATH=/metrics
export METRICS_PUBLISH_QUANTILES="false"
export EGRESSES_INITIAL_CAPACITY="1"
export HTTP2_DISABLE="true"
export WAIT_STARTUP_SECONDS="8"

# Define receiver specific env variables.
export SERVICE_NAME="kafka-broker-receiver"
export SERVICE_NAMESPACE="knative-eventing"
export INGRESS_PORT="8080"
export METRICS_PORT="9090"
export INSTANCE_ID="receiver"

# Run receiver.
java \
  -XX:+UnlockDiagnosticVMOptions \
  -XX:+DebugNonSafepoints \
  -Dlogback.configurationFile="${RESOURCES_DIR}"/config-logging.xml \
  -jar "${PROJECT_ROOT_DIR}"/receiver/target/receiver-1.0-SNAPSHOT.jar >"${LOG_DIR}/receiver.log" &
receiver_pid=$!

# Define expected env variables.
export SERVICE_NAME="kafka-broker-dispatcher"
export SERVICE_NAMESPACE="knative-eventing"
export INGRESS_PORT="8080"
export METRICS_PORT="9089"
export INSTANCE_ID="dispatcher"

# Run dispatcher.
java \
  -XX:+UnlockDiagnosticVMOptions \
  -XX:+DebugNonSafepoints \
  -Dlogback.configurationFile="${RESOURCES_DIR}"/config-logging.xml \
  -jar "${PROJECT_ROOT_DIR}"/dispatcher/target/dispatcher-1.0-SNAPSHOT.jar >"${LOG_DIR}/dispatcher.log" &
dispatcher_pid=$!

# Download Sacura
GO111MODULE=off go get github.com/pierdipi/sacura/cmd/sacura || exit 1

# Suppress failure since it fails when it doesn't receive all events.
echo "Warm up $(date)"
! sacura --config "${RESOURCES_DIR}"/config-sacura-warmup.yaml

echo "Attach profilers $(date)"
./async-profiler/profiler.sh \
  -d 540 \
  -o html \
  -f profile-"${EVENT}${SUFFIX}-receiver".html \
  -e "${EVENT}" \
  $receiver_pid &
./async-profiler/profiler.sh \
  -d 540 \
  -o html \
  -f profile-"${EVENT}${SUFFIX}-dispatcher".html \
  -e "${EVENT}" \
  $dispatcher_pid &

echo "Starting Sacura $(date)"
! sacura --config "${RESOURCES_DIR}"/config-sacura.yaml

echo "Sacura finished $(date)"

kill $receiver_pid
kill $dispatcher_pid
kill -9 $kafka_pid
kill -9 $zookeeper_pid

rm -rf kafka kafka.tgz async-profiler async-profiler.tgz

exit 0
