#!/usr/bin/env bash

readonly BUILD_DIR="$(dirname $0)/../build"

export EVENTING_KAFKA_CONTROL_PLANE_ARTIFACT="${BUILD_DIR}/eventing-kafka-controller.yaml"
export EVENTING_KAFKA_CONTROL_PLANE_PROMETHEUS_OPERATOR_ARTIFACT="${BUILD_DIR}/eventing-kafka-controller-prometheus-operator.yaml"

export EVENTING_KAFKA_SOURCE_ARTIFACT="${BUILD_DIR}/eventing-kafka-source.yaml"
export EVENTING_KAFKA_SOURCE_PROMETHEUS_OPERATOR_ARTIFACT="${BUILD_DIR}/eventing-kafka-source-prometheus-operator.yaml"

export EVENTING_KAFKA_BROKER_ARTIFACT="${BUILD_DIR}/eventing-kafka-broker.yaml"
export EVENTING_KAFKA_BROKER_PROMETHEUS_OPERATOR_ARTIFACT="${BUILD_DIR}/eventing-kafka-broker-prometheus-operator.yaml"

export EVENTING_KAFKA_SINK_ARTIFACT="${BUILD_DIR}/eventing-kafka-sink.yaml"
export EVENTING_KAFKA_SINK_PROMETHEUS_OPERATOR_ARTIFACT="${BUILD_DIR}/eventing-kafka-sink-prometheus-operator.yaml"

export EVENTING_KAFKA_CHANNEL_ARTIFACT="${BUILD_DIR}/eventing-kafka-channel.yaml"
export EVENTING_KAFKA_CHANNEL_PROMETHEUS_OPERATOR_ARTIFACT="${BUILD_DIR}/eventing-kafka-channel-prometheus-operator.yaml"

export EVENTING_KAFKA_POST_INSTALL_ARTIFACT="${BUILD_DIR}/eventing-kafka-post-install.yaml"

export EVENTING_KAFKA_ARTIFACT="${BUILD_DIR}/eventing-kafka.yaml"
