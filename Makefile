SHELL := /usr/bin/env bash

ROOT_DIR:=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

.DEFAULT_GOAL := up

# Create a KinD cluster
kind-up:
	${ROOT_DIR}/hack/create-kind-cluster.sh

# Delete KinD cluster
kind-down:
	kind delete cluster

# Install dependencies and publish test images
deps-up:
	source ${ROOT_DIR}/test/e2e-common.sh && knative_setup

# Delete dependencies
deps-down:
	source ${ROOT_DIR}/test/e2e-common.sh && knative_teardown

# Install Eventing Kafka Broker
up:
	source ${ROOT_DIR}/test/e2e-common.sh && test_setup

# Delete Eventing Kafka Broker
down:
	source ${ROOT_DIR}/test/e2e-common.sh && test_teardown

# Run unit tests
ut:
	${ROOT_DIR}/test/presubmit-tests.sh --unit-tests

# Run data-plane unit tests
dp-ut:
	cd ${ROOT_DIR}/data-plane && ./mvnw verify -B -U && cd -

# Run control-plane unit tests
cp-ut:
	go test -race ./...

# Run unit tests
bt:
	${ROOT_DIR}/test/presubmit-tests.sh --build-tests

# Run integration tests
it:
	set -e
	source ${ROOT_DIR}/test/e2e-common.sh && go_test_e2e -timeout=30m ./test/e2e_new/...
	source ${ROOT_DIR}/test/e2e-common.sh && go_test_e2e -timeout=30m ./test/e2e/...
	source ${ROOT_DIR}/test/e2e-common.sh && go_test_e2e -tags=deletecm ./test/e2e/...

# Install sacura resources
sacura-up:
	source ${ROOT_DIR}/test/e2e-common.sh && apply_sacura

# Run sacura test
sacura-test:
	set -e
	source ${ROOT_DIR}/test/e2e-common.sh && apply_sacura
	source ${ROOT_DIR}/test/e2e-common.sh && go_test_e2e -tags=sacura -timeout=40m ./test/e2e/...

# Clean up sacura test
sacura-down:
	source ${ROOT_DIR}/test/e2e-common.sh && delete_sacura

# Chaos duck test
chaos-up:
	export REPLICAS="3"
	source ${ROOT_DIR}/test/e2e-common.sh && scale_controlplane kafka-controller kafka-webhook-eventing eventing-webhook eventing-controller
	source ${ROOT_DIR}/test/e2e-common.sh && wait_until_pods_running knative-eventing
	source ${ROOT_DIR}/test/e2e-common.sh && apply_chaos

# Cleanup chaos test
chaos-down:
	export REPLICAS="1"
	source ${ROOT_DIR}/test/e2e-common.sh && scale_controlplane kafka-controller kafka-webhook-eventing eventing-webhook eventing-controller
	source ${ROOT_DIR}/test/e2e-common.sh && delete_chaos


profiler:
	${ROOT_DIR}/data-plane/profiler/run.sh

# Run code generators
generate: generate-proto update-codegen

# Run protobuf generator
generate-proto:
	${ROOT_DIR}/proto/hack/generate-proto.sh

# Run API generator
update-codegen:
	${ROOT_DIR}/hack/update-codegen.sh
