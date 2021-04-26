#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

REPO_ROOT=$(dirname $0)/..
REPO_ROOT=$(readlink -m $REPO_ROOT)

DATA_PLANE_OUTPUT_DIR=${REPO_ROOT}/data-plane/contract/src/main/java
CONTROL_PLANE_OUTPUT_DIR=${REPO_ROOT}

mkdir -p $DATA_PLANE_OUTPUT_DIR
mkdir -p $CONTROL_PLANE_OUTPUT_DIR

protoc --proto_path="${REPO_ROOT}/proto" \
  --java_out=$DATA_PLANE_OUTPUT_DIR \
  --go_out=$CONTROL_PLANE_OUTPUT_DIR \
  ${REPO_ROOT}/proto/*
