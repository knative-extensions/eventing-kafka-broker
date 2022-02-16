#!/usr/bin/env bash

set -euo pipefail

source $(dirname $0)/resolve.sh

GITHUB_ACTIONS=true $(dirname $0)/../../hack/update-codegen.sh
git apply openshift/patches/*

release=$1

broker_cp_output_file="openshift/release/knative-eventing-kafka-broker-cp-ci.yaml"
rm -rf "${broker_cp_output_file}"
broker_dp_output_file="openshift/release/knative-eventing-kafka-broker-dp-ci.yaml"
rm -rf "${broker_dp_output_file}"

if [ "$release" == "ci" ]; then
  image_prefix="registry.ci.openshift.org/openshift/knative-nightly:knative-eventing-kafka-"
  tag=""
else
  image_prefix="registry.ci.openshift.org/openshift/knative-${release}:knative-eventing-kafka-"
  tag=""
fi

# the Broker Control Plane parts
resolve_resources control-plane/config/100-broker $broker_cp_output_file "$image_prefix" "$tag"
resolve_resources control-plane/config/100-sink $broker_cp_output_file "$image_prefix" "$tag"
resolve_resources control-plane/config/100-source $broker_cp_output_file "$image_prefix" "$tag"
resolve_resources control-plane/config/200-controller $broker_cp_output_file "$image_prefix" "$tag"
resolve_resources control-plane/config/200-webhook $broker_cp_output_file "$image_prefix" "$tag"

# the Broker Data Plane folders

# The DP folder for Broker:
resolve_resources data-plane/config/broker $broker_dp_output_file "$image_prefix" "$tag"
# The DP folder for Sink:
resolve_resources data-plane/config/sink $broker_dp_output_file "$image_prefix" "$tag"
# The DP folder for Source:
resolve_resources data-plane/config/source $broker_dp_output_file "$image_prefix" "$tag"
