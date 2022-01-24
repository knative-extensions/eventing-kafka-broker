#!/usr/bin/env bash

source $(dirname $0)/resolve.sh

release=$1

broker_cp_output_file="openshift/release/knative-eventing-kafka-broker-cp-ci.yaml"
broker_dp_output_file="openshift/release/knative-eventing-kafka-broker-dp-ci.yaml"

if [ "$release" == "ci" ]; then
    image_prefix="registry.ci.openshift.org/openshift/knative-nightly:knative-eventing-kafka-"
    tag=""
else
    image_prefix="registry.ci.openshift.org/openshift/knative-${release}:knative-eventing-kafka-"
    tag=""
fi

# the Broker Control Plane parts
# The generic CP root folder
resolve_resources control-plane/config/100-broker $broker_cp_output_file $image_prefix $tag

resolve_resources control-plane/config/100-sink cp_sink.yaml $image_prefix $tag
cat cp_sink.yaml >> $broker_cp_output_file
rm cp_sink.yaml

resolve_resources control-plane/config/200-controller cp_broker.yaml $image_prefix $tag
cat cp_broker.yaml >> $broker_cp_output_file
rm cp_broker.yaml
resolve_resources control-plane/config/200-webhook cp_broker.yaml $image_prefix $tag
cat cp_broker.yaml >> $broker_cp_output_file
rm cp_broker.yaml

# the Broker Data Plane folders
# The generic DP root folder
resolve_resources data-plane/config $broker_dp_output_file $image_prefix $tag

# The DP folder for Broker:
resolve_resources data-plane/config/broker dp_broker.yaml $image_prefix $tag
cat dp_broker.yaml >> $broker_dp_output_file
rm dp_broker.yaml

resolve_resources data-plane/config/broker/template dp_broker.yaml $image_prefix $tag
cat dp_broker.yaml >> $broker_dp_output_file
rm dp_broker.yaml

# The DP folder for Sink:
resolve_resources data-plane/config/sink dp_sink.yaml $image_prefix $tag
cat dp_sink.yaml >> $broker_dp_output_file
rm dp_sink.yaml

resolve_resources data-plane/config/sink/template dp_sink.yaml $image_prefix $tag
cat dp_sink.yaml >> $broker_dp_output_file
rm dp_sink.yaml
