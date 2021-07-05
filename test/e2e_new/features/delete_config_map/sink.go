/*
 * Copyright 2021 The Knative Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package delete_config_map

import (
	. "github.com/cloudevents/sdk-go/v2/test"
	"knative.dev/reconciler-test/pkg/eventshub"
	"knative.dev/reconciler-test/pkg/feature"

	"knative.dev/eventing-kafka-broker/test/e2e_new/resources/configmap"
	"knative.dev/eventing-kafka-broker/test/e2e_new/resources/kafka"
	"knative.dev/eventing-kafka-broker/test/e2e_new/resources/kafkasink"
)

func Sink() *feature.Feature {
	const configMapName = "kafka-sink-sinks"

	f := feature.NewFeature()

	sourceName := feature.MakeRandomK8sName("source")
	kafkaSinkName := feature.MakeRandomK8sName("kafkasink")
	topicName := feature.MakeRandomK8sName("topic")

	ev := FullEvent()

	// Create the sink
	f.Setup("install sink", kafkasink.Install(
		kafkaSinkName,
		topicName,
		kafka.BootstrapServersPlaintextArray,
		kafkasink.WithReplicationFactor(1),
		kafkasink.WithNumPartitions(10),
	))
	f.Setup("sink is ready", kafkasink.IsReady(kafkaSinkName))

	// Let's delete the contract config map and then wait for it to be recreated
	f.Setup("delete config map", configmap.DeleteFromKnativeNamespace(configMapName))
	f.Setup("wait config map", configmap.ExistsInKnativeNamespace(configMapName))
	f.Setup("sink is ready after deleting the contract config map", kafkasink.IsReady(kafkaSinkName))

	// Let's send a message and check if it's received
	f.Setup("install source", eventshub.Install(
		sourceName,
		eventshub.StartSenderToResource(kafkasink.GVR(), kafkaSinkName),
		eventshub.InputEvent(ev),
	))

	// TODO VerifyMessagesInTopic should be ported to make this working...

	return f
}
