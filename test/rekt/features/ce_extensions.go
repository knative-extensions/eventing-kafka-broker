/*
 * Copyright 2024 The Knative Authors
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

package features

import (
	"github.com/cloudevents/sdk-go/v2/test"
	"github.com/google/uuid"
	testpkg "knative.dev/eventing-kafka-broker/test/pkg"
	"knative.dev/eventing-kafka-broker/test/rekt/resources/kafkachannel"
	"knative.dev/eventing-kafka-broker/test/rekt/resources/kafkasink"
	"knative.dev/eventing-kafka-broker/test/rekt/resources/kafkasource"
	"knative.dev/eventing-kafka-broker/test/rekt/resources/kafkatopic"
	"knative.dev/eventing/test/rekt/resources/broker"
	"knative.dev/eventing/test/rekt/resources/subscription"
	"knative.dev/eventing/test/rekt/resources/trigger"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/reconciler-test/pkg/eventshub"
	eventasssert "knative.dev/reconciler-test/pkg/eventshub/assert"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/manifest"
	"knative.dev/reconciler-test/pkg/resources/service"
)

func KnativeKafkaCEExtensionsAdded() *feature.FeatureSet {
	return &feature.FeatureSet{
		Name: "KnativeKafkaCEExtensionsAdded",
		Features: []*feature.Feature{
			brokerAddsKnativeKafkaCEExtensions(),
			channelAddsKnativeKafkaCEExtensions(),
			sourceAddsKnativeKafkaCEExtensions(),
		},
	}
}

func brokerAddsKnativeKafkaCEExtensions() *feature.Feature {
	f := feature.NewFeature()

	brokerName := feature.MakeRandomK8sName("broker")
	triggerName := feature.MakeRandomK8sName("trigger")
	senderName := feature.MakeRandomK8sName("sender")
	sinkName := feature.MakeRandomK8sName("sink")

	inputEvent := test.FullEvent()

	f.Setup("install broker", broker.Install(brokerName, broker.WithEnvConfig()...))
	f.Setup("broker is ready", broker.IsReady(brokerName))
	f.Setup("broker is addressable", broker.IsAddressable(brokerName))

	f.Setup("install sink", eventshub.Install(sinkName, eventshub.StartReceiver))

	f.Setup("install trigger", trigger.Install(
		triggerName,
		trigger.WithBrokerName(brokerName),
		trigger.WithSubscriber(service.AsKReference(sinkName), ""),
	))

	f.Requirement("install source", eventshub.Install(
		senderName,
		eventshub.StartSenderToResource(broker.GVR(), brokerName),
		eventshub.InputEvent(inputEvent),
	))

	f.Alpha("broker").
		Must("must add Knative Kafka CE extensions", eventasssert.OnStore(sinkName).
			MatchEvent(test.ContainsExtensions("knativekafkapartition", "knativekafkaoffset", "knativekafkatopic")).
			Exact(1))
	return f
}

func channelAddsKnativeKafkaCEExtensions() *feature.Feature {
	f := feature.NewFeature()

	channelName := feature.MakeRandomK8sName("channel")
	subscriptionName := feature.MakeRandomK8sName("subscription")
	senderName := feature.MakeRandomK8sName("sender")
	sinkName := feature.MakeRandomK8sName("sink")

	inputEvent := test.FullEvent()

	f.Setup("install channel", kafkachannel.Install(
		channelName,
		kafkachannel.WithNumPartitions("3"),
		kafkachannel.WithReplicationFactor("1"),
	))
	f.Setup("channel is ready", kafkachannel.IsReady(channelName))

	f.Setup("install sink", eventshub.Install(sinkName, eventshub.StartReceiver))

	f.Setup("install subscription", subscription.Install(
		subscriptionName,
		subscription.WithChannel(&duckv1.KReference{
			Kind:       "KafkaChannel",
			Name:       channelName,
			APIVersion: kafkachannel.GVR().GroupVersion().String(),
		}),
		subscription.WithSubscriber(service.AsKReference(sinkName), "", ""),
	))

	f.Requirement("install source", eventshub.Install(
		senderName,
		eventshub.StartSenderToResource(kafkachannel.GVR(), channelName),
		eventshub.InputEvent(inputEvent),
	))

	f.Alpha("channel").
		Must("add Knative Kafka CE extensions", eventasssert.OnStore(sinkName).
			MatchEvent(test.ContainsExtensions("knativekafkapartition", "knativekafkaoffset", "knativekafkatopic")).
			Exact(1))
	return f
}

func sourceAddsKnativeKafkaCEExtensions() *feature.Feature {
	f := feature.NewFeature()

	kafkaSource := feature.MakeRandomK8sName("kafka-source")
	topic := feature.MakeRandomK8sName("topic")
	kafkaSink := feature.MakeRandomK8sName("kafkaSink")
	sinkName := feature.MakeRandomK8sName("eventshub-receiver")
	sender := feature.MakeRandomK8sName("eventshub-sender")

	event := test.FullEvent()
	event.SetID(uuid.New().String())

	f.Setup("install kafka topic", kafkatopic.Install(topic))
	f.Setup("topic is ready", kafkatopic.IsReady(topic))

	// Binary content mode is default for Kafka Sink.
	f.Setup("install kafkasink", kafkasink.Install(kafkaSink, topic, testpkg.BootstrapServersPlaintextArr))
	f.Setup("kafkasink is ready", kafkasink.IsReady(kafkaSink))

	f.Setup("install eventshub receiver", eventshub.Install(sinkName, eventshub.StartReceiver))

	kafkaSourceOpts := []manifest.CfgFn{
		kafkasource.WithSink(service.AsDestinationRef(sinkName)),
		kafkasource.WithTopics([]string{topic}),
		kafkasource.WithBootstrapServers(testpkg.BootstrapServersPlaintextArr),
	}

	f.Setup("install kafka source", kafkasource.Install(kafkaSource, kafkaSourceOpts...))
	f.Setup("kafka source is ready", kafkasource.IsReady(kafkaSource))

	options := []eventshub.EventsHubOption{
		eventshub.StartSenderToResource(kafkasink.GVR(), kafkaSink),
		eventshub.InputEvent(event),
	}
	f.Requirement("install eventshub sender", eventshub.Install(sender, options...))

	f.Alpha("source").
		Must("add Knative Kafka CE extensions", eventasssert.OnStore(sinkName).
			MatchEvent(test.ContainsExtensions("knativekafkapartition", "knativekafkaoffset", "knativekafkatopic")).
			Exact(1))
	return f
}
