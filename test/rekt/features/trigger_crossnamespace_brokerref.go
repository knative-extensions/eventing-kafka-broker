/*
 * Copyright 2025 The Knative Authors
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
	brokerconfigmap "knative.dev/eventing-kafka-broker/test/rekt/resources/configmap/broker"
	"knative.dev/eventing/test/rekt/resources/broker"
	"knative.dev/eventing/test/rekt/resources/trigger"
	"knative.dev/reconciler-test/pkg/eventshub"
	"knative.dev/reconciler-test/pkg/eventshub/assert"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/resources/service"
)

// TriggerCrossNamespaceBrokerRef tests cross-namespace Trigger → Broker references.
func TriggerCrossNamespaceBrokerRef() *feature.Feature {
	f := feature.NewFeatureNamed("Trigger cross-namespace BrokerRef")

	brokerName := feature.MakeRandomK8sName("broker")
	brokerConfigName := feature.MakeRandomK8sName("broker-config")
	triggerName := feature.MakeRandomK8sName("trigger")
	sinkName := feature.MakeRandomK8sName("sink")
	sourceName := feature.MakeRandomK8sName("source")

	event := test.FullEvent()
	eventID := uuid.New().String()
	event.SetID(eventID)

	f.Setup("install broker config", brokerconfigmap.Install(brokerConfigName,
		brokerconfigmap.WithBootstrapServer(testpkg.BootstrapServersPlaintext),
		brokerconfigmap.WithNumPartitions(1),
		brokerconfigmap.WithReplicationFactor(1)))

	f.Setup("install broker", broker.Install(brokerName, append(
		broker.WithEnvConfig(),
		broker.WithConfig(brokerConfigName))...,
	))

	f.Setup("broker is ready", broker.IsReady(brokerName))

	f.Setup("install sink", eventshub.Install(sinkName, eventshub.StartReceiver))

	f.Requirement("install trigger", trigger.Install(triggerName,
		trigger.WithBrokerName(brokerName),
		trigger.WithSubscriber(service.AsKReference(sinkName), ""),
	))

	f.Requirement("trigger is ready", trigger.IsReady(triggerName))

	f.Assert("send event through broker", eventshub.Install(sourceName,
		eventshub.InputEvent(event),
		eventshub.StartSenderToResource(broker.GVR(), brokerName),
	))

	f.Assert("event is received by sink", assert.OnStore(sinkName).MatchEvent(test.HasId(eventID)).Exact(1))

	return f
}

// TriggerCrossNamespaceBrokerRefMultipleTriggers tests multiple Triggers referencing the same Broker.
func TriggerCrossNamespaceBrokerRefMultipleTriggers() *feature.Feature {
	f := feature.NewFeatureNamed("Multiple Triggers cross-namespace BrokerRef")

	brokerName := feature.MakeRandomK8sName("broker")
	brokerConfigName := feature.MakeRandomK8sName("broker-config")
	trigger1Name := feature.MakeRandomK8sName("trigger1")
	trigger2Name := feature.MakeRandomK8sName("trigger2")
	sink1Name := feature.MakeRandomK8sName("sink1")
	sink2Name := feature.MakeRandomK8sName("sink2")
	sourceName := feature.MakeRandomK8sName("source")

	event := test.FullEvent()
	eventID := uuid.New().String()
	event.SetID(eventID)

	f.Setup("install broker config", brokerconfigmap.Install(brokerConfigName,
		brokerconfigmap.WithBootstrapServer(testpkg.BootstrapServersPlaintext),
		brokerconfigmap.WithNumPartitions(1),
		brokerconfigmap.WithReplicationFactor(1)))

	f.Setup("install broker", broker.Install(brokerName, append(
		broker.WithEnvConfig(),
		broker.WithConfig(brokerConfigName))...,
	))

	f.Setup("broker is ready", broker.IsReady(brokerName))

	f.Setup("install sink1", eventshub.Install(sink1Name, eventshub.StartReceiver))

	f.Requirement("install trigger1", trigger.Install(trigger1Name,
		trigger.WithBrokerName(brokerName),
		trigger.WithSubscriber(service.AsKReference(sink1Name), ""),
	))

	f.Requirement("trigger1 is ready", trigger.IsReady(trigger1Name))

	f.Setup("install sink2", eventshub.Install(sink2Name, eventshub.StartReceiver))

	f.Requirement("install trigger2", trigger.Install(trigger2Name,
		trigger.WithBrokerName(brokerName),
		trigger.WithSubscriber(service.AsKReference(sink2Name), ""),
	))

	f.Requirement("trigger2 is ready", trigger.IsReady(trigger2Name))

	f.Assert("send event through broker", eventshub.Install(sourceName,
		eventshub.InputEvent(event),
		eventshub.StartSenderToResource(broker.GVR(), brokerName),
	))

	f.Assert("event received by sink1", assert.OnStore(sink1Name).MatchEvent(test.HasId(eventID)).Exact(1))

	f.Assert("event received by sink2", assert.OnStore(sink2Name).MatchEvent(test.HasId(eventID)).Exact(1))

	return f
}

// TriggerCrossNamespaceBrokerRefWithFilter tests event filtering across namespaces.
func TriggerCrossNamespaceBrokerRefWithFilter() *feature.Feature {
	f := feature.NewFeatureNamed("Trigger cross-namespace BrokerRef with filter")

	brokerName := feature.MakeRandomK8sName("broker")
	brokerConfigName := feature.MakeRandomK8sName("broker-config")
	triggerMatchName := feature.MakeRandomK8sName("trigger-match")
	triggerNoMatchName := feature.MakeRandomK8sName("trigger-nomatch")
	sinkMatchName := feature.MakeRandomK8sName("sink-match")
	sinkNoMatchName := feature.MakeRandomK8sName("sink-nomatch")

	eventMatch := test.FullEvent()
	eventMatch.SetType("com.example.match")
	eventMatchID := uuid.New().String()
	eventMatch.SetID(eventMatchID)

	eventNoMatch := test.FullEvent()
	eventNoMatch.SetType("com.example.nomatch")
	eventNoMatchID := uuid.New().String()
	eventNoMatch.SetID(eventNoMatchID)

	f.Setup("install broker config", brokerconfigmap.Install(brokerConfigName,
		brokerconfigmap.WithBootstrapServer(testpkg.BootstrapServersPlaintext),
		brokerconfigmap.WithNumPartitions(1),
		brokerconfigmap.WithReplicationFactor(1)))

	f.Setup("install broker", broker.Install(brokerName, append(
		broker.WithEnvConfig(),
		broker.WithConfig(brokerConfigName))...,
	))

	f.Setup("broker is ready", broker.IsReady(brokerName))

	f.Setup("install sink for matching filter", eventshub.Install(sinkMatchName, eventshub.StartReceiver))

	f.Requirement("install trigger with matching filter", trigger.Install(triggerMatchName,
		trigger.WithBrokerName(brokerName),
		trigger.WithFilter(map[string]string{"type": "com.example.match"}),
		trigger.WithSubscriber(service.AsKReference(sinkMatchName), ""),
	))

	f.Requirement("trigger with matching filter is ready", trigger.IsReady(triggerMatchName))

	f.Setup("install sink for non-matching filter", eventshub.Install(sinkNoMatchName, eventshub.StartReceiver))

	f.Requirement("install trigger with non-matching filter", trigger.Install(triggerNoMatchName,
		trigger.WithBrokerName(brokerName),
		trigger.WithFilter(map[string]string{"type": "com.example.different"}),
		trigger.WithSubscriber(service.AsKReference(sinkNoMatchName), ""),
	))

	f.Requirement("trigger with non-matching filter is ready", trigger.IsReady(triggerNoMatchName))

	f.Assert("send matching event", eventshub.Install(feature.MakeRandomK8sName("source-match"),
		eventshub.InputEvent(eventMatch),
		eventshub.StartSenderToResource(broker.GVR(), brokerName),
	))

	f.Assert("send non-matching event", eventshub.Install(feature.MakeRandomK8sName("source-nomatch"),
		eventshub.InputEvent(eventNoMatch),
		eventshub.StartSenderToResource(broker.GVR(), brokerName),
	))

	f.Assert("matching event received by matching sink", assert.OnStore(sinkMatchName).MatchEvent(test.HasId(eventMatchID)).Exact(1))

	f.Assert("matching event NOT received by non-matching sink", assert.OnStore(sinkNoMatchName).MatchEvent(test.HasId(eventMatchID)).Not())

	f.Assert("non-matching event NOT received by matching sink", assert.OnStore(sinkMatchName).MatchEvent(test.HasId(eventNoMatchID)).Not())

	return f
}
