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
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka"
	"knative.dev/eventing-kafka-broker/test/e2e_new/resources/configmap"
	"knative.dev/eventing/test/rekt/resources/broker"
	"knative.dev/eventing/test/rekt/resources/trigger"
	"knative.dev/reconciler-test/pkg/eventshub"
	. "knative.dev/reconciler-test/pkg/eventshub/assert"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/resources/svc"
)

func Broker() *feature.Feature {
	f := feature.NewFeature()

	sourceName := feature.MakeRandomK8sName("source")
	sinkName := feature.MakeRandomK8sName("sink")
	triggerName := feature.MakeRandomK8sName("trigger")
	brokerName := feature.MakeRandomK8sName("broker")

	ev := FullEvent()

	// Create the broker
	f.Setup("install broker", broker.Install(
		brokerName,
		broker.WithBrokerClass(kafka.BrokerClass),
	))
	f.Setup("broker is ready", broker.IsReady(brokerName))
	f.Setup("broker is addressable", broker.IsAddressable(brokerName))

	// Install the trigger and wait for the readiness
	f.Setup("install sink", eventshub.Install(
		sinkName,
		eventshub.StartReceiver,
	))
	f.Setup("install trigger", trigger.Install(
		triggerName,
		brokerName,
		trigger.WithSubscriber(svc.AsKReference(sinkName), ""),
	))
	f.Setup("trigger is ready", trigger.IsReady(triggerName))

	// Let's delete the contract config map and then wait for it to be recreated
	f.Setup("delete config map", configmap.DeleteBrokerContract)
	f.Setup("wait config map", configmap.ExistsBrokerContract)
	f.Setup("trigger is ready after deleting the contract config map", trigger.IsReady(triggerName))

	// Let's send a message and check if it's received
	f.Setup("install source", eventshub.Install(
		sourceName,
		eventshub.StartSenderToResource(broker.GVR(), brokerName),
		eventshub.InputEvent(ev),
	))
	f.Assert("receive event", OnStore(sinkName).MatchEvent(HasId(ev.ID())).Exact(1))

	return f
}
