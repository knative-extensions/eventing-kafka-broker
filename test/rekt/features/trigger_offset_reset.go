/*
 * Copyright 2022 The Knative Authors
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
	"context"
	"time"

	"github.com/cloudevents/sdk-go/v2/test"
	"github.com/google/uuid"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/eventing/test/rekt/resources/broker"
	"knative.dev/eventing/test/rekt/resources/trigger"
	"knative.dev/pkg/system"
	"knative.dev/reconciler-test/pkg/eventshub"
	"knative.dev/reconciler-test/pkg/eventshub/assert"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/resources/service"

	"knative.dev/eventing-kafka-broker/test/rekt/resources/configmap"
)

// TriggerLatestOffset tests that a Trigger receives only new events.
func TriggerLatestOffset() *feature.Feature {
	f := feature.NewFeatureNamed("Consumer latest auto.offset.reset config")

	brokerName := feature.MakeRandomK8sName("broker")
	cmName := feature.MakeRandomK8sName("cm-config")

	source1 := feature.MakeRandomK8sName("source1")
	source2 := feature.MakeRandomK8sName("source2")

	trigger1Name := feature.MakeRandomK8sName("trigger1")
	sink1 := feature.MakeRandomK8sName("sink1")

	trigger2Name := feature.MakeRandomK8sName("trigger2")
	sink2 := feature.MakeRandomK8sName("sink2")

	event1 := test.FullEvent()
	eventID1 := uuid.New().String()
	event1.SetID(eventID1)

	event2 := test.FullEvent()
	eventID2 := uuid.New().String()
	event2.SetID(eventID2)

	f.Setup("install config", configmap.Copy(types.NamespacedName{Namespace: system.Namespace(), Name: "kafka-broker-config"}, cmName))
	f.Setup("install broker", broker.Install(brokerName, append(broker.WithEnvConfig(), broker.WithConfig(cmName))...))
	f.Setup("broker is ready", broker.IsReady(brokerName))

	f.Setup("install sink1", eventshub.Install(sink1, eventshub.StartReceiver))
	f.Setup("install sink2", eventshub.Install(sink2, eventshub.StartReceiver))
	f.Setup("install trigger 1", trigger.Install(trigger1Name, trigger.WithBrokerName(brokerName), trigger.WithSubscriber(service.AsKReference(sink1), "")))
	f.Setup("trigger 1 is ready", trigger.IsReady(trigger1Name))

	f.Requirement("send event 1", eventshub.Install(source1, eventshub.InputEvent(event1), eventshub.StartSenderToResource(broker.GVR(), brokerName)))
	f.Requirement("event 1 received", assert.OnStore(sink1).MatchEvent(test.HasId(eventID1)).Exact(1))

	f.Assert("install trigger 2", trigger.Install(trigger2Name, trigger.WithBrokerName(brokerName), trigger.WithSubscriber(service.AsKReference(sink2), "")))
	f.Assert("trigger 2 is ready", trigger.IsReady(trigger2Name))

	f.Assert("send event 2", func(ctx context.Context, t feature.T) {
		trigger.IsReady(trigger2Name)(ctx, t) // Wait for trigger ready
		eventshub.Install(source2, eventshub.InputEvent(event2), eventshub.StartSenderToResource(broker.GVR(), brokerName))(ctx, t)
	})

	// Both triggers receive event 1 and 2.
	f.Assert("event 2 is received by sink 1", assert.OnStore(sink1).MatchEvent(test.HasId(eventID2)).Exact(1))
	f.Assert("event 2 is received by sink 2", assert.OnStore(sink2).MatchEvent(test.HasId(eventID2)).Exact(1))

	// Trigger 2 doesn't receive event 1 (sent before it was ready)
	f.Assert("event 1 is not received by sink 2", func(ctx context.Context, t feature.T) {
		trigger.IsReady(trigger2Name)(ctx, t) // Wait for trigger ready
		time.Sleep(20 * time.Second)          // eventually
		assert.OnStore(sink2).MatchEvent(test.HasId(eventID1)).Not()(ctx, t)
	})

	return f
}
