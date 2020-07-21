// +build e2e

/*
 * Copyright 2020 The Knative Authors
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

package e2e

import (
	"fmt"
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	. "github.com/cloudevents/sdk-go/v2/test"
	"github.com/google/uuid"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1beta1"
	testlib "knative.dev/eventing/test/lib"
	"knative.dev/eventing/test/lib/recordevents"
	"knative.dev/eventing/test/lib/resources"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka"
)

func TestBrokerTrigger(t *testing.T) {
	t.Parallel()

	// 5  -> Ok
	// 10 -> Ok
	// 15 -> Ok
	// 20 -> No -> Some (1-4) connection refused by the HTTP Client (probably need retries)
	for i := 0; i < 1; i++ {

		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {

			client := testlib.Setup(t, true)
			defer testlib.TearDown(client)

			const (
				senderName  = "sender"
				brokerName  = "broker"
				triggerName = "trigger"
				subscriber  = "subscriber"

				eventType       = "type1"
				eventSource     = "source1"
				eventBody       = `{"msg":"e2e-eventtransformation-body"}`
				extension1      = "ext1"
				valueExtension1 = "value1"
			)

			nonMatchingEventId := uuid.New().String()
			eventId := uuid.New().String()

			client.CreateBrokerV1Beta1OrFail(
				brokerName,
				resources.WithBrokerClassForBrokerV1Beta1(kafka.BrokerClass),
			)

			eventTracker, _ := recordevents.StartEventRecordOrFail(client, subscriber)
			defer eventTracker.Cleanup()

			client.CreateTriggerOrFailV1Beta1(
				triggerName,
				resources.WithBrokerV1Beta1(brokerName),
				resources.WithSubscriberServiceRefForTriggerV1Beta1(subscriber),
				func(trigger *eventing.Trigger) {
					trigger.Spec.Filter = &eventing.TriggerFilter{
						Attributes: map[string]string{
							"source":   eventSource,
							extension1: valueExtension1,
							"type":     "",
						},
					}
				},
			)

			client.WaitForAllTestResourcesReadyOrFail()

			t.Logf("Sending events to %s/%s", client.Namespace, brokerName)

			nonMatchingEvent := cloudevents.NewEvent()
			nonMatchingEvent.SetID(eventId)
			nonMatchingEvent.SetType(eventType)
			nonMatchingEvent.SetSource(eventSource)
			nonMatchingEvent.SetExtension(extension1, valueExtension1+"a")
			if err := nonMatchingEvent.SetData(cloudevents.ApplicationJSON, []byte(eventBody)); err != nil {
				t.Fatalf("Cannot set the payload of the event: %s", err.Error())
			}

			client.SendEventToAddressable(
				senderName+"non-matching",
				brokerName,
				testlib.BrokerTypeMeta,
				nonMatchingEvent,
			)

			eventToSend := cloudevents.NewEvent()
			eventToSend.SetID(eventId)
			eventToSend.SetType(eventType)
			eventToSend.SetSource(eventSource)
			eventToSend.SetExtension(extension1, valueExtension1)
			if err := eventToSend.SetData(cloudevents.ApplicationJSON, []byte(eventBody)); err != nil {
				t.Fatalf("Cannot set the payload of the event: %s", err.Error())
			}

			client.SendEventToAddressable(
				senderName+"matching",
				brokerName,
				testlib.BrokerTypeMeta,
				eventToSend,
			)

			eventTracker.AssertAtLeast(1, recordevents.MatchEvent(
				HasId(eventId),
				HasSource(eventSource),
				HasType(eventType),
				HasData([]byte(eventBody)),
			))

			eventTracker.AssertNot(recordevents.MatchEvent(
				HasId(nonMatchingEventId),
			))
		})
	}
}
