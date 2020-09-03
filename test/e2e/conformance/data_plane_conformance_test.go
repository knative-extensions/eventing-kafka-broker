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

package conformance

import (
	"fmt"
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	cetest "github.com/cloudevents/sdk-go/v2/test"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1beta1"
	conformance "knative.dev/eventing/test/conformance/helpers"
	testlib "knative.dev/eventing/test/lib"
	"knative.dev/eventing/test/lib/recordevents"
	"knative.dev/eventing/test/lib/resources"
	"knative.dev/eventing/test/lib/sender"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka"
	pkgtesting "knative.dev/eventing-kafka-broker/test/pkg/testing"
)

var (
	podMeta = metav1.TypeMeta{
		Kind:       "Pod",
		APIVersion: "v1",
	}
)

func TestBrokerIngressV1Beta1(t *testing.T) {
	pkgtesting.RunMultiple(t, func(t *testing.T) {

		// TODO re-use Eventing conformance test once it's runnable with non channel based brokers.

		client := testlib.Setup(t, false)
		defer testlib.TearDown(client)

		broker := createBroker(client)

		triggerName := "trigger"
		loggerName := "logger-pod"
		eventTracker, _ := recordevents.StartEventRecordOrFail(client, loggerName)
		client.WaitForAllTestResourcesReadyOrFail()

		trigger := client.CreateTriggerOrFailV1Beta1(
			triggerName,
			resources.WithBrokerV1Beta1(broker.Name),
			resources.WithAttributesTriggerFilterV1Beta1("", "", nil),
			resources.WithSubscriberServiceRefForTriggerV1Beta1(loggerName),
		)

		client.WaitForResourceReadyOrFail(trigger.Name, testlib.TriggerTypeMeta)
		t.Run("Ingress Supports CE0.3", func(t *testing.T) {
			eventID := "CE0.3"
			event := cloudevents.NewEvent()
			event.SetID(eventID)
			event.SetType(testlib.DefaultEventType)
			event.SetSource("0.3.event.sender.test.knative.dev")
			body := fmt.Sprintf(`{"msg":%q}`, eventID)

			if err := event.SetData(cloudevents.ApplicationJSON, []byte(body)); err != nil {
				t.Fatalf("Cannot set the payload of the event: %s", err.Error())
			}
			event.Context.AsV03()
			event.SetSpecVersion("0.3")
			senderName := "v03-test-sender"
			client.SendEventToAddressable(senderName, broker.Name, testlib.BrokerTypeMeta, event, sender.WithEncoding(cloudevents.EncodingStructured))
			originalEventMatcher := recordevents.MatchEvent(cetest.AllOf(
				cetest.HasId(eventID),
				cetest.HasSpecVersion("0.3"),
			))
			client.WaitForResourceReadyOrFail(senderName, &podMeta)
			eventTracker.AssertAtLeast(1, originalEventMatcher)
		})

		t.Run("Ingress Supports CE1.0", func(t *testing.T) {
			eventID := "CE1.0"
			event := cloudevents.NewEvent()
			event.SetID(eventID)
			event.SetType(testlib.DefaultEventType)
			event.SetSource("1.0.event.sender.test.knative.dev")
			body := fmt.Sprintf(`{"msg":%q}`, eventID)
			if err := event.SetData(cloudevents.ApplicationJSON, []byte(body)); err != nil {
				t.Fatalf("Cannot set the payload of the event: %s", err.Error())
			}

			senderName := "v10-test-sender"
			client.SendEventToAddressable(senderName, broker.Name, testlib.BrokerTypeMeta, event, sender.WithEncoding(cloudevents.EncodingStructured))
			originalEventMatcher := recordevents.MatchEvent(cetest.AllOf(
				cetest.HasId(eventID),
				cetest.HasSpecVersion("1.0"),
			))
			client.WaitForResourceReadyOrFail(senderName, &podMeta)
			eventTracker.AssertAtLeast(1, originalEventMatcher)

		})

		t.Run("Ingress Supports Structured Mode", func(t *testing.T) {
			eventID := "Structured-Mode"
			event := cloudevents.NewEvent()
			event.SetID(eventID)
			event.SetType(testlib.DefaultEventType)
			event.SetSource("structured.mode.event.sender.test.knative.dev")
			body := fmt.Sprintf(`{"msg":%q}`, eventID)
			if err := event.SetData(cloudevents.ApplicationJSON, []byte(body)); err != nil {
				t.Fatalf("Cannot set the payload of the event: %s", err.Error())
			}
			senderName := "structured-test-sender"
			client.SendEventToAddressable(senderName, broker.Name, testlib.BrokerTypeMeta, event, sender.WithEncoding(cloudevents.EncodingStructured))
			originalEventMatcher := recordevents.MatchEvent(cetest.AllOf(
				cetest.HasId(eventID),
			))
			client.WaitForResourceReadyOrFail(senderName, &podMeta)
			eventTracker.AssertAtLeast(1, originalEventMatcher)
		})

		t.Run("Ingress Supports Binary Mode", func(t *testing.T) {
			eventID := "Binary-Mode"
			event := cloudevents.NewEvent()
			event.SetID(eventID)
			event.SetType(testlib.DefaultEventType)
			event.SetSource("binary.mode.event.sender.test.knative.dev")
			body := fmt.Sprintf(`{"msg":%q}`, eventID)
			if err := event.SetData(cloudevents.ApplicationJSON, []byte(body)); err != nil {
				t.Fatalf("Cannot set the payload of the event: %s", err.Error())
			}
			senderName := "binary-test-sender"
			client.SendEventToAddressable(senderName, broker.Name, testlib.BrokerTypeMeta, event, sender.WithEncoding(cloudevents.EncodingBinary))
			originalEventMatcher := recordevents.MatchEvent(cetest.AllOf(
				cetest.HasId(eventID),
			))
			client.WaitForResourceReadyOrFail(senderName, &podMeta)
			eventTracker.AssertAtLeast(1, originalEventMatcher)
		})

		t.Run("Respond with 2XX on good CE", func(t *testing.T) {
			eventID := "2hundred-on-good-ce"
			body := fmt.Sprintf(`{"msg":%q}`, eventID)
			responseSink := "http://" + client.GetServiceHost(loggerName)
			senderName := "twohundred-test-sender"
			client.SendRequestToAddressable(senderName, broker.Name, testlib.BrokerTypeMeta,
				map[string]string{
					"ce-specversion": "1.0",
					"ce-type":        testlib.DefaultEventType,
					"ce-source":      "2xx.request.sender.test.knative.dev",
					"ce-id":          eventID,
					"content-type":   cloudevents.ApplicationJSON,
				},
				body,
				sender.WithResponseSink(responseSink),
			)
			client.WaitForResourceReadyOrFail(senderName, &podMeta)
			eventTracker.AssertExact(1, recordevents.MatchEvent(sender.MatchStatusCode(202))) // should probably be a range

		})
		// Respond with 400 on bad CE
		t.Run("Respond with 400 on bad CE", func(t *testing.T) {
			eventID := "four-hundred-on-bad-ce"
			body := ";la}{kjsdf;oai2095{}{}8234092349807asdfashdf"
			responseSink := "http://" + client.GetServiceHost(loggerName)
			senderName := "fourhundres-test-sender"
			client.SendRequestToAddressable(senderName, broker.Name, testlib.BrokerTypeMeta,
				map[string]string{
					"ce-specversion": "9000.1", // its over 9,000!
					"ce-type":        testlib.DefaultEventType,
					"ce-source":      "400.request.sender.test.knative.dev",
					"ce-id":          eventID,
					"content-type":   cloudevents.ApplicationJSON,
				},
				body,
				sender.WithResponseSink(responseSink))
			client.WaitForResourceReadyOrFail(senderName, &podMeta)
			eventTracker.AssertExact(1, recordevents.MatchEvent(sender.MatchStatusCode(400)))
		})
	})
}

func TestBrokerConsumerV1Beta1(t *testing.T) {
	// TODO re-enable this test
	t.Skip("This scenario is already covered by TestEventTransformationForTrigger*")

	pkgtesting.RunMultiple(t, func(t *testing.T) {

		client := testlib.Setup(t, true)
		defer testlib.TearDown(client)

		broker := createBroker(client)

		runner := testlib.ComponentsTestRunner{
			ComponentFeatureMap: map[metav1.TypeMeta][]testlib.Feature{
				*testlib.BrokerTypeMeta: pkgtesting.Features,
			},
			ComponentsToTest:   pkgtesting.Components,
			ComponentName:      broker.Name,
			ComponentNamespace: broker.Namespace,
		}

		conformance.BrokerV1Beta1ConsumerDataPlaneTestHelper(t, kafka.BrokerClass, runner)
	})
}

func createBroker(client *testlib.Client) *eventing.Broker {

	broker := client.CreateBrokerV1Beta1OrFail("broker",
		resources.WithBrokerClassForBrokerV1Beta1(kafka.BrokerClass),
	)

	client.WaitForResourceReadyOrFail(broker.Name, testlib.BrokerTypeMeta)
	return broker
}
