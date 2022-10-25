//go:build e2e
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

package e2e_broker

import (
	"context"
	"fmt"
	"testing"

	. "github.com/cloudevents/sdk-go/v2/test"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/google/uuid"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/broker"
	testingpkg "knative.dev/eventing-kafka-broker/test/pkg"
	brokertest "knative.dev/eventing-kafka-broker/test/pkg/broker"
	kafkatest "knative.dev/eventing-kafka-broker/test/pkg/kafka"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1"
	testlib "knative.dev/eventing/test/lib"
	"knative.dev/eventing/test/lib/recordevents"
	"knative.dev/eventing/test/lib/resources"
)

func TestBrokerTrigger(t *testing.T) {
	testingpkg.RunMultiple(t, func(t *testing.T) {

		ctx := context.Background()

		client := testlib.Setup(t, true)
		defer testlib.TearDown(client)

		const (
			senderName  = "sender"
			triggerName = "trigger"
			subscriber  = "subscriber"

			defaultNumPartitions     = 10
			defaultReplicationFactor = 3
			verifierName             = "num-partitions-replication-factor-verifier"

			eventType       = "type1"
			eventSource     = "source1"
			eventBody       = `{"msg":"e2e-eventtransformation-body"}`
			extension1      = "ext1"
			valueExtension1 = "value1"
		)

		nonMatchingEventId := uuid.New().String()
		eventId := uuid.New().String()

		brokerName := brokertest.Creator(client, "v1")

		eventTracker, _ := recordevents.StartEventRecordOrFail(ctx, client, subscriber)

		client.CreateTriggerOrFail(
			triggerName,
			resources.WithBroker(brokerName),
			resources.WithSubscriberServiceRefForTrigger(subscriber),
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

		client.WaitForAllTestResourcesReadyOrFail(ctx)

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
			ctx,
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
			ctx,
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

		config := &kafkatest.Config{
			BootstrapServers:  testingpkg.BootstrapServersPlaintext,
			ReplicationFactor: defaultReplicationFactor,
			NumPartitions:     defaultNumPartitions,
			Topic: kafka.BrokerTopic(broker.TopicPrefix, &eventing.Broker{
				ObjectMeta: metav1.ObjectMeta{
					Name:      brokerName,
					Namespace: client.Namespace,
				},
			}),
		}

		err := kafkatest.VerifyNumPartitionAndReplicationFactor(
			client.Kube,
			client.Tracker,
			client.Namespace,
			verifierName,
			config,
		)
		if err != nil {
			t.Errorf("failed to verify num partitions and replication factors: %v", err)
		}
	})
}

func TestBrokerWithConfig(t *testing.T) {
	testingpkg.RunMultiple(t, func(t *testing.T) {

		ctx := context.Background()

		client := testlib.Setup(t, true)
		defer testlib.TearDown(client)

		const (
			configMapName = "br-config"
			senderName    = "sender"
			triggerName   = "trigger"
			subscriber    = "subscriber"

			numPartitions     = 20
			replicationFactor = 1
			verifierName      = "num-partitions-replication-factor-verifier"

			eventType       = "type1"
			eventSource     = "source1"
			eventBody       = `{"msg":"e2e-body"}`
			extension1      = "ext1"
			valueExtension1 = "value1"
		)

		eventId := uuid.New().String()

		brokerName := brokertest.CreatorWithOptions(client, "v1",
			[]resources.BrokerOption{
				brokertest.WithBrokerClassFromEnvVar,
			},
			[]brokertest.ConfigOptions{
				func(data map[string]string) {
					data[kafka.DefaultTopicNumPartitionConfigMapKey] = fmt.Sprintf("%d", numPartitions)
					data[kafka.DefaultTopicReplicationFactorConfigMapKey] = fmt.Sprintf("%d", replicationFactor)
					data[kafka.BootstrapServersConfigMapKey] = testingpkg.BootstrapServersPlaintext
				},
			})

		eventTracker, _ := recordevents.StartEventRecordOrFail(ctx, client, subscriber)

		client.CreateTriggerOrFail(
			triggerName,
			resources.WithBroker(brokerName),
			resources.WithSubscriberServiceRefForTrigger(subscriber),
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

		client.WaitForAllTestResourcesReadyOrFail(ctx)

		t.Logf("Sending events to %s/%s", client.Namespace, brokerName)

		eventToSend := cloudevents.NewEvent()
		eventToSend.SetID(eventId)
		eventToSend.SetType(eventType)
		eventToSend.SetSource(eventSource)
		eventToSend.SetExtension(extension1, valueExtension1)
		if err := eventToSend.SetData(cloudevents.ApplicationJSON, []byte(eventBody)); err != nil {
			t.Fatalf("Cannot set the payload of the event: %s", err.Error())
		}

		client.SendEventToAddressable(
			ctx,
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

		t.Logf("Verify num partitions and replication factor")

		config := &kafkatest.Config{
			BootstrapServers:  testingpkg.BootstrapServersPlaintext,
			ReplicationFactor: replicationFactor,
			NumPartitions:     numPartitions,
			Topic: kafka.BrokerTopic(broker.TopicPrefix, &eventing.Broker{
				ObjectMeta: metav1.ObjectMeta{
					Name:      brokerName,
					Namespace: client.Namespace,
				},
			}),
		}

		err := kafkatest.VerifyNumPartitionAndReplicationFactor(
			client.Kube,
			client.Tracker,
			client.Namespace,
			verifierName,
			config,
		)
		if err != nil {
			t.Errorf("failed to verify num partitions and replication factors: %v", err)
		}
	})
}
