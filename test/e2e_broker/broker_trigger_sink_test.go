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
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/pointer"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1"
	testlib "knative.dev/eventing/test/lib"
	"knative.dev/eventing/test/lib/resources"
	duckv1 "knative.dev/pkg/apis/duck/v1"

	eventingv1alpha1 "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/eventing/v1alpha1"
	eventingv1alpha1clientset "knative.dev/eventing-kafka-broker/control-plane/pkg/client/clientset/versioned/typed/eventing/v1alpha1"
	testingpkg "knative.dev/eventing-kafka-broker/test/pkg"
	"knative.dev/eventing-kafka-broker/test/pkg/addressable"
	brokertest "knative.dev/eventing-kafka-broker/test/pkg/broker"
	"knative.dev/eventing-kafka-broker/test/pkg/sink"
)

/*
+---------+     +--------+     +---------+   +---------------------------+
|  Broker +---->+ Trigger+---->+KafkaSink+-->+kafka consumer (test image)|
+----+----+     +--------+     +----+----+   +---------------------------+

	|                              ^
	|          +--------+          |
	+--------->+ Trigger+----------+
	           +--------+
*/
func TestBrokerV1TriggersV1SinkV1Alpha1(t *testing.T) {
	testingpkg.RunMultipleN(t, 10, func(t *testing.T) {

		ctx := context.Background()

		client := testlib.Setup(t, true)
		defer testlib.TearDown(client)

		clientSet, err := eventingv1alpha1clientset.NewForConfig(client.Config)
		assert.Nil(t, err)

		// Create a KafkaSink with the following spec.
		kss := eventingv1alpha1.KafkaSinkSpec{
			Topic:             "kafka-sink-" + client.Namespace,
			NumPartitions:     pointer.Int32Ptr(10),
			ReplicationFactor: func(rf int16) *int16 { return &rf }(1),
			BootstrapServers:  testingpkg.BootstrapServersPlaintextArr,
		}

		createFunc := sink.CreatorV1Alpha1(clientSet, kss)

		kafkaSink, err := createFunc(types.NamespacedName{
			Namespace: client.Namespace,
			Name:      "kafka-sink",
		})
		assert.Nil(t, err)

		client.WaitForResourceReadyOrFail(kafkaSink.Name, &kafkaSink.TypeMeta)

		// Create a Kafka Broker
		brokerName := brokertest.Creator(client, "v1")

		// Create 2 Triggers with the same subscriber

		sinkReference := &duckv1.KReference{
			Kind:       eventingv1alpha1.Kind("KafkaSink").Kind,
			APIVersion: eventingv1alpha1.SchemeGroupVersion.String(),
			Namespace:  client.Namespace,
			Name:       "kafka-sink",
		}

		client.CreateTriggerOrFail(
			"trigger-1",
			resources.WithBroker(brokerName),
			resources.WithAttributesTriggerFilter("", "trigger1", nil),
			func(trigger *eventing.Trigger) {
				trigger.Spec.Subscriber.Ref = sinkReference
			},
		)

		client.CreateTriggerOrFail(
			"trigger-2",
			resources.WithBroker(brokerName),
			resources.WithAttributesTriggerFilter("", "trigger2", nil),
			func(trigger *eventing.Trigger) {
				trigger.Spec.Subscriber.Ref = sinkReference
			},
		)

		client.WaitForAllTestResourcesReadyOrFail(ctx)

		brokerAddressable := addressable.Addressable{
			NamespacedName: types.NamespacedName{
				Namespace: client.Namespace,
				Name:      brokerName,
			},
			TypeMeta: *testlib.BrokerTypeMeta,
		}

		// Send events to the Broker for both triggers.

		setTypeMutator := func(t string) addressable.EventMutator {
			return func(event *cloudevents.Event) {
				event.SetType(t)
			}
		}

		idsTrigger1 := addressable.Send(t, brokerAddressable, setTypeMutator("trigger1"))
		idsTrigger2 := addressable.Send(t, brokerAddressable, setTypeMutator("trigger2"))

		// Read events from the topic.
		sink.Verify(t, client, eventingv1alpha1.ModeStructured, kss.Topic, append(idsTrigger1, idsTrigger2...))
	})
}
