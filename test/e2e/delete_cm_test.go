// +build deletecm

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
	"context"
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	. "github.com/cloudevents/sdk-go/v2/test"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/pointer"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1beta1"
	testlib "knative.dev/eventing/test/lib"
	"knative.dev/eventing/test/lib/recordevents"
	"knative.dev/eventing/test/lib/resources"
	"knative.dev/pkg/system"

	eventingv1alpha1 "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/eventing/v1alpha1"
	eventingv1alpha1clientset "knative.dev/eventing-kafka-broker/control-plane/pkg/client/clientset/versioned/typed/eventing/v1alpha1"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka"
	"knative.dev/eventing-kafka-broker/test/pkg/addressable"
	kafkatest "knative.dev/eventing-kafka-broker/test/pkg/kafka"
	"knative.dev/eventing-kafka-broker/test/pkg/sink"
)

func TestDeleteBrokerCM(t *testing.T) {

	const (
		triggerName = "trigger"
		brokerName  = "broker"
		subscriber  = "subscriber"
		senderName  = "sender"

		configMapName = "kafka-broker-brokers-triggers"

		eventType   = "type1"
		eventSource = "source1"
	)

	eventId := uuid.New().String()

	ctx := context.Background()

	client := testlib.Setup(t, true)
	defer testlib.TearDown(client)

	client.CreateBrokerV1OrFail(
		brokerName,
		resources.WithBrokerClassForBrokerV1(kafka.BrokerClass),
	)

	eventTracker, _ := recordevents.StartEventRecordOrFail(ctx, client, subscriber)

	client.CreateTriggerOrFailV1Beta1(
		triggerName,
		resources.WithBrokerV1Beta1(brokerName),
		resources.WithSubscriberServiceRefForTriggerV1Beta1(subscriber),
		func(trigger *eventing.Trigger) {
			trigger.Spec.Filter = &eventing.TriggerFilter{
				Attributes: map[string]string{
					"source": eventSource,
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

	client.SendEventToAddressable(
		ctx,
		senderName+"matching",
		brokerName,
		testlib.BrokerTypeMeta,
		eventToSend,
	)

	eventTracker.AssertAtLeast(1, recordevents.MatchEvent(HasId(eventId)))

	t.Logf("Deleting ConfigMap %s", configMapName)

	err := client.Kube.CoreV1().ConfigMaps(system.Namespace()).Delete(ctx, configMapName, metav1.DeleteOptions{})
	assert.Nil(t, err)

	client.WaitForAllTestResourcesReadyOrFail(ctx)

	eventId = uuid.New().String()
	eventToSend.SetID(eventId)

	client.SendEventToAddressable(
		ctx,
		senderName+"matching-2",
		brokerName,
		testlib.BrokerTypeMeta,
		eventToSend,
	)

	eventTracker.AssertAtLeast(1, recordevents.MatchEvent(HasId(eventId)))
}

func TestDeleteSinkCM(t *testing.T) {

	const (
		configMapName = "kafka-sink-sinks"
	)

	ctx := context.Background()

	client := testlib.Setup(t, false)
	defer testlib.TearDown(client)

	clientSet, err := eventingv1alpha1clientset.NewForConfig(client.Config)
	assert.Nil(t, err)

	// Create a KafkaSink with the following spec.

	kss := eventingv1alpha1.KafkaSinkSpec{
		Topic:             "kafka-sink-" + client.Namespace,
		NumPartitions:     pointer.Int32Ptr(10),
		ReplicationFactor: func(rf int16) *int16 { return &rf }(1),
		BootstrapServers:  kafkatest.BootstrapServersArr,
	}

	createFunc := sink.CreatorV1Alpha1(clientSet, kss)

	kafkaSink, err := createFunc(types.NamespacedName{
		Namespace: client.Namespace,
		Name:      "kafka-sink",
	})
	assert.Nil(t, err)

	client.WaitForResourceReadyOrFail(kafkaSink.Name, &kafkaSink.TypeMeta)

	// Send events to the KafkaSink.
	ids := addressable.Send(t, kafkaSink)

	// Read events from the topic.
	verify(t, client, eventingv1alpha1.ModeStructured, kss.Topic, ids)

	t.Logf("Deleting ConfigMap %s", configMapName)

	err = client.Kube.CoreV1().ConfigMaps(system.Namespace()).Delete(ctx, configMapName, metav1.DeleteOptions{})
	assert.Nil(t, err)

	client.WaitForResourceReadyOrFail(kafkaSink.Name, &kafkaSink.TypeMeta)

	// Send events to the KafkaSink.
	ids = addressable.Send(t, kafkaSink)

	// Read events from the topic.
	verify(t, client, eventingv1alpha1.ModeStructured, kss.Topic, ids)
}