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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/pointer"
	testlib "knative.dev/eventing/test/lib"

	eventingv1alpha1 "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/eventing/v1alpha1"
	eventingv1alpha1clientset "knative.dev/eventing-kafka-broker/control-plane/pkg/client/clientset/versioned/typed/eventing/v1alpha1"
	"knative.dev/eventing-kafka-broker/test/pkg/addressable"
	kafkatest "knative.dev/eventing-kafka-broker/test/pkg/kafka"
	"knative.dev/eventing-kafka-broker/test/pkg/sink"
	pkgtesting "knative.dev/eventing-kafka-broker/test/pkg/testing"
)

func TestKafkaSinkV1Alpha1DefaultContentMode(t *testing.T) {
	pkgtesting.RunMultipleN(t, 20, func(t *testing.T) {

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
	})
}

func TestKafkaSinkV1Alpha1StructuredContentMode(t *testing.T) {
	pkgtesting.RunMultipleN(t, 20, func(t *testing.T) {

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
			ContentMode:       pointer.StringPtr(eventingv1alpha1.ModeStructured),
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
	})
}

func TestKafkaSinkV1Alpha1BinaryContentMode(t *testing.T) {
	pkgtesting.RunMultipleN(t, 20, func(t *testing.T) {

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
			ContentMode:       pointer.StringPtr(eventingv1alpha1.ModeBinary),
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
		verify(t, client, eventingv1alpha1.ModeBinary, kss.Topic, ids)
	})
}

func verify(t *testing.T, client *testlib.Client, mode, topic string, ids []string) {

	err := kafkatest.VerifyMessagesInTopic(
		client.Kube.Kube,
		client.Tracker,
		types.NamespacedName{
			Namespace: client.Namespace,
			Name:      "verify-messages",
		},
		&kafkatest.ConsumerConfig{
			BootstrapServers: kafkatest.BootstrapServers,
			Topic:            topic,
			IDS:              strings.Join(ids, ","),
			ContentMode:      mode,
		},
	)
	assert.Nil(t, err, "failed to verify messages in topic: %v - (see pod logs)", err)
}
