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

package testing

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgotesting "k8s.io/client-go/testing"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	. "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/channel"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka"
	messagingv1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
)

const (
	ChannelName             = "kc"
	ChannelNamespace        = "test-nc"
	ChannelUUID             = "c1234567-8901-2345-6789-123456789101"
	ChannelBootstrapServers = "kafka:9092"
)

func ChannelTopic() string {
	c := NewChannel().(metav1.Object)
	return kafka.ChannelTopic(TopicPrefix, c)
}

func NewChannel(options ...KRShapedOption) runtime.Object {
	c := &messagingv1beta1.KafkaChannel{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ChannelNamespace,
			Name:      ChannelName,
			UID:       ChannelUUID,
		},
	}
	for _, opt := range options {
		opt(c)
	}
	c.SetDefaults(context.Background())
	return c
}

func ChannelReceiverPod(namespace string, annotations map[string]string) runtime.Object {
	return &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        "kafka-channel-receiver",
			Namespace:   namespace,
			Annotations: annotations,
			Labels: map[string]string{
				"app": base.ChannelReceiverLabel,
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}
}

func ChannelDispatcherPod(namespace string, annotations map[string]string) runtime.Object {
	return &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        "kafka-channel-dispatcher",
			Namespace:   namespace,
			Annotations: annotations,
			Labels: map[string]string{
				"app": base.ChannelDispatcherLabel,
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}
}

func ChannelDispatcherPodUpdate(namespace string, annotations map[string]string) clientgotesting.UpdateActionImpl {
	return clientgotesting.NewUpdateAction(
		schema.GroupVersionResource{
			Group:    "*",
			Version:  "v1",
			Resource: "Pod",
		},
		namespace,
		ChannelDispatcherPod(namespace, annotations),
	)
}

func ChannelReceiverPodUpdate(namespace string, annotations map[string]string) clientgotesting.UpdateActionImpl {
	return clientgotesting.NewUpdateAction(
		schema.GroupVersionResource{
			Group:    "*",
			Version:  "v1",
			Resource: "Pod",
		},
		namespace,
		ChannelReceiverPod(namespace, annotations),
	)
}
