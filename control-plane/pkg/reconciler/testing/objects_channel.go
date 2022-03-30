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
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgotesting "k8s.io/client-go/testing"
	"k8s.io/utils/pointer"
	messagingv1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	"knative.dev/eventing-kafka/pkg/channel/consolidated/reconciler/controller/resources"
	eventingduckv1 "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/network"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	. "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/channel"
)

const (
	ChannelName             = "kc"
	ChannelNamespace        = "test-nc"
	ChannelUUID             = "c1234567-8901-2345-6789-123456789101"
	ChannelBootstrapServers = "kafka-1:9092,kafka-2:9093"
	ChannelServiceName      = "kc-kn-channel"

	Subscription1Name     = "sub-1"
	Subscription2Name     = "sub-2"
	Subscription1UUID     = "2f9b5e8e-deb6-11e8-9f32-f2801f1b9fd1"
	Subscription2UUID     = "34c5aec8-deb6-11e8-9f32-f2801f1b9fd1"
	Subscription1URI      = "sub-1-uri"
	Subscription2URI      = "sub-2-uri"
	Subscription1ReplyURI = "sub-1-reply-uri"
)

func ChannelTopic() string {
	c := NewChannel()
	return kafka.ChannelTopic(TopicPrefix, c)
}

func NewChannel(options ...KRShapedOption) *messagingv1beta1.KafkaChannel {
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

func WithNumPartitions(np int32) KRShapedOption {
	return func(obj duckv1.KRShaped) {
		ch := obj.(*messagingv1beta1.KafkaChannel)
		ch.Spec.NumPartitions = np
	}
}

func WithReplicationFactor(rp int16) KRShapedOption {
	return func(obj duckv1.KRShaped) {
		ch := obj.(*messagingv1beta1.KafkaChannel)
		ch.Spec.ReplicationFactor = rp
	}
}

func WithRetentionDuration(rd string) KRShapedOption {
	return func(obj duckv1.KRShaped) {
		ch := obj.(*messagingv1beta1.KafkaChannel)
		ch.Spec.RetentionDuration = rd
	}
}

func WithChannelDelivery(d *eventingduckv1.DeliverySpec) KRShapedOption {
	return func(obj duckv1.KRShaped) {
		ch := obj.(*messagingv1beta1.KafkaChannel)
		ch.Spec.Delivery = d
	}
}

func WithChannelDeadLetterSinkURI(uri string) KRShapedOption {
	return func(obj duckv1.KRShaped) {
		ch := obj.(*messagingv1beta1.KafkaChannel)
		ch.Status.DeliveryStatus.DeadLetterSinkURI, _ = apis.ParseURL(uri)
	}
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

func ChannelAddressable(env *config.Env) func(obj duckv1.KRShaped) {

	return func(obj duckv1.KRShaped) {
		channel := obj.(*messagingv1beta1.KafkaChannel)

		channel.Status.Address = &duckv1.Addressable{}

		channel.Status.Address.URL = &apis.URL{
			Scheme: "http",
			Host:   network.GetServiceHostname(env.IngressName, env.SystemNamespace),
			Path:   fmt.Sprintf("/%s/%s", channel.Namespace, channel.Name),
		}

		channel.GetConditionSet().Manage(&channel.Status).MarkTrue(base.ConditionAddressable)
	}
}

func ChannelReference() *contract.Reference {
	return &contract.Reference{
		Uuid:      ChannelUUID,
		Namespace: ChannelNamespace,
		Name:      ChannelName,
	}
}

func WithInitKafkaChannelConditions(obj duckv1.KRShaped) {
	channel := obj.(*messagingv1beta1.KafkaChannel)
	channel.Status.InitializeConditions()
}

type SubscriberInfo struct {
	spec   *eventingduckv1.SubscriberSpec
	status *eventingduckv1.SubscriberStatus
}

type subscriberInfoOption func(sub *SubscriberInfo)

func WithSubscribers(subscribers ...*SubscriberInfo) func(obj duckv1.KRShaped) {
	return func(obj duckv1.KRShaped) {
		channel := obj.(*messagingv1beta1.KafkaChannel)
		if len(channel.Spec.Subscribers) == 0 {
			channel.Spec.Subscribers = []eventingduckv1.SubscriberSpec{}
		}

		if len(channel.Status.Subscribers) == 0 {
			channel.Status.Subscribers = []eventingduckv1.SubscriberStatus{}
		}

		for _, s := range subscribers {
			if s.spec != nil {
				channel.Spec.Subscribers = append(channel.Spec.Subscribers, *s.spec)
			}
			if s.status != nil {
				channel.Status.Subscribers = append(channel.Status.Subscribers, *s.status)
			}
		}
	}
}

func Subscriber1(options ...subscriberInfoOption) *SubscriberInfo {
	s := &SubscriberInfo{
		spec: &eventingduckv1.SubscriberSpec{
			UID:           Subscription1UUID,
			Generation:    1,
			SubscriberURI: apis.HTTP(Subscription1URI),
			ReplyURI:      apis.HTTP(Subscription1ReplyURI),
		},
		status: &eventingduckv1.SubscriberStatus{
			UID:                Subscription1UUID,
			ObservedGeneration: 1,
			Ready:              "True",
		},
	}
	for _, opt := range options {
		opt(s)
	}
	return s
}

func GetSubscriberSpec(s *SubscriberInfo) *eventingduckv1.SubscriberSpec {
	return s.spec
}

func Subscriber2(options ...subscriberInfoOption) *SubscriberInfo {
	s := &SubscriberInfo{
		spec: &eventingduckv1.SubscriberSpec{
			UID:           Subscription2UUID,
			Generation:    1,
			SubscriberURI: apis.HTTP(Subscription2URI),
			// no replies on this one
		},
		status: &eventingduckv1.SubscriberStatus{
			UID:                Subscription2UUID,
			ObservedGeneration: 1,
			Ready:              "True",
		},
	}
	for _, opt := range options {
		opt(s)
	}
	return s
}

func WithFreshSubscriber(sub *SubscriberInfo) {
	sub.status.UID = sub.spec.UID
	sub.status.ObservedGeneration = sub.spec.Generation
	sub.status.Ready = corev1.ConditionTrue
}

func WithNoSubscriberURI(sub *SubscriberInfo) {
	sub.spec.SubscriberURI = nil
	if sub.status == nil {
		sub.status = &eventingduckv1.SubscriberStatus{
			UID:                sub.spec.UID,
			ObservedGeneration: sub.spec.Generation,
		}
	}
	sub.status.Ready = "False"
	sub.status.Message = "Subscription not ready: failed to resolve subscriber config: failed to resolve Subscription.Spec.Subscriber: empty subscriber URI"
}

func WithUnreadySubscriber(sub *SubscriberInfo) {
	sub.status.Ready = "False"
	sub.status.Message = fmt.Sprintf("Subscriber %v not ready: %v %v", sub.spec.UID, "failed to reconcile consumer group,", "internal error")
}

func WithUnknownSubscriber(sub *SubscriberInfo) {
	sub.status.Ready = "Unknown"
	sub.status.Message = fmt.Sprintf("Subscriber %v not ready: %v", sub.spec.UID, "consumer group status unknown")
}

func NewPerChannelService(env *config.Env) *corev1.Service {
	s := &corev1.Service{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Service",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      ChannelServiceName,
			Namespace: ChannelNamespace,
			Labels: map[string]string{
				resources.MessagingRoleLabel: resources.MessagingRole,
			},
			OwnerReferences: []metav1.OwnerReference{
				ChannelAsOwnerReference(),
			},
		},
		Spec: corev1.ServiceSpec{
			Type:         corev1.ServiceTypeExternalName,
			ExternalName: network.GetServiceHostname(env.IngressName, env.SystemNamespace),
		},
	}

	return s
}

func ChannelAsOwnerReference() metav1.OwnerReference {
	return metav1.OwnerReference{
		APIVersion:         messagingv1beta1.SchemeGroupVersion.String(),
		Kind:               "KafkaChannel",
		Name:               ChannelName,
		UID:                ChannelUUID,
		Controller:         pointer.Bool(true),
		BlockOwnerDeletion: pointer.Bool(true),
	}
}
