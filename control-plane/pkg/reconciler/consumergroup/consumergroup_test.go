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

package consumergroup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/Shopify/sarama"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgotesting "k8s.io/client-go/testing"
	"k8s.io/utils/pointer"
	bindings "knative.dev/eventing-kafka/pkg/apis/bindings/v1beta1"
	sources "knative.dev/eventing-kafka/pkg/apis/sources/v1beta1"
	kubeclient "knative.dev/pkg/client/injection/kube/client/fake"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	. "knative.dev/pkg/reconciler/testing"

	eventingduckv1alpha1 "knative.dev/eventing/pkg/apis/duck/v1alpha1"
	"knative.dev/eventing/pkg/scheduler"

	eventing "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/eventing/v1alpha1"
	internals "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing"
	kafkainternals "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing/v1alpha1"
	fakekafkainternalsclient "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/client/fake"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/reconciler/eventing/v1alpha1/consumergroup"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	kafkatesting "knative.dev/eventing-kafka-broker/control-plane/pkg/kafka/testing"
	. "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/testing"
)

type SchedulerFunc func(vpod scheduler.VPod) ([]eventingduckv1alpha1.Placement, error)

func (f SchedulerFunc) Schedule(vpod scheduler.VPod) ([]eventingduckv1alpha1.Placement, error) {
	return f(vpod)
}

const (
	testSchedulerKey = "scheduler"

	systemNamespace = "knative-eventing"
)

func TestReconcileKind(t *testing.T) {

	tt := TableTest{
		{
			Name: "Consumers in multiple pods",
			Objects: []runtime.Object{
				NewService(),
				NewConsumerGroup(
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics("t1", "t2"),
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
							ConsumerGroupIdConfig("my.group.id"),
						),
					)),
					ConsumerGroupReplicas(2),
					ConsumerForTrigger(),
				),
			},
			Key: ConsumerGroupTestKey,
			OtherTestData: map[string]interface{}{
				testSchedulerKey: SchedulerFunc(func(vpod scheduler.VPod) ([]eventingduckv1alpha1.Placement, error) {
					return []eventingduckv1alpha1.Placement{
						{PodName: "p1", VReplicas: 1},
						{PodName: "p2", VReplicas: 1},
					}, nil
				}),
			},
			WantCreates: []runtime.Object{
				NewConsumer(1,
					ConsumerSpec(NewConsumerSpec(
						ConsumerTopics("t1", "t2"),
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
							ConsumerGroupIdConfig("my.group.id"),
						),
						ConsumerVReplicas(1),
						ConsumerPlacement(kafkainternals.PodBind{PodName: "p1", PodNamespace: systemNamespace}),
					)),
				),
				NewConsumer(2,
					ConsumerSpec(NewConsumerSpec(
						ConsumerTopics("t1", "t2"),
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
							ConsumerGroupIdConfig("my.group.id"),
						),
						ConsumerVReplicas(1),
						ConsumerPlacement(kafkainternals.PodBind{PodName: "p2", PodNamespace: systemNamespace}),
					)),
				),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: func() runtime.Object {
						cg := NewConsumerGroup(
							ConsumerGroupConsumerSpec(NewConsumerSpec(
								ConsumerTopics("t1", "t2"),
								ConsumerConfigs(
									ConsumerBootstrapServersConfig(ChannelBootstrapServers),
									ConsumerGroupIdConfig("my.group.id"),
								),
							)),
							ConsumerGroupReplicas(2),
							ConsumerGroupStatusReplicas(0),
							ConsumerForTrigger(),
						)
						cg.Status.Placements = []eventingduckv1alpha1.Placement{
							{PodName: "p1", VReplicas: 1},
							{PodName: "p2", VReplicas: 1},
						}
						_ = cg.MarkReconcileConsumersFailed("PropagateSubscriberURI", ErrNoSubscriberURI)
						cg.MarkScheduleSucceeded()
						return cg
					}(),
				},
			},
		},
		{
			Name: "Consumers in multiple pods, with auth spec, one exists - secret not found",
			Objects: []runtime.Object{
				NewConsumer(2,
					ConsumerSpec(NewConsumerSpec(
						ConsumerTopics("t1", "t2"),
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
							ConsumerGroupIdConfig("my.group.id"),
						),
						ConsumerAuth(&kafkainternals.Auth{
							AuthSpec: &eventing.Auth{Secret: &eventing.Secret{Ref: &eventing.SecretReference{
								Name: "non-existing secret",
							}}},
						}),
						ConsumerDelivery(NewConsumerSpecDelivery("", ConsumerInitialOffset(sources.OffsetLatest))),
						ConsumerVReplicas(1),
						ConsumerPlacement(kafkainternals.PodBind{
							PodName:      "p2",
							PodNamespace: systemNamespace,
						}),
					)),
					ConsumerReady(),
				),
				NewConsumerGroup(
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics("t1", "t2"),
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
							ConsumerGroupIdConfig("my.group.id"),
						),
						ConsumerDelivery(NewConsumerSpecDelivery("", ConsumerInitialOffset(sources.OffsetLatest))),
						ConsumerAuth(&kafkainternals.Auth{
							AuthSpec: &eventing.Auth{Secret: &eventing.Secret{Ref: &eventing.SecretReference{
								Name: "non-existing secret",
							}}},
						}),
					)),
					ConsumerForTrigger(),
					ConsumerGroupReplicas(2),
				),
			},
			Key: ConsumerGroupTestKey,
			OtherTestData: map[string]interface{}{
				testSchedulerKey: SchedulerFunc(func(vpod scheduler.VPod) ([]eventingduckv1alpha1.Placement, error) {
					return []eventingduckv1alpha1.Placement{
						{PodName: "p1", VReplicas: 1},
						{PodName: "p2", VReplicas: 1},
					}, nil
				}),
			},
			WantErr: true,
			WantEvents: []string{
				"Warning InternalError failed to initialize consumer group offset: failed to create config options for Kafka cluster auth: failed to get secret test-cg-ns/non-existing secret: secrets \"non-existing secret\" not found",
			},
			WantCreates: []runtime.Object{},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: func() runtime.Object {
						cg := NewConsumerGroup(
							ConsumerGroupConsumerSpec(NewConsumerSpec(
								ConsumerTopics("t1", "t2"),
								ConsumerConfigs(
									ConsumerBootstrapServersConfig(ChannelBootstrapServers),
									ConsumerGroupIdConfig("my.group.id"),
								),
								ConsumerDelivery(NewConsumerSpecDelivery("", ConsumerInitialOffset(sources.OffsetLatest))),
								ConsumerAuth(&kafkainternals.Auth{
									AuthSpec: &eventing.Auth{Secret: &eventing.Secret{Ref: &eventing.SecretReference{
										Name: "non-existing secret",
									}}},
								}),
							)),
							ConsumerGroupReplicas(2),
							ConsumerForTrigger(),
						)
						cg.InitializeConditions()
						_ = cg.MarkInitializeOffsetFailed("InitializeOffset", errors.New("failed to create config options for Kafka cluster auth: failed to get secret test-cg-ns/non-existing secret: secrets \"non-existing secret\" not found"))
						return cg
					}(),
				},
			},
		},
		{
			Name: "Consumers in multiple pods, with auth spec, one exists - ready",
			Objects: []runtime.Object{
				NewSSLSecret(ConsumerGroupNamespace, SecretName),
				NewConsumer(2,
					ConsumerSpec(NewConsumerSpec(
						ConsumerTopics("t1", "t2"),
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
							ConsumerGroupIdConfig("my.group.id"),
						),
						ConsumerDelivery(NewConsumerSpecDelivery("", ConsumerInitialOffset(sources.OffsetLatest))),
						ConsumerAuth(&kafkainternals.Auth{
							AuthSpec: &eventing.Auth{Secret: &eventing.Secret{Ref: &eventing.SecretReference{
								Name: SecretName,
							}}},
						}),
						ConsumerVReplicas(1),
						ConsumerPlacement(kafkainternals.PodBind{
							PodName:      "p2",
							PodNamespace: systemNamespace,
						}),
					)),
					ConsumerReady(),
				),
				NewConsumerGroup(
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics("t1", "t2"),
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
							ConsumerGroupIdConfig("my.group.id"),
						),
						ConsumerDelivery(NewConsumerSpecDelivery("", ConsumerInitialOffset(sources.OffsetLatest))),
						ConsumerAuth(&kafkainternals.Auth{
							AuthSpec: &eventing.Auth{Secret: &eventing.Secret{Ref: &eventing.SecretReference{
								Name: SecretName,
							}}},
						}),
					)),
					ConsumerForTrigger(),
					ConsumerGroupReplicas(2),
				),
			},
			Key: ConsumerGroupTestKey,
			OtherTestData: map[string]interface{}{
				testSchedulerKey: SchedulerFunc(func(vpod scheduler.VPod) ([]eventingduckv1alpha1.Placement, error) {
					return []eventingduckv1alpha1.Placement{
						{PodName: "p1", VReplicas: 1},
						{PodName: "p2", VReplicas: 1},
					}, nil
				}),
			},
			WantCreates: []runtime.Object{
				NewConsumer(1,
					ConsumerSpec(NewConsumerSpec(
						ConsumerTopics("t1", "t2"),
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
							ConsumerGroupIdConfig("my.group.id"),
						),
						ConsumerDelivery(NewConsumerSpecDelivery("", ConsumerInitialOffset(sources.OffsetLatest))),
						ConsumerAuth(&kafkainternals.Auth{
							AuthSpec: &eventing.Auth{Secret: &eventing.Secret{Ref: &eventing.SecretReference{
								Name: SecretName,
							}}},
						}),
						ConsumerVReplicas(1),
						ConsumerPlacement(kafkainternals.PodBind{PodName: "p1", PodNamespace: systemNamespace}),
					)),
				),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: func() runtime.Object {
						cg := NewConsumerGroup(
							ConsumerGroupConsumerSpec(NewConsumerSpec(
								ConsumerTopics("t1", "t2"),
								ConsumerConfigs(
									ConsumerBootstrapServersConfig(ChannelBootstrapServers),
									ConsumerGroupIdConfig("my.group.id"),
								),
								ConsumerDelivery(NewConsumerSpecDelivery("", ConsumerInitialOffset(sources.OffsetLatest))),
								ConsumerAuth(&kafkainternals.Auth{
									AuthSpec: &eventing.Auth{Secret: &eventing.Secret{Ref: &eventing.SecretReference{
										Name: SecretName,
									}}},
								}),
							)),
							ConsumerGroupReplicas(2),
							ConsumerGroupStatusReplicas(1),
							ConsumerForTrigger(),
						)
						cg.Status.Placements = []eventingduckv1alpha1.Placement{
							{PodName: "p1", VReplicas: 1},
							{PodName: "p2", VReplicas: 1},
						}
						cg.MarkReconcileConsumersSucceeded()
						cg.MarkScheduleSucceeded()
						cg.Status.SubscriberURI = ConsumerSubscriberURI
						return cg
					}(),
				},
			},
		},
		{
			Name: "Consumers in multiple pods, with net spec, one exists - secret not found",
			Objects: []runtime.Object{
				NewConsumer(2,
					ConsumerSpec(NewConsumerSpec(
						ConsumerTopics("t1", "t2"),
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
							ConsumerGroupIdConfig("my.group.id"),
						),
						ConsumerAuth(&kafkainternals.Auth{
							NetSpec: &bindings.KafkaNetSpec{
								SASL: bindings.KafkaSASLSpec{
									Enable: true,
									User: bindings.SecretValueFromSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: "non-existing secret",
											},
											Key: "user",
										},
									},
									Password: bindings.SecretValueFromSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: "non-existing secret",
											},
											Key: "password",
										},
									},
									Type: bindings.SecretValueFromSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: "non-existing secret",
											},
											Key: "type",
										},
									},
								},
								TLS: bindings.KafkaTLSSpec{
									Enable: true,
								},
							},
						}),
						ConsumerDelivery(NewConsumerSpecDelivery("", ConsumerInitialOffset(sources.OffsetLatest))),
						ConsumerVReplicas(1),
						ConsumerPlacement(kafkainternals.PodBind{
							PodName:      "p2",
							PodNamespace: systemNamespace,
						}),
					)),
					ConsumerReady(),
				),
				NewConsumerGroup(
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics("t1", "t2"),
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
							ConsumerGroupIdConfig("my.group.id"),
						),
						ConsumerDelivery(NewConsumerSpecDelivery("", ConsumerInitialOffset(sources.OffsetLatest))),
						ConsumerAuth(&kafkainternals.Auth{
							NetSpec: &bindings.KafkaNetSpec{
								SASL: bindings.KafkaSASLSpec{
									Enable: true,
									User: bindings.SecretValueFromSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: "non-existing secret",
											},
											Key: "user",
										},
									},
									Password: bindings.SecretValueFromSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: "non-existing secret",
											},
											Key: "password",
										},
									},
									Type: bindings.SecretValueFromSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: "non-existing secret",
											},
											Key: "type",
										},
									},
								},
								TLS: bindings.KafkaTLSSpec{
									Enable: true,
								},
							},
						}),
					)),
					ConsumerForTrigger(),
					ConsumerGroupReplicas(2),
				),
			},
			Key: ConsumerGroupTestKey,
			OtherTestData: map[string]interface{}{
				testSchedulerKey: SchedulerFunc(func(vpod scheduler.VPod) ([]eventingduckv1alpha1.Placement, error) {
					return []eventingduckv1alpha1.Placement{
						{PodName: "p1", VReplicas: 1},
						{PodName: "p2", VReplicas: 1},
					}, nil
				}),
			},
			WantErr: true,
			WantEvents: []string{
				"Warning InternalError failed to initialize consumer group offset: failed to create config options for Kafka cluster auth: failed to read secret test-cg-ns/non-existing secret: secret \"non-existing secret\" not found",
			},
			WantCreates: []runtime.Object{},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: func() runtime.Object {
						cg := NewConsumerGroup(
							ConsumerGroupConsumerSpec(NewConsumerSpec(
								ConsumerTopics("t1", "t2"),
								ConsumerConfigs(
									ConsumerBootstrapServersConfig(ChannelBootstrapServers),
									ConsumerGroupIdConfig("my.group.id"),
								),
								ConsumerDelivery(NewConsumerSpecDelivery("", ConsumerInitialOffset(sources.OffsetLatest))),
								ConsumerAuth(&kafkainternals.Auth{
									NetSpec: &bindings.KafkaNetSpec{
										SASL: bindings.KafkaSASLSpec{
											Enable: true,
											User: bindings.SecretValueFromSource{
												SecretKeyRef: &corev1.SecretKeySelector{
													LocalObjectReference: corev1.LocalObjectReference{
														Name: "non-existing secret",
													},
													Key: "user",
												},
											},
											Password: bindings.SecretValueFromSource{
												SecretKeyRef: &corev1.SecretKeySelector{
													LocalObjectReference: corev1.LocalObjectReference{
														Name: "non-existing secret",
													},
													Key: "password",
												},
											},
											Type: bindings.SecretValueFromSource{
												SecretKeyRef: &corev1.SecretKeySelector{
													LocalObjectReference: corev1.LocalObjectReference{
														Name: "non-existing secret",
													},
													Key: "type",
												},
											},
										},
										TLS: bindings.KafkaTLSSpec{
											Enable: true,
										},
									},
								}),
							)),
							ConsumerGroupReplicas(2),
							ConsumerForTrigger(),
						)
						cg.InitializeConditions()
						_ = cg.MarkInitializeOffsetFailed("InitializeOffset", errors.New("failed to create config options for Kafka cluster auth: failed to read secret test-cg-ns/non-existing secret: secret \"non-existing secret\" not found"))
						return cg
					}(),
				},
			},
		},
		{
			Name: "Consumers in multiple pods, with net spec, one exists - ready",
			Objects: []runtime.Object{
				NewSASLSSLSecret(ConsumerGroupNamespace, SecretName),
				NewConsumer(2,
					ConsumerSpec(NewConsumerSpec(
						ConsumerTopics("t1", "t2"),
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
							ConsumerGroupIdConfig("my.group.id"),
						),
						ConsumerDelivery(NewConsumerSpecDelivery("", ConsumerInitialOffset(sources.OffsetLatest))),
						ConsumerAuth(&kafkainternals.Auth{
							NetSpec: &bindings.KafkaNetSpec{
								SASL: bindings.KafkaSASLSpec{
									Enable: true,
									User: bindings.SecretValueFromSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: SecretName,
											},
											Key: "user",
										},
									},
									Password: bindings.SecretValueFromSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: SecretName,
											},
											Key: "password",
										},
									},
									Type: bindings.SecretValueFromSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: SecretName,
											},
											Key: "type",
										},
									},
								},
								TLS: bindings.KafkaTLSSpec{
									Enable: true,
								},
							},
						}),
						ConsumerVReplicas(1),
						ConsumerPlacement(kafkainternals.PodBind{
							PodName:      "p2",
							PodNamespace: systemNamespace,
						}),
					)),
					ConsumerReady(),
				),
				NewConsumerGroup(
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics("t1", "t2"),
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
							ConsumerGroupIdConfig("my.group.id"),
						),
						ConsumerDelivery(NewConsumerSpecDelivery("", ConsumerInitialOffset(sources.OffsetLatest))),
						ConsumerAuth(&kafkainternals.Auth{
							NetSpec: &bindings.KafkaNetSpec{
								SASL: bindings.KafkaSASLSpec{
									Enable: true,
									User: bindings.SecretValueFromSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: SecretName,
											},
											Key: "user",
										},
									},
									Password: bindings.SecretValueFromSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: SecretName,
											},
											Key: "password",
										},
									},
									Type: bindings.SecretValueFromSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: SecretName,
											},
											Key: "type",
										},
									},
								},
								TLS: bindings.KafkaTLSSpec{
									Enable: true,
								},
							},
						}),
					)),
					ConsumerForTrigger(),
					ConsumerGroupReplicas(2),
				),
			},
			Key: ConsumerGroupTestKey,
			OtherTestData: map[string]interface{}{
				testSchedulerKey: SchedulerFunc(func(vpod scheduler.VPod) ([]eventingduckv1alpha1.Placement, error) {
					return []eventingduckv1alpha1.Placement{
						{PodName: "p1", VReplicas: 1},
						{PodName: "p2", VReplicas: 1},
					}, nil
				}),
			},
			WantCreates: []runtime.Object{
				NewConsumer(1,
					ConsumerSpec(NewConsumerSpec(
						ConsumerTopics("t1", "t2"),
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
							ConsumerGroupIdConfig("my.group.id"),
						),
						ConsumerDelivery(NewConsumerSpecDelivery("", ConsumerInitialOffset(sources.OffsetLatest))),
						ConsumerAuth(&kafkainternals.Auth{
							NetSpec: &bindings.KafkaNetSpec{
								SASL: bindings.KafkaSASLSpec{
									Enable: true,
									User: bindings.SecretValueFromSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: SecretName,
											},
											Key: "user",
										},
									},
									Password: bindings.SecretValueFromSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: SecretName,
											},
											Key: "password",
										},
									},
									Type: bindings.SecretValueFromSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: SecretName,
											},
											Key: "type",
										},
									},
								},
								TLS: bindings.KafkaTLSSpec{
									Enable: true,
								},
							},
						}),
						ConsumerVReplicas(1),
						ConsumerPlacement(kafkainternals.PodBind{PodName: "p1", PodNamespace: systemNamespace}),
					)),
				),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: func() runtime.Object {
						cg := NewConsumerGroup(
							ConsumerGroupConsumerSpec(NewConsumerSpec(
								ConsumerTopics("t1", "t2"),
								ConsumerConfigs(
									ConsumerBootstrapServersConfig(ChannelBootstrapServers),
									ConsumerGroupIdConfig("my.group.id"),
								),
								ConsumerDelivery(NewConsumerSpecDelivery("", ConsumerInitialOffset(sources.OffsetLatest))),
								ConsumerAuth(&kafkainternals.Auth{
									NetSpec: &bindings.KafkaNetSpec{
										SASL: bindings.KafkaSASLSpec{
											Enable: true,
											User: bindings.SecretValueFromSource{
												SecretKeyRef: &corev1.SecretKeySelector{
													LocalObjectReference: corev1.LocalObjectReference{
														Name: SecretName,
													},
													Key: "user",
												},
											},
											Password: bindings.SecretValueFromSource{
												SecretKeyRef: &corev1.SecretKeySelector{
													LocalObjectReference: corev1.LocalObjectReference{
														Name: SecretName,
													},
													Key: "password",
												},
											},
											Type: bindings.SecretValueFromSource{
												SecretKeyRef: &corev1.SecretKeySelector{
													LocalObjectReference: corev1.LocalObjectReference{
														Name: SecretName,
													},
													Key: "type",
												},
											},
										},
										TLS: bindings.KafkaTLSSpec{
											Enable: true,
										},
									},
								}),
							)),
							ConsumerGroupReplicas(2),
							ConsumerGroupStatusReplicas(1),
							ConsumerForTrigger(),
						)
						cg.Status.Placements = []eventingduckv1alpha1.Placement{
							{PodName: "p1", VReplicas: 1},
							{PodName: "p2", VReplicas: 1},
						}
						cg.MarkReconcileConsumersSucceeded()
						cg.MarkScheduleSucceeded()
						cg.Status.SubscriberURI = ConsumerSubscriberURI
						return cg
					}(),
				},
			},
		},
		{
			Name: "Consumer update",
			Objects: []runtime.Object{
				NewService(),
				NewConsumer(1,
					ConsumerSpec(NewConsumerSpec(
						ConsumerTopics("t1", "t2"),
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
							ConsumerGroupIdConfig("my.group.id"),
						),
						ConsumerVReplicas(1),
						ConsumerPlacement(kafkainternals.PodBind{PodName: "p1", PodNamespace: systemNamespace}),
						ConsumerSubscriber(NewSourceSinkReference()),
					)),
				),
				NewConsumerGroup(
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics("t1", "t2"),
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
							ConsumerGroupIdConfig("my.group.id"),
						),
						ConsumerSubscriber(NewSourceSink2Reference()),
					)),
					ConsumerGroupReplicas(1),
					ConsumerForTrigger(),
				),
			},
			Key: ConsumerGroupTestKey,
			OtherTestData: map[string]interface{}{
				testSchedulerKey: SchedulerFunc(func(vpod scheduler.VPod) ([]eventingduckv1alpha1.Placement, error) {
					return []eventingduckv1alpha1.Placement{
						{PodName: "p1", VReplicas: 1},
					}, nil
				}),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewConsumer(1,
						ConsumerSpec(NewConsumerSpec(
							ConsumerTopics("t1", "t2"),
							ConsumerConfigs(
								ConsumerBootstrapServersConfig(ChannelBootstrapServers),
								ConsumerGroupIdConfig("my.group.id"),
							),
							ConsumerVReplicas(1),
							ConsumerPlacement(kafkainternals.PodBind{PodName: "p1", PodNamespace: systemNamespace}),
							ConsumerSubscriber(NewSourceSink2Reference()),
						)),
					),
				},
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: func() runtime.Object {
						cg := NewConsumerGroup(
							ConsumerGroupConsumerSpec(NewConsumerSpec(
								ConsumerTopics("t1", "t2"),
								ConsumerConfigs(
									ConsumerBootstrapServersConfig(ChannelBootstrapServers),
									ConsumerGroupIdConfig("my.group.id"),
								),
								ConsumerSubscriber(NewSourceSink2Reference()),
							)),
							ConsumerGroupReplicas(1),
							ConsumerGroupStatusReplicas(0),
							ConsumerForTrigger(),
						)
						cg.Status.Placements = []eventingduckv1alpha1.Placement{
							{PodName: "p1", VReplicas: 1},
						}
						_ = cg.MarkReconcileConsumersFailed("PropagateSubscriberURI", ErrNoSubscriberURI)
						cg.MarkScheduleSucceeded()
						return cg
					}(),
				},
			},
		},
		{
			Name: "Consumers in multiple pods, one exists - not ready",
			Objects: []runtime.Object{
				NewConsumer(2,
					ConsumerSpec(NewConsumerSpec(
						ConsumerTopics("t1", "t2"),
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
							ConsumerGroupIdConfig("my.group.id"),
						),
						ConsumerVReplicas(1),
						ConsumerPlacement(kafkainternals.PodBind{
							PodName:      "p2",
							PodNamespace: systemNamespace,
						}),
					)),
				),
				NewConsumerGroup(
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics("t1", "t2"),
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
							ConsumerGroupIdConfig("my.group.id"),
						),
					)),
					ConsumerForTrigger(),
					ConsumerGroupReplicas(2),
				),
			},
			Key: ConsumerGroupTestKey,
			OtherTestData: map[string]interface{}{
				testSchedulerKey: SchedulerFunc(func(vpod scheduler.VPod) ([]eventingduckv1alpha1.Placement, error) {
					return []eventingduckv1alpha1.Placement{
						{PodName: "p1", VReplicas: 1},
						{PodName: "p2", VReplicas: 1},
					}, nil
				}),
			},
			WantCreates: []runtime.Object{
				NewConsumer(1,
					ConsumerSpec(NewConsumerSpec(
						ConsumerTopics("t1", "t2"),
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
							ConsumerGroupIdConfig("my.group.id"),
						),
						ConsumerVReplicas(1),
						ConsumerPlacement(kafkainternals.PodBind{PodName: "p1", PodNamespace: systemNamespace}),
					)),
				),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: func() runtime.Object {
						cg := NewConsumerGroup(
							ConsumerGroupConsumerSpec(NewConsumerSpec(
								ConsumerTopics("t1", "t2"),
								ConsumerConfigs(
									ConsumerBootstrapServersConfig(ChannelBootstrapServers),
									ConsumerGroupIdConfig("my.group.id"),
								),
							)),
							ConsumerGroupReplicas(2),
							ConsumerGroupStatusReplicas(0),
							ConsumerForTrigger(),
						)
						cg.Status.Placements = []eventingduckv1alpha1.Placement{
							{PodName: "p1", VReplicas: 1},
							{PodName: "p2", VReplicas: 1},
						}
						_ = cg.MarkReconcileConsumersFailed("PropagateSubscriberURI", ErrNoSubscriberURI)
						cg.MarkScheduleSucceeded()
						return cg
					}(),
				},
			},
		},
		{
			Name: "Consumers in multiple pods, one exists - ready",
			Objects: []runtime.Object{
				NewConsumer(2,
					ConsumerSpec(NewConsumerSpec(
						ConsumerTopics("t1", "t2"),
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
							ConsumerGroupIdConfig("my.group.id"),
						),
						ConsumerVReplicas(1),
						ConsumerPlacement(kafkainternals.PodBind{
							PodName:      "p2",
							PodNamespace: systemNamespace,
						}),
					)),
					ConsumerReady(),
				),
				NewConsumerGroup(
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics("t1", "t2"),
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
							ConsumerGroupIdConfig("my.group.id"),
						),
					)),
					ConsumerForTrigger(),
					ConsumerGroupReplicas(2),
				),
			},
			Key: ConsumerGroupTestKey,
			OtherTestData: map[string]interface{}{
				testSchedulerKey: SchedulerFunc(func(vpod scheduler.VPod) ([]eventingduckv1alpha1.Placement, error) {
					return []eventingduckv1alpha1.Placement{
						{PodName: "p1", VReplicas: 1},
						{PodName: "p2", VReplicas: 1},
					}, nil
				}),
			},
			WantCreates: []runtime.Object{
				NewConsumer(1,
					ConsumerSpec(NewConsumerSpec(
						ConsumerTopics("t1", "t2"),
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
							ConsumerGroupIdConfig("my.group.id"),
						),
						ConsumerVReplicas(1),
						ConsumerPlacement(kafkainternals.PodBind{PodName: "p1", PodNamespace: systemNamespace}),
					)),
				),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: func() runtime.Object {
						cg := NewConsumerGroup(
							ConsumerGroupConsumerSpec(NewConsumerSpec(
								ConsumerTopics("t1", "t2"),
								ConsumerConfigs(
									ConsumerBootstrapServersConfig(ChannelBootstrapServers),
									ConsumerGroupIdConfig("my.group.id"),
								),
							)),
							ConsumerGroupReplicas(2),
							ConsumerGroupStatusReplicas(1),
							ConsumerForTrigger(),
						)
						cg.Status.Placements = []eventingduckv1alpha1.Placement{
							{PodName: "p1", VReplicas: 1},
							{PodName: "p2", VReplicas: 1},
						}
						cg.MarkReconcileConsumersSucceeded()
						cg.MarkScheduleSucceeded()
						cg.Status.SubscriberURI = ConsumerSubscriberURI
						return cg
					}(),
				},
			},
		},
		{
			Name: "Consumers in multiple pods, one exists - ready, increase replicas, propagate dead letter sink URI",
			Objects: []runtime.Object{
				NewConsumer(2,
					ConsumerSpec(NewConsumerSpec(
						ConsumerTopics("t1", "t2"),
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
							ConsumerGroupIdConfig("my.group.id"),
						),
						ConsumerDelivery(NewConsumerSpecDelivery(internals.Ordered,
							NewConsumerSpecDeliveryDeadLetterSink(),
						)),
						ConsumerVReplicas(1),
						ConsumerPlacement(kafkainternals.PodBind{
							PodName:      "p2",
							PodNamespace: systemNamespace,
						}),
					)),
					ConsumerReady(),
				),
				NewConsumerGroup(
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics("t1", "t2"),
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
							ConsumerGroupIdConfig("my.group.id"),
						),
						ConsumerDelivery(NewConsumerSpecDelivery(internals.Ordered,
							NewConsumerSpecDeliveryDeadLetterSink(),
							ConsumerInitialOffset(sources.OffsetLatest),
						)),
					)),
					ConsumerGroupReplicas(3),
					ConsumerForTrigger(),
				),
			},
			Key: ConsumerGroupTestKey,
			OtherTestData: map[string]interface{}{
				testSchedulerKey: SchedulerFunc(func(vpod scheduler.VPod) ([]eventingduckv1alpha1.Placement, error) {
					return []eventingduckv1alpha1.Placement{
						{PodName: "p1", VReplicas: 1},
						{PodName: "p2", VReplicas: 2},
					}, nil
				}),
			},
			WantCreates: []runtime.Object{
				NewConsumer(1,
					ConsumerSpec(NewConsumerSpec(
						ConsumerTopics("t1", "t2"),
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
							ConsumerGroupIdConfig("my.group.id"),
						),
						ConsumerDelivery(NewConsumerSpecDelivery(internals.Ordered,
							NewConsumerSpecDeliveryDeadLetterSink(),
							ConsumerInitialOffset(sources.OffsetLatest),
						)),
						ConsumerVReplicas(1),
						ConsumerPlacement(kafkainternals.PodBind{PodName: "p1", PodNamespace: systemNamespace}),
					)),
				),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				clientgotesting.NewUpdateAction(
					schema.GroupVersionResource{
						Group:    kafkainternals.SchemeGroupVersion.Group,
						Version:  kafkainternals.SchemeGroupVersion.Version,
						Resource: "consumers",
					},
					ConsumerNamespace,
					NewConsumer(2,
						ConsumerSpec(NewConsumerSpec(
							ConsumerTopics("t1", "t2"),
							ConsumerConfigs(
								ConsumerBootstrapServersConfig(ChannelBootstrapServers),
								ConsumerGroupIdConfig("my.group.id"),
							),
							ConsumerDelivery(NewConsumerSpecDelivery(internals.Ordered,
								NewConsumerSpecDeliveryDeadLetterSink(),
								ConsumerInitialOffset(sources.OffsetLatest),
							)),
							ConsumerVReplicas(2),
							ConsumerPlacement(kafkainternals.PodBind{PodName: "p2", PodNamespace: systemNamespace}),
						)),
						ConsumerReady(),
					),
				),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: func() runtime.Object {
						cg := NewConsumerGroup(
							ConsumerGroupConsumerSpec(NewConsumerSpec(
								ConsumerTopics("t1", "t2"),
								ConsumerConfigs(
									ConsumerBootstrapServersConfig(ChannelBootstrapServers),
									ConsumerGroupIdConfig("my.group.id"),
								),
								ConsumerDelivery(NewConsumerSpecDelivery(internals.Ordered,
									NewConsumerSpecDeliveryDeadLetterSink(),
									ConsumerInitialOffset(sources.OffsetLatest),
								)),
							)),
							ConsumerGroupReplicas(3),
							ConsumerForTrigger(),
						)
						cg.Status.Placements = []eventingduckv1alpha1.Placement{
							{PodName: "p1", VReplicas: 1},
							{PodName: "p2", VReplicas: 2},
						}
						cg.MarkReconcileConsumersSucceeded()
						cg.MarkScheduleSucceeded()
						cg.Status.SubscriberURI = ConsumerSubscriberURI
						cg.Status.DeadLetterSinkURI = ConsumerDeadLetterSinkURI
						cg.Status.Replicas = pointer.Int32(1)
						return cg
					}(),
				},
			},
		},
		{
			Name: "Consumers in multiple pods, one exists - ready, increase replicas",
			Objects: []runtime.Object{
				NewConsumer(2,
					ConsumerSpec(NewConsumerSpec(
						ConsumerTopics("t1", "t2"),
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
							ConsumerGroupIdConfig("my.group.id"),
						),
						ConsumerVReplicas(1),
						ConsumerPlacement(kafkainternals.PodBind{
							PodName:      "p2",
							PodNamespace: systemNamespace,
						}),
					)),
					ConsumerReady(),
				),
				NewConsumerGroup(
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics("t1", "t2"),
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
							ConsumerGroupIdConfig("my.group.id"),
						),
					)),
					ConsumerGroupReplicas(3),
					ConsumerForTrigger(),
				),
			},
			Key: ConsumerGroupTestKey,
			OtherTestData: map[string]interface{}{
				testSchedulerKey: SchedulerFunc(func(vpod scheduler.VPod) ([]eventingduckv1alpha1.Placement, error) {
					return []eventingduckv1alpha1.Placement{
						{PodName: "p1", VReplicas: 1},
						{PodName: "p2", VReplicas: 2},
					}, nil
				}),
			},
			WantCreates: []runtime.Object{
				NewConsumer(1,
					ConsumerSpec(NewConsumerSpec(
						ConsumerTopics("t1", "t2"),
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
							ConsumerGroupIdConfig("my.group.id"),
						),
						ConsumerVReplicas(1),
						ConsumerPlacement(kafkainternals.PodBind{PodName: "p1", PodNamespace: systemNamespace}),
					)),
				),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				clientgotesting.NewUpdateAction(
					schema.GroupVersionResource{
						Group:    kafkainternals.SchemeGroupVersion.Group,
						Version:  kafkainternals.SchemeGroupVersion.Version,
						Resource: "consumers",
					},
					ConsumerNamespace,
					NewConsumer(2,
						ConsumerSpec(NewConsumerSpec(
							ConsumerTopics("t1", "t2"),
							ConsumerConfigs(
								ConsumerBootstrapServersConfig(ChannelBootstrapServers),
								ConsumerGroupIdConfig("my.group.id"),
							),
							ConsumerVReplicas(2),
							ConsumerPlacement(kafkainternals.PodBind{PodName: "p2", PodNamespace: systemNamespace}),
						)),
						ConsumerReady(),
					),
				),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: func() runtime.Object {
						cg := NewConsumerGroup(
							ConsumerGroupConsumerSpec(NewConsumerSpec(
								ConsumerTopics("t1", "t2"),
								ConsumerConfigs(
									ConsumerBootstrapServersConfig(ChannelBootstrapServers),
									ConsumerGroupIdConfig("my.group.id"),
								),
							)),
							ConsumerGroupReplicas(3),
							ConsumerForTrigger(),
						)
						cg.Status.Placements = []eventingduckv1alpha1.Placement{
							{PodName: "p1", VReplicas: 1},
							{PodName: "p2", VReplicas: 2},
						}
						cg.MarkReconcileConsumersSucceeded()
						cg.MarkScheduleSucceeded()
						cg.Status.SubscriberURI = ConsumerSubscriberURI
						cg.Status.Replicas = pointer.Int32(1)
						return cg
					}(),
				},
			},
		},
		{
			Name: "Consumers in multiple pods, one exists, different placement",
			Objects: []runtime.Object{
				NewService(),
				NewConsumer(1,
					ConsumerSpec(NewConsumerSpec(
						ConsumerTopics("t1", "t2"),
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
							ConsumerGroupIdConfig("my.group.id"),
						),
						ConsumerVReplicas(1),
						ConsumerPlacement(kafkainternals.PodBind{PodName: "p3", PodNamespace: systemNamespace}),
					)),
				),
				NewConsumerGroup(
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics("t1", "t2"),
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
							ConsumerGroupIdConfig("my.group.id"),
						),
					)),
					ConsumerGroupReplicas(2),
					ConsumerForTrigger(),
				),
			},
			Key: ConsumerGroupTestKey,
			OtherTestData: map[string]interface{}{
				testSchedulerKey: SchedulerFunc(func(vpod scheduler.VPod) ([]eventingduckv1alpha1.Placement, error) {
					return []eventingduckv1alpha1.Placement{
						{PodName: "p1", VReplicas: 1},
						{PodName: "p2", VReplicas: 1},
					}, nil
				}),
			},
			WantDeletes: []clientgotesting.DeleteActionImpl{
				{
					ActionImpl: clientgotesting.ActionImpl{
						Namespace: ConsumerNamespace,
						Resource: schema.GroupVersionResource{
							Group:    kafkainternals.SchemeGroupVersion.Group,
							Version:  kafkainternals.SchemeGroupVersion.Version,
							Resource: "consumers",
						},
					},
					Name: NewConsumer(1).Name,
				},
			},
			WantCreates: []runtime.Object{
				NewConsumer(1,
					ConsumerSpec(NewConsumerSpec(
						ConsumerTopics("t1", "t2"),
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
							ConsumerGroupIdConfig("my.group.id"),
						),
						ConsumerVReplicas(1),
						ConsumerPlacement(kafkainternals.PodBind{PodName: "p1", PodNamespace: systemNamespace}),
					)),
				),
				NewConsumer(2,
					ConsumerSpec(NewConsumerSpec(
						ConsumerTopics("t1", "t2"),
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
							ConsumerGroupIdConfig("my.group.id"),
						),
						ConsumerVReplicas(1),
						ConsumerPlacement(kafkainternals.PodBind{PodName: "p2", PodNamespace: systemNamespace}),
					)),
				),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: func() runtime.Object {
						cg := NewConsumerGroup(
							ConsumerGroupConsumerSpec(NewConsumerSpec(
								ConsumerTopics("t1", "t2"),
								ConsumerConfigs(
									ConsumerBootstrapServersConfig(ChannelBootstrapServers),
									ConsumerGroupIdConfig("my.group.id"),
								),
							)),
							ConsumerGroupReplicas(2),
							ConsumerForTrigger(),
						)
						cg.Status.Placements = []eventingduckv1alpha1.Placement{
							{PodName: "p1", VReplicas: 1},
							{PodName: "p2", VReplicas: 1},
						}
						_ = cg.MarkReconcileConsumersFailed("PropagateSubscriberURI", ErrNoSubscriberURI)
						cg.MarkScheduleSucceeded()
						cg.Status.Replicas = pointer.Int32(0)
						return cg
					}(),
				},
			},
		},
		{
			Name: "Schedulers failed",
			Objects: []runtime.Object{
				NewConsumerGroup(
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics("t1", "t2"),
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
							ConsumerGroupIdConfig("my.group.id"),
						),
					)),
					ConsumerGroupReplicas(2),
					ConsumerForTrigger(),
				),
			},
			Key: ConsumerGroupTestKey,
			OtherTestData: map[string]interface{}{
				testSchedulerKey: SchedulerFunc(func(vpod scheduler.VPod) ([]eventingduckv1alpha1.Placement, error) {
					return nil, io.EOF
				}),
			},
			WantErr: true,
			WantEvents: []string{
				"Warning InternalError failed to schedule consumers: EOF",
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: func() runtime.Object {
						cg := NewConsumerGroup(
							ConsumerGroupConsumerSpec(NewConsumerSpec(
								ConsumerTopics("t1", "t2"),
								ConsumerConfigs(
									ConsumerBootstrapServersConfig(ChannelBootstrapServers),
									ConsumerGroupIdConfig("my.group.id"),
								),
							)),
							ConsumerGroupReplicas(2),
							ConsumerForTrigger(),
						)
						cg.GetConditionSet().Manage(cg.GetStatus()).InitializeConditions()
						_ = cg.MarkScheduleConsumerFailed("Schedule", io.EOF)
						return cg
					}(),
				},
			},
		},
	}

	tt.Test(t, NewFactory(nil, func(ctx context.Context, listers *Listers, env *config.Env, row *TableRow) controller.Reconciler {

		r := Reconciler{
			SchedulerFunc: func(s string) scheduler.Scheduler {
				return row.OtherTestData[testSchedulerKey].(scheduler.Scheduler)
			},
			ConsumerLister:  listers.GetConsumerLister(),
			InternalsClient: fakekafkainternalsclient.Get(ctx).InternalV1alpha1(),
			SecretLister:    listers.GetSecretLister(),
			KubeClient:      kubeclient.Get(ctx),
			NameGenerator:   &CounterGenerator{},
			NewKafkaClient: func(addrs []string, config *sarama.Config) (sarama.Client, error) {
				return &kafkatesting.MockKafkaClient{}, nil
			},
			InitOffsetsFunc: func(ctx context.Context, kafkaClient sarama.Client, kafkaAdminClient sarama.ClusterAdmin, topics []string, consumerGroup string) (int32, error) {
				return 1, nil
			},
			SystemNamespace: systemNamespace,
			Name:            row.Name,
		}

		return consumergroup.NewReconciler(
			ctx,
			logging.FromContext(ctx),
			fakekafkainternalsclient.Get(ctx),
			listers.GetConsumerGroupLister(),
			controller.GetEventRecorder(ctx),
			r,
		)
	}))
}

type CounterGenerator struct {
	counter int
}

func (c *CounterGenerator) GenerateName(base string) string {
	c.counter++
	return fmt.Sprintf("%s%d", base, c.counter)
}
