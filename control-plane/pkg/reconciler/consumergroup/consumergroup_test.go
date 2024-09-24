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
	"time"

	"github.com/IBM/sarama"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgotesting "k8s.io/client-go/testing"
	"k8s.io/utils/pointer"
	"knative.dev/pkg/apis"
	kubeclient "knative.dev/pkg/client/injection/kube/client/fake"

	bindings "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/bindings/v1beta1"
	sources "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/sources/v1beta1"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/prober"

	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	. "knative.dev/pkg/reconciler/testing"

	eventingduckv1alpha1 "knative.dev/eventing/pkg/apis/duck/v1alpha1"
	"knative.dev/eventing/pkg/scheduler"

	kafkainternals "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing/v1alpha1"
	kafkasource "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/sources/v1beta1"
	fakekafkainternalsclient "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/client/fake"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/reconciler/eventing/v1alpha1/consumergroup"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/counter"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	kafkatesting "knative.dev/eventing-kafka-broker/control-plane/pkg/kafka/testing"

	configapis "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/config"
	. "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/testing"

	cm "knative.dev/pkg/configmap/testing"

	kedaclient "knative.dev/eventing-kafka-broker/third_party/pkg/client/injection/client/fake"
)

const (
	testSchedulerKey = "scheduler"
	noTestScheduler  = "no-scheduler"

	systemNamespace = "knative-eventing"
	finalizerName   = "consumergroups.internal.kafka.eventing.knative.dev"
)

var finalizerUpdatedEvent = Eventf(
	corev1.EventTypeNormal,
	"FinalizerUpdate",
	fmt.Sprintf(`Updated %q finalizers`, ConsumerGroupName),
)

func TestReconcileKind(t *testing.T) {
	//TODO: Add tests with KEDA installed
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
				testSchedulerKey: scheduler.SchedulerFunc(func(ctx context.Context, vpod scheduler.VPod) ([]eventingduckv1alpha1.Placement, error) {
					return []eventingduckv1alpha1.Placement{
						{PodName: "p1", VReplicas: 1},
						{PodName: "p2", VReplicas: 1},
					}, nil
				}),
			},
			WantCreates: []runtime.Object{
				NewConsumer(1,
					ConsumerFinalizer(),
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
					ConsumerFinalizer(),
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
							ConsumerGroupStatusSelector(ConsumerLabels),
						)
						cg.Status.Placements = []eventingduckv1alpha1.Placement{
							{PodName: "p1", VReplicas: 1},
							{PodName: "p2", VReplicas: 1},
						}
						_ = cg.MarkReconcileConsumersFailed("PropagateSubscriberURI", ErrNoSubscriberURI)
						cg.MarkScheduleSucceeded()
						cg.MarkAutoscalerDisabled() // KEDA not installed
						return cg
					}(),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
		},
		{
			Name: "Consumers in multiple pods, with pods pending and unknown phase",
			Objects: []runtime.Object{
				NewService(),
				NewDispatcherPod("p1", PodLabel("app", kafkainternals.SourceStatefulSetName), DispatcherLabel(), PodPending()),
				NewDispatcherPod("p2", PodLabel("app", kafkainternals.SourceStatefulSetName), DispatcherLabel()),
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
				testSchedulerKey: scheduler.SchedulerFunc(func(ctx context.Context, vpod scheduler.VPod) ([]eventingduckv1alpha1.Placement, error) {
					return []eventingduckv1alpha1.Placement{
						{PodName: "p1", VReplicas: 1},
						{PodName: "p2", VReplicas: 1},
					}, nil
				}),
			},
			WantCreates: []runtime.Object{
				NewConfigMapWithBinaryData(systemNamespace, "p1", nil, DispatcherPodAsOwnerReference("p1")),
				NewConfigMapWithBinaryData(systemNamespace, "p2", nil, DispatcherPodAsOwnerReference("p2")),
				NewConsumer(1,
					ConsumerFinalizer(),
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
					ConsumerFinalizer(),
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
							ConsumerGroupStatusSelector(ConsumerLabels),
							ConsumerGroupStatusReplicas(0),
							ConsumerForTrigger(),
						)
						cg.Status.Placements = []eventingduckv1alpha1.Placement{
							{PodName: "p1", VReplicas: 1},
							{PodName: "p2", VReplicas: 1},
						}
						_ = cg.MarkReconcileConsumersFailed("PropagateSubscriberURI", ErrNoSubscriberURI)
						cg.MarkScheduleSucceeded()
						cg.MarkAutoscalerDisabled() // KEDA not installed
						return cg
					}(),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			SkipNamespaceValidation: true,
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
							SecretSpec: &kafkainternals.SecretSpec{
								Ref: &kafkainternals.SecretReference{
									Name:      "non-existing secret",
									Namespace: "non-existing secret",
								}},
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
							SecretSpec: &kafkainternals.SecretSpec{
								Ref: &kafkainternals.SecretReference{
									Name:      "non-existing secret",
									Namespace: "non-existing secret",
								}},
						}),
					)),
					ConsumerForTrigger(),
					ConsumerGroupReplicas(2),
				),
			},
			Key: ConsumerGroupTestKey,
			OtherTestData: map[string]interface{}{
				testSchedulerKey: scheduler.SchedulerFunc(func(ctx context.Context, vpod scheduler.VPod) ([]eventingduckv1alpha1.Placement, error) {
					return []eventingduckv1alpha1.Placement{
						{PodName: "p1", VReplicas: 1},
						{PodName: "p2", VReplicas: 1},
					}, nil
				}),
			},
			WantErr: true,
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
				"Warning InternalError failed to initialize consumer group offset: failed to get secret for Kafka cluster auth: failed to get secret non-existing secret/non-existing secret: secrets \"non-existing secret\" not found",
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
									SecretSpec: &kafkainternals.SecretSpec{
										Ref: &kafkainternals.SecretReference{
											Name:      "non-existing secret",
											Namespace: "non-existing secret",
										}},
								}),
							)),
							ConsumerGroupReplicas(2),
							ConsumerGroupStatusSelector(ConsumerLabels),
							ConsumerForTrigger(),
						)
						cg.InitializeConditions()
						_ = cg.MarkInitializeOffsetFailed("InitializeOffset", errors.New("failed to get secret for Kafka cluster auth: failed to get secret non-existing secret/non-existing secret: secrets \"non-existing secret\" not found"))
						return cg
					}(),
				},
			},
		},
		{
			Name: "Consumers in multiple pods, propagate consumer not ready condition",
			Objects: []runtime.Object{
				NewSSLSecret(SecretNamespace, SecretName),
				NewConsumer(2,
					ConsumerSpec(NewConsumerSpec(
						ConsumerTopics("t1", "t2"),
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
							ConsumerGroupIdConfig("my.group.id"),
						),
						ConsumerDelivery(NewConsumerSpecDelivery("", ConsumerInitialOffset(sources.OffsetLatest))),
						ConsumerAuth(&kafkainternals.Auth{
							SecretSpec: &kafkainternals.SecretSpec{
								Ref: &kafkainternals.SecretReference{
									Name:      SecretName,
									Namespace: SecretNamespace,
								}},
						}),
						ConsumerVReplicas(1),
						ConsumerPlacement(kafkainternals.PodBind{
							PodName:      "p2",
							PodNamespace: systemNamespace,
						}),
					)),
					ConsumerNotReady(),
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
							SecretSpec: &kafkainternals.SecretSpec{
								Ref: &kafkainternals.SecretReference{
									Name:      SecretName,
									Namespace: SecretNamespace,
								}},
						}),
					)),
					ConsumerForTrigger(),
					ConsumerGroupReplicas(2),
				),
			},
			Key: ConsumerGroupTestKey,
			OtherTestData: map[string]interface{}{
				testSchedulerKey: scheduler.SchedulerFunc(func(ctx context.Context, vpod scheduler.VPod) ([]eventingduckv1alpha1.Placement, error) {
					return []eventingduckv1alpha1.Placement{
						{PodName: "p1", VReplicas: 1},
						{PodName: "p2", VReplicas: 1},
					}, nil
				}),
			},
			WantErr: false,
			WantCreates: []runtime.Object{
				NewConsumer(1,
					ConsumerFinalizer(),
					ConsumerSpec(NewConsumerSpec(
						ConsumerTopics("t1", "t2"),
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
							ConsumerGroupIdConfig("my.group.id"),
						),
						ConsumerDelivery(NewConsumerSpecDelivery("", ConsumerInitialOffset(sources.OffsetLatest))),
						ConsumerAuth(&kafkainternals.Auth{
							SecretSpec: &kafkainternals.SecretSpec{
								Ref: &kafkainternals.SecretReference{
									Name:      SecretName,
									Namespace: SecretNamespace,
								}},
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
									SecretSpec: &kafkainternals.SecretSpec{
										Ref: &kafkainternals.SecretReference{
											Name:      SecretName,
											Namespace: SecretNamespace,
										}},
								}),
							)),
							ConsumerGroupReplicas(2),
							ConsumerGroupStatusSelector(ConsumerLabels),
							ConsumerGroupStatusReplicas(0),
							ConsumerForTrigger(),
						)
						cg.Status.Placements = []eventingduckv1alpha1.Placement{
							{PodName: "p1", VReplicas: 1},
							{PodName: "p2", VReplicas: 1},
						}
						_ = cg.MarkReconcileConsumersFailedCondition(&apis.Condition{
							Type:    kafkainternals.ConsumerConditionBind,
							Status:  corev1.ConditionFalse,
							Reason:  "ConsumerBinding",
							Message: "failed to bind resource to pod: EOF",
						})
						cg.MarkScheduleSucceeded()
						cg.MarkAutoscalerDisabled() // KEDA not installed
						return cg
					}(),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
		},
		{
			Name: "Consumers in multiple pods, with auth spec, one exists - ready",
			Objects: []runtime.Object{
				NewSSLSecret(SecretNamespace, SecretName),
				NewConsumer(2,
					ConsumerSpec(NewConsumerSpec(
						ConsumerTopics("t1", "t2"),
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
							ConsumerGroupIdConfig("my.group.id"),
						),
						ConsumerDelivery(NewConsumerSpecDelivery("", ConsumerInitialOffset(sources.OffsetLatest))),
						ConsumerAuth(&kafkainternals.Auth{
							SecretSpec: &kafkainternals.SecretSpec{
								Ref: &kafkainternals.SecretReference{
									Name:      SecretName,
									Namespace: SecretNamespace,
								}},
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
							SecretSpec: &kafkainternals.SecretSpec{
								Ref: &kafkainternals.SecretReference{
									Name:      SecretName,
									Namespace: SecretNamespace,
								}},
						}),
					)),
					ConsumerForTrigger(),
					ConsumerGroupReplicas(2),
				),
			},
			Key: ConsumerGroupTestKey,
			OtherTestData: map[string]interface{}{
				testSchedulerKey: scheduler.SchedulerFunc(func(ctx context.Context, vpod scheduler.VPod) ([]eventingduckv1alpha1.Placement, error) {
					return []eventingduckv1alpha1.Placement{
						{PodName: "p1", VReplicas: 1},
						{PodName: "p2", VReplicas: 1},
					}, nil
				}),
			},
			WantCreates: []runtime.Object{
				NewConsumer(1,
					ConsumerFinalizer(),
					ConsumerSpec(NewConsumerSpec(
						ConsumerTopics("t1", "t2"),
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
							ConsumerGroupIdConfig("my.group.id"),
						),
						ConsumerDelivery(NewConsumerSpecDelivery("", ConsumerInitialOffset(sources.OffsetLatest))),
						ConsumerAuth(&kafkainternals.Auth{
							SecretSpec: &kafkainternals.SecretSpec{
								Ref: &kafkainternals.SecretReference{
									Name:      SecretName,
									Namespace: SecretNamespace,
								}},
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
									SecretSpec: &kafkainternals.SecretSpec{
										Ref: &kafkainternals.SecretReference{
											Name:      SecretName,
											Namespace: SecretNamespace,
										}},
								}),
							)),
							ConsumerGroupReplicas(2),
							ConsumerGroupStatusSelector(ConsumerLabels),
							ConsumerGroupStatusReplicas(1),
							ConsumerForTrigger(),
						)
						cg.Status.Placements = []eventingduckv1alpha1.Placement{
							{PodName: "p1", VReplicas: 1},
							{PodName: "p2", VReplicas: 1},
						}
						cg.MarkReconcileConsumersSucceeded()
						cg.MarkScheduleSucceeded()
						cg.MarkAutoscalerDisabled() // KEDA not installed
						cg.Status.SubscriberURI = ConsumerSubscriberURI
						return cg
					}(),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
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
				testSchedulerKey: scheduler.SchedulerFunc(func(ctx context.Context, vpod scheduler.VPod) ([]eventingduckv1alpha1.Placement, error) {
					return []eventingduckv1alpha1.Placement{
						{PodName: "p1", VReplicas: 1},
						{PodName: "p2", VReplicas: 1},
					}, nil
				}),
			},
			WantErr: true,
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
				"Warning InternalError failed to initialize consumer group offset: failed to get secret for Kafka cluster auth: failed to read secret test-cg-ns/non-existing secret: secret \"non-existing secret\" not found",
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
							ConsumerGroupStatusSelector(ConsumerLabels),
							ConsumerForTrigger(),
						)
						cg.InitializeConditions()
						_ = cg.MarkInitializeOffsetFailed("InitializeOffset", errors.New("failed to get secret for Kafka cluster auth: failed to read secret test-cg-ns/non-existing secret: secret \"non-existing secret\" not found"))
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
				testSchedulerKey: scheduler.SchedulerFunc(func(ctx context.Context, vpod scheduler.VPod) ([]eventingduckv1alpha1.Placement, error) {
					return []eventingduckv1alpha1.Placement{
						{PodName: "p1", VReplicas: 1},
						{PodName: "p2", VReplicas: 1},
					}, nil
				}),
			},
			WantCreates: []runtime.Object{
				NewConsumer(1,
					ConsumerFinalizer(),
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
							ConsumerGroupStatusSelector(ConsumerLabels),
							ConsumerForTrigger(),
						)
						cg.Status.Placements = []eventingduckv1alpha1.Placement{
							{PodName: "p1", VReplicas: 1},
							{PodName: "p2", VReplicas: 1},
						}
						cg.MarkReconcileConsumersSucceeded()
						cg.MarkScheduleSucceeded()
						cg.MarkAutoscalerDisabled() // KEDA not installed
						cg.Status.SubscriberURI = ConsumerSubscriberURI
						return cg
					}(),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
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
				testSchedulerKey: scheduler.SchedulerFunc(func(ctx context.Context, vpod scheduler.VPod) ([]eventingduckv1alpha1.Placement, error) {
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
							ConsumerGroupStatusSelector(ConsumerLabels),
						)
						cg.Status.Placements = []eventingduckv1alpha1.Placement{
							{PodName: "p1", VReplicas: 1},
						}
						_ = cg.MarkReconcileConsumersFailed("PropagateSubscriberURI", ErrNoSubscriberURI)
						cg.MarkScheduleSucceeded()
						cg.MarkAutoscalerDisabled() // KEDA not installed
						return cg
					}(),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
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
				testSchedulerKey: scheduler.SchedulerFunc(func(ctx context.Context, vpod scheduler.VPod) ([]eventingduckv1alpha1.Placement, error) {
					return []eventingduckv1alpha1.Placement{
						{PodName: "p1", VReplicas: 1},
						{PodName: "p2", VReplicas: 1},
					}, nil
				}),
			},
			WantCreates: []runtime.Object{
				NewConsumer(1,
					ConsumerFinalizer(),
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
							ConsumerGroupStatusSelector(ConsumerLabels),
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
						cg.MarkAutoscalerDisabled() // KEDA not installed
						return cg
					}(),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
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
				testSchedulerKey: scheduler.SchedulerFunc(func(ctx context.Context, vpod scheduler.VPod) ([]eventingduckv1alpha1.Placement, error) {
					return []eventingduckv1alpha1.Placement{
						{PodName: "p1", VReplicas: 1},
						{PodName: "p2", VReplicas: 1},
					}, nil
				}),
			},
			WantCreates: []runtime.Object{
				NewConsumer(1,
					ConsumerFinalizer(),
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
							ConsumerGroupStatusSelector(ConsumerLabels),
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
						cg.MarkAutoscalerDisabled() // KEDA not installed
						cg.Status.SubscriberURI = ConsumerSubscriberURI
						return cg
					}(),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
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
						ConsumerDelivery(NewConsumerSpecDelivery(kafkasource.Ordered,
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
						ConsumerDelivery(NewConsumerSpecDelivery(kafkasource.Ordered,
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
				testSchedulerKey: scheduler.SchedulerFunc(func(ctx context.Context, vpod scheduler.VPod) ([]eventingduckv1alpha1.Placement, error) {
					return []eventingduckv1alpha1.Placement{
						{PodName: "p1", VReplicas: 1},
						{PodName: "p2", VReplicas: 2},
					}, nil
				}),
			},
			WantCreates: []runtime.Object{
				NewConsumer(1,
					ConsumerFinalizer(),
					ConsumerSpec(NewConsumerSpec(
						ConsumerTopics("t1", "t2"),
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
							ConsumerGroupIdConfig("my.group.id"),
						),
						ConsumerDelivery(NewConsumerSpecDelivery(kafkasource.Ordered,
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
							ConsumerDelivery(NewConsumerSpecDelivery(kafkasource.Ordered,
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
								ConsumerDelivery(NewConsumerSpecDelivery(kafkasource.Ordered,
									NewConsumerSpecDeliveryDeadLetterSink(),
									ConsumerInitialOffset(sources.OffsetLatest),
								)),
							)),
							ConsumerGroupStatusSelector(ConsumerLabels),
							ConsumerGroupReplicas(3),
							ConsumerForTrigger(),
						)
						cg.Status.Placements = []eventingduckv1alpha1.Placement{
							{PodName: "p1", VReplicas: 1},
							{PodName: "p2", VReplicas: 2},
						}
						cg.MarkReconcileConsumersSucceeded()
						cg.MarkScheduleSucceeded()
						cg.MarkAutoscalerDisabled() // KEDA not installed
						cg.Status.SubscriberURI = ConsumerSubscriberURI
						cg.Status.DeadLetterSinkURI = ConsumerDeadLetterSinkURI
						cg.Status.Replicas = pointer.Int32(1)
						return cg
					}(),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
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
				testSchedulerKey: scheduler.SchedulerFunc(func(ctx context.Context, vpod scheduler.VPod) ([]eventingduckv1alpha1.Placement, error) {
					return []eventingduckv1alpha1.Placement{
						{PodName: "p1", VReplicas: 1},
						{PodName: "p2", VReplicas: 2},
					}, nil
				}),
			},
			WantCreates: []runtime.Object{
				NewConsumer(1,
					ConsumerFinalizer(),
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
							ConsumerGroupStatusSelector(ConsumerLabels),
							ConsumerGroupReplicas(3),
							ConsumerForTrigger(),
						)
						cg.Status.Placements = []eventingduckv1alpha1.Placement{
							{PodName: "p1", VReplicas: 1},
							{PodName: "p2", VReplicas: 2},
						}
						cg.MarkReconcileConsumersSucceeded()
						cg.MarkScheduleSucceeded()
						cg.MarkAutoscalerDisabled() // KEDA not installed
						cg.Status.SubscriberURI = ConsumerSubscriberURI
						cg.Status.Replicas = pointer.Int32(1)
						return cg
					}(),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
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
				testSchedulerKey: scheduler.SchedulerFunc(func(ctx context.Context, vpod scheduler.VPod) ([]eventingduckv1alpha1.Placement, error) {
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
					ConsumerFinalizer(),
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
					ConsumerFinalizer(),
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
							ConsumerGroupStatusSelector(ConsumerLabels),
							ConsumerGroupReplicas(2),
							ConsumerForTrigger(),
						)
						cg.Status.Placements = []eventingduckv1alpha1.Placement{
							{PodName: "p1", VReplicas: 1},
							{PodName: "p2", VReplicas: 1},
						}
						_ = cg.MarkReconcileConsumersFailed("PropagateSubscriberURI", ErrNoSubscriberURI)
						cg.MarkScheduleSucceeded()
						cg.MarkAutoscalerDisabled() // KEDA not installed
						cg.Status.Replicas = pointer.Int32(0)
						return cg
					}(),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
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
				testSchedulerKey: scheduler.SchedulerFunc(func(ctx context.Context, vpod scheduler.VPod) ([]eventingduckv1alpha1.Placement, error) {
					return nil, io.EOF
				}),
			},
			WantErr: true,
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
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
							ConsumerGroupStatusSelector(ConsumerLabels),
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

		ctx, _ = kedaclient.With(ctx)
		store := configapis.NewStore(ctx)
		_, exampleConfig := cm.ConfigMapsFromTestFile(t, configapis.FlagsConfigName)
		store.OnConfigChanged(exampleConfig)

		r := &Reconciler{
			SchedulerFunc: func(s string) (Scheduler, bool) {
				ss := row.OtherTestData[testSchedulerKey].(scheduler.Scheduler)
				return Scheduler{
					Scheduler: ss,
					SchedulerConfig: SchedulerConfig{
						StatefulSetName: kafkainternals.SourceStatefulSetName,
					},
				}, true
			},
			ConsumerLister:  listers.GetConsumerLister(),
			InternalsClient: fakekafkainternalsclient.Get(ctx).InternalV1alpha1(),
			SecretLister:    listers.GetSecretLister(),
			ConfigMapLister: listers.GetConfigMapLister(),
			PodLister:       listers.GetPodLister(),
			KubeClient:      kubeclient.Get(ctx),
			KedaClient:      kedaclient.Get(ctx),
			NameGenerator:   &CounterGenerator{},
			GetKafkaClient: func(_ context.Context, addrs []string, _ *corev1.Secret) (sarama.Client, error) {
				return &kafkatesting.MockKafkaClient{}, nil
			},
			GetKafkaClusterAdmin: func(_ context.Context, _ []string, _ *corev1.Secret) (sarama.ClusterAdmin, error) {
				return &kafkatesting.MockKafkaClusterAdmin{
					T: t,
				}, nil
			},
			InitOffsetsFunc: func(ctx context.Context, kafkaClient sarama.Client, kafkaAdminClient sarama.ClusterAdmin, topics []string, consumerGroup string) (int32, error) {
				return 1, nil
			},
			SystemNamespace:                    systemNamespace,
			AutoscalerConfig:                   "",
			DeleteConsumerGroupMetadataCounter: counter.NewExpiringCounter(ctx),
			InitOffsetLatestInitialOffsetCache: prober.NewLocalExpiringCache[string, prober.Status, struct{}](ctx, time.Second),
			EnqueueKey:                         func(key string) {},
		}

		r.KafkaFeatureFlags = configapis.FromContext(store.ToContext(ctx))

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

func DispatcherLabel() PodOption {
	return PodLabel("app.kubernetes.io/kind", "kafka-dispatcher")
}

func TestReconcileKindNoAutoscaler(t *testing.T) {

	tt := TableTest{
		{
			Name: "Consumers in multiple pods with autoscaler disabled",
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
				testSchedulerKey: scheduler.SchedulerFunc(func(ctx context.Context, vpod scheduler.VPod) ([]eventingduckv1alpha1.Placement, error) {
					return []eventingduckv1alpha1.Placement{
						{PodName: "p1", VReplicas: 1},
						{PodName: "p2", VReplicas: 1},
					}, nil
				}),
			},
			WantCreates: []runtime.Object{
				NewConsumer(1,
					ConsumerFinalizer(),
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
							ConsumerGroupStatusSelector(ConsumerLabels),
							ConsumerGroupStatusReplicas(1),
							ConsumerForTrigger(),
						)
						cg.Status.Placements = []eventingduckv1alpha1.Placement{
							{PodName: "p1", VReplicas: 1},
							{PodName: "p2", VReplicas: 1},
						}
						cg.MarkReconcileConsumersSucceeded()
						cg.MarkScheduleSucceeded()
						cg.MarkAutoscalerDisabled() // autoscaler feature disabled
						cg.Status.SubscriberURI = ConsumerSubscriberURI
						return cg
					}(),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
		},
	}

	tt.Test(t, NewFactory(nil, func(ctx context.Context, listers *Listers, env *config.Env, row *TableRow) controller.Reconciler {

		ctx, _ = kedaclient.With(ctx)

		r := &Reconciler{
			SchedulerFunc: func(s string) (Scheduler, bool) {
				ss := row.OtherTestData[testSchedulerKey].(scheduler.Scheduler)
				return Scheduler{
					Scheduler: ss,
				}, true
			},
			ConsumerLister:  listers.GetConsumerLister(),
			InternalsClient: fakekafkainternalsclient.Get(ctx).InternalV1alpha1(),
			SecretLister:    listers.GetSecretLister(),
			ConfigMapLister: listers.GetConfigMapLister(),
			PodLister:       listers.GetPodLister(),
			KubeClient:      kubeclient.Get(ctx),
			KedaClient:      kedaclient.Get(ctx),
			NameGenerator:   &CounterGenerator{},
			GetKafkaClient: func(_ context.Context, addrs []string, _ *corev1.Secret) (sarama.Client, error) {
				return &kafkatesting.MockKafkaClient{}, nil
			},
			GetKafkaClusterAdmin: func(_ context.Context, _ []string, _ *corev1.Secret) (sarama.ClusterAdmin, error) {
				return &kafkatesting.MockKafkaClusterAdmin{
					T: t,
				}, nil
			},
			InitOffsetsFunc: func(ctx context.Context, kafkaClient sarama.Client, kafkaAdminClient sarama.ClusterAdmin, topics []string, consumerGroup string) (int32, error) {
				return 1, nil
			},
			SystemNamespace:                    systemNamespace,
			DeleteConsumerGroupMetadataCounter: counter.NewExpiringCounter(ctx),
			InitOffsetLatestInitialOffsetCache: prober.NewLocalExpiringCache[string, prober.Status, struct{}](ctx, time.Second),
			EnqueueKey:                         func(key string) {},
		}

		r.KafkaFeatureFlags = configapis.DefaultFeaturesConfig()

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

func TestFinalizeKind(t *testing.T) {

	testKey := fmt.Sprintf("%s/%s", ConsumerGroupNamespace, ConsumerGroupName)

	table := TableTest{
		{
			Name: "Finalize normal - no consumers",
			Objects: []runtime.Object{
				NewSASLSSLSecret(ConsumerGroupNamespace, SecretName),
				NewDeletedConsumeGroup(
					ConsumerGroupReplicas(2),
					ConsumerGroupOwnerRef(SourceAsOwnerReference()),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
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
				),
			},
			Key: testKey,
			OtherTestData: map[string]interface{}{
				testSchedulerKey: scheduler.SchedulerFunc(func(ctx context.Context, vpod scheduler.VPod) ([]eventingduckv1alpha1.Placement, error) {
					return nil, nil
				}),
			},
			SkipNamespaceValidation: true, // WantCreates compare the source namespace with configmap namespace, so skip it
		},
		{
			Name: "Finalize normal - with consumers",
			Objects: []runtime.Object{
				NewSASLSSLSecret(ConsumerGroupNamespace, SecretName),
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
				NewDeletedConsumeGroup(
					ConsumerGroupReplicas(1),
					ConsumerGroupOwnerRef(SourceAsOwnerReference()),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
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
				),
			},
			Key: testKey,
			OtherTestData: map[string]interface{}{
				testSchedulerKey: scheduler.SchedulerFunc(func(ctx context.Context, vpod scheduler.VPod) ([]eventingduckv1alpha1.Placement, error) {
					return nil, nil
				}),
			},
			WantDeletes: []clientgotesting.DeleteActionImpl{
				{
					ActionImpl: clientgotesting.ActionImpl{
						Namespace: ConsumerGroupNamespace,
						Resource: schema.GroupVersionResource{
							Group:    kafkainternals.SchemeGroupVersion.Group,
							Version:  kafkainternals.SchemeGroupVersion.Version,
							Resource: "consumers",
						},
					},
					Name: fmt.Sprintf("%s-%d", ConsumerNamePrefix, 1),
				},
			},
			SkipNamespaceValidation: true, // WantCreates compare the source namespace with configmap namespace, so skip it
		},
		{
			Name: "Finalize normal - with consumers, no valid scheduler",
			Objects: []runtime.Object{
				NewSASLSSLSecret(ConsumerGroupNamespace, SecretName),
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
				NewDeletedConsumeGroup(
					ConsumerGroupReplicas(1),
					ConsumerGroupOwnerRef(SourceAsOwnerReference()),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
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
				),
			},
			Key: testKey,
			OtherTestData: map[string]interface{}{
				noTestScheduler: true,
			},
			WantDeletes: []clientgotesting.DeleteActionImpl{
				{
					ActionImpl: clientgotesting.ActionImpl{
						Namespace: ConsumerGroupNamespace,
						Resource: schema.GroupVersionResource{
							Group:    kafkainternals.SchemeGroupVersion.Group,
							Version:  kafkainternals.SchemeGroupVersion.Version,
							Resource: "consumers",
						},
					},
					Name: fmt.Sprintf("%s-%d", ConsumerNamePrefix, 1),
				},
			},
			SkipNamespaceValidation: true, // WantCreates compare the source namespace with configmap namespace, so skip it
		},
		{
			Name: "Finalize normal - with consumers and existing placements",
			Objects: []runtime.Object{
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
				NewDeletedConsumeGroup(
					ConsumerGroupOwnerRef(SourceAsOwnerReference()),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
							ConsumerGroupIdConfig("my.group.id"),
						),
					)),
				),
			},
			Key: testKey,
			OtherTestData: map[string]interface{}{
				testSchedulerKey: scheduler.SchedulerFunc(func(ctx context.Context, vpod scheduler.VPod) ([]eventingduckv1alpha1.Placement, error) {
					return nil, nil
				}),
			},
			WantDeletes: []clientgotesting.DeleteActionImpl{
				{
					ActionImpl: clientgotesting.ActionImpl{
						Namespace: ConsumerGroupNamespace,
						Resource: schema.GroupVersionResource{
							Group:    kafkainternals.SchemeGroupVersion.Group,
							Version:  kafkainternals.SchemeGroupVersion.Version,
							Resource: "consumers",
						},
					},
					Name: fmt.Sprintf("%s-%d", ConsumerNamePrefix, 1),
				},
			},
			SkipNamespaceValidation: true, // WantCreates compare the source namespace with configmap namespace, so skip it
		},
		{
			Name: "Finalize normal - failed consumer group deletion with " + sarama.ErrUnknownTopicOrPartition.Error(),
			Objects: []runtime.Object{
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
				NewDeletedConsumeGroup(
					ConsumerGroupOwnerRef(SourceAsOwnerReference()),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
							ConsumerGroupIdConfig("my.group.id"),
						),
					)),
				),
			},
			Key: testKey,
			OtherTestData: map[string]interface{}{
				testSchedulerKey: scheduler.SchedulerFunc(func(ctx context.Context, vpod scheduler.VPod) ([]eventingduckv1alpha1.Placement, error) {
					return nil, nil
				}),
				kafkatesting.ErrorOnDeleteConsumerGroupTestKey: sarama.ErrUnknownTopicOrPartition,
			},
			WantDeletes: []clientgotesting.DeleteActionImpl{
				{
					ActionImpl: clientgotesting.ActionImpl{
						Namespace: ConsumerGroupNamespace,
						Resource: schema.GroupVersionResource{
							Group:    kafkainternals.SchemeGroupVersion.Group,
							Version:  kafkainternals.SchemeGroupVersion.Version,
							Resource: "consumers",
						},
					},
					Name: fmt.Sprintf("%s-%d", ConsumerNamePrefix, 1),
				},
			},
			SkipNamespaceValidation: true, // WantCreates compare the source namespace with configmap namespace, so skip it
		},
		{
			Name: "Finalize normal - failed consumer group deletion with " + sarama.ErrGroupIDNotFound.Error(),
			Objects: []runtime.Object{
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
				NewDeletedConsumeGroup(
					ConsumerGroupOwnerRef(SourceAsOwnerReference()),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
							ConsumerGroupIdConfig("my.group.id"),
						),
					)),
				),
			},
			Key: testKey,
			OtherTestData: map[string]interface{}{
				testSchedulerKey: scheduler.SchedulerFunc(func(ctx context.Context, vpod scheduler.VPod) ([]eventingduckv1alpha1.Placement, error) {
					return nil, nil
				}),
				kafkatesting.ErrorOnDeleteConsumerGroupTestKey: sarama.ErrGroupIDNotFound,
			},
			WantDeletes: []clientgotesting.DeleteActionImpl{
				{
					ActionImpl: clientgotesting.ActionImpl{
						Namespace: ConsumerGroupNamespace,
						Resource: schema.GroupVersionResource{
							Group:    kafkainternals.SchemeGroupVersion.Group,
							Version:  kafkainternals.SchemeGroupVersion.Version,
							Resource: "consumers",
						},
					},
					Name: fmt.Sprintf("%s-%d", ConsumerNamePrefix, 1),
				},
			},
			SkipNamespaceValidation: true, // WantCreates compare the source namespace with configmap namespace, so skip it
		},
		{
			Name: "Finalize error - failed consumer group deletion with " + sarama.ErrClusterAuthorizationFailed.Error(),
			Objects: []runtime.Object{
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
				NewDeletedConsumeGroup(
					ConsumerGroupOwnerRef(SourceAsOwnerReference()),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
							ConsumerGroupIdConfig("my.group.id"),
						),
					)),
				),
			},
			WantErr: true,
			Key:     testKey,
			OtherTestData: map[string]interface{}{
				testSchedulerKey: scheduler.SchedulerFunc(func(ctx context.Context, vpod scheduler.VPod) ([]eventingduckv1alpha1.Placement, error) {
					return nil, nil
				}),
				kafkatesting.ErrorOnDeleteConsumerGroupTestKey: sarama.ErrClusterAuthorizationFailed,
			},
			WantDeletes: []clientgotesting.DeleteActionImpl{
				{
					ActionImpl: clientgotesting.ActionImpl{
						Namespace: ConsumerGroupNamespace,
						Resource: schema.GroupVersionResource{
							Group:    kafkainternals.SchemeGroupVersion.Group,
							Version:  kafkainternals.SchemeGroupVersion.Version,
							Resource: "consumers",
						},
					},
					Name: fmt.Sprintf("%s-%d", ConsumerNamePrefix, 1),
				},
			},
			WantEvents: []string{
				Eventf(
					corev1.EventTypeWarning,
					"InternalError",
					"failed to delete consumer group offset: unable to delete the consumer group my.group.id: "+sarama.ErrClusterAuthorizationFailed.Error()+" (retry num 1)",
				),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: func() runtime.Object {
						cg := NewDeletedConsumeGroup(
							ConsumerGroupOwnerRef(SourceAsOwnerReference()),
							ConsumerGroupConsumerSpec(NewConsumerSpec(
								ConsumerConfigs(
									ConsumerBootstrapServersConfig(ChannelBootstrapServers),
									ConsumerGroupIdConfig("my.group.id"),
								),
							)),
						)

						_ = cg.MarkDeleteOffsetFailed("DeleteConsumerGroupOffset", fmt.Errorf("unable to delete the consumer group my.group.id: kafka server: The client is not authorized to send this request type (retry num 1)"))

						return cg
					}(),
				},
			},
			SkipNamespaceValidation: true, // WantCreates compare the source namespace with configmap namespace, so skip it
		},
	}

	table.Test(t, NewFactory(nil, func(ctx context.Context, listers *Listers, env *config.Env, row *TableRow) controller.Reconciler {

		errorOnDeleteKafkaCG := row.OtherTestData[kafkatesting.ErrorOnDeleteConsumerGroupTestKey]

		r := &Reconciler{
			SchedulerFunc: func(s string) (Scheduler, bool) {
				if noScheduler, ok := row.OtherTestData[noTestScheduler]; ok && noScheduler.(bool) == true {
					return Scheduler{}, false
				}
				ss := row.OtherTestData[testSchedulerKey].(scheduler.Scheduler)
				return Scheduler{
					Scheduler: ss,
				}, true
			},
			ConsumerLister:  listers.GetConsumerLister(),
			InternalsClient: fakekafkainternalsclient.Get(ctx).InternalV1alpha1(),
			SecretLister:    listers.GetSecretLister(),
			ConfigMapLister: listers.GetConfigMapLister(),
			PodLister:       listers.GetPodLister(),
			GetKafkaClient: func(_ context.Context, addrs []string, _ *corev1.Secret) (sarama.Client, error) {
				return &kafkatesting.MockKafkaClient{}, nil
			},
			GetKafkaClusterAdmin: func(_ context.Context, _ []string, _ *corev1.Secret) (sarama.ClusterAdmin, error) {
				return &kafkatesting.MockKafkaClusterAdmin{
					T:                          t,
					ErrorOnDeleteConsumerGroup: ErrorAssertOrNil(errorOnDeleteKafkaCG),
				}, nil
			},
			KafkaFeatureFlags:                  configapis.DefaultFeaturesConfig(),
			DeleteConsumerGroupMetadataCounter: counter.NewExpiringCounter(ctx),
			InitOffsetLatestInitialOffsetCache: prober.NewLocalExpiringCache[string, prober.Status, struct{}](ctx, time.Second),
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

func patchFinalizers() clientgotesting.PatchActionImpl {
	action := clientgotesting.PatchActionImpl{}
	action.Name = ConsumerGroupName
	action.Namespace = ConsumerGroupNamespace
	patch := `{"metadata":{"finalizers":["` + finalizerName + `"],"resourceVersion":""}}`
	action.Patch = []byte(patch)
	return action
}
