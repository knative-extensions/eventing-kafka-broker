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

package v2

import (
	"context"
	"fmt"
	"testing"

	"github.com/Shopify/sarama"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgotesting "k8s.io/client-go/testing"
	"k8s.io/utils/pointer"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/kmeta"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/network"
	. "knative.dev/pkg/reconciler/testing"
	"knative.dev/pkg/system"

	kubeclient "knative.dev/pkg/client/injection/kube/client/fake"

	internals "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	kafkatesting "knative.dev/eventing-kafka-broker/control-plane/pkg/kafka/testing"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/prober"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/prober/probertesting"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/receiver"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	. "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/testing"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/security"

	messagingv1beta "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	fakeeventingkafkaclient "knative.dev/eventing-kafka/pkg/client/injection/client/fake"
	messagingv1beta1kafkachannelreconciler "knative.dev/eventing-kafka/pkg/client/injection/reconciler/messaging/v1beta1/kafkachannel"

	kafkainternals "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing/v1alpha1"
	fakeconsumergroupinformer "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/client/fake"
)

const (
	testProber = "testProber"

	finalizerName                 = "kafkachannels.messaging.knative.dev"
	TestExpectedDataNumPartitions = "TestExpectedDataNumPartitions"
	TestExpectedReplicationFactor = "TestExpectedReplicationFactor"
)

var finalizerUpdatedEvent = Eventf(
	corev1.EventTypeNormal,
	"FinalizerUpdate",
	fmt.Sprintf(`Updated %q finalizers`, ChannelName),
)

var DefaultEnv = &config.Env{
	DataPlaneConfigMapNamespace: "knative-eventing",
	DataPlaneConfigMapName:      "kafka-channel-channels-subscriptions",
	GeneralConfigMapName:        "kafka-channel-config",
	IngressName:                 "kafka-channel-ingress",
	SystemNamespace:             "knative-eventing",
	DataPlaneConfigFormat:       base.Json,
}

func TestReconcileKind(t *testing.T) {

	messagingv1beta.RegisterAlternateKafkaChannelConditionSet(conditionSet)

	env := *DefaultEnv
	testKey := fmt.Sprintf("%s/%s", ChannelNamespace, ChannelName)

	table := TableTest{
		{
			Name: "bad workqueue key",
			// Make sure Reconcile handles bad keys.
			Key: "too/many/parts",
		},
		{
			Name: "key not found",
			// Make sure Reconcile handles good keys that don't exist.
			Key: "foo/not-found",
		},
		{
			Name: "Channel not found",
			Key:  testKey,
		},
		{
			Name: "Channel is being deleted, probe not ready",
			Key:  testKey,
			Objects: []runtime.Object{
				NewChannel(
					WithInitKafkaChannelConditions,
					WithDeletedTimeStamp),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				NewConfigMapWithBinaryData(&env, nil),
			},
			OtherTestData: map[string]interface{}{
				testProber: probertesting.MockProber(prober.StatusNotReady),
			},
		},
		{
			Name: "Channel is being deleted, probe ready",
			Key:  testKey,
			Objects: []runtime.Object{
				NewChannel(
					WithInitKafkaChannelConditions,
					WithDeletedTimeStamp),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				NewConfigMapWithBinaryData(&env, nil),
			},
			WantErr: true,
			OtherTestData: map[string]interface{}{
				testProber: probertesting.MockProber(prober.StatusReady),
			},
		},
		{
			Name: "Channel is being deleted with contract resource deletion",
			Key:  testKey,
			Objects: []runtime.Object{
				NewChannel(
					WithInitKafkaChannelConditions,
					WithDeletedTimeStamp),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				NewConfigMapFromContract(&contract.Contract{
					Generation: 0,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Ingress: &contract.Ingress{
								Host: receiver.Host(ChannelNamespace, ChannelName),
							},
						},
					},
				}, &env),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Generation: 1,
				}),
			},
			OtherTestData: map[string]interface{}{
				testProber: probertesting.MockProber(prober.StatusNotReady),
			},
		},
		{
			Name: "Reconciled normal - no subscription (no cons group) - no auth",
			Objects: []runtime.Object{
				NewChannel(),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				ChannelReceiverPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				ChannelDispatcherPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Ingress: &contract.Ingress{
								Host: receiver.Host(ChannelNamespace, ChannelName),
							},
						},
					},
				}),
				ChannelReceiverPodUpdate(env.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
				ChannelDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantCreates: []runtime.Object{
				NewConfigMapWithBinaryData(&env, nil),
				NewPerChannelService(&env),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigParsed,
						StatusConfigMapUpdatedReady(&env),
						StatusTopicReadyWithName(ChannelTopic()),
						ChannelAddressable(&env),
						StatusProbeSucceeded,
						StatusChannelSubscribers(),
					),
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
			Name: "Reconciled normal - with delivery",
			Objects: []runtime.Object{
				NewChannel(
					WithChannelDelivery(&eventingduck.DeliverySpec{
						DeadLetterSink: ServiceDestination,
						Retry:          pointer.Int32(5),
					}),
				),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				ChannelReceiverPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				ChannelDispatcherPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Ingress: &contract.Ingress{
								Host: receiver.Host(ChannelNamespace, ChannelName),
							},
							EgressConfig: &contract.EgressConfig{
								DeadLetter: ServiceURL,
								Retry:      5,
							},
						},
					},
				}),
				ChannelReceiverPodUpdate(env.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
				ChannelDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantCreates: []runtime.Object{
				NewConfigMapWithBinaryData(&env, nil),
				NewPerChannelService(&env),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithChannelDelivery(&eventingduck.DeliverySpec{
							DeadLetterSink: ServiceDestination,
							Retry:          pointer.Int32(5),
						}),
						WithInitKafkaChannelConditions,
						StatusConfigParsed,
						StatusConfigMapUpdatedReady(&env),
						StatusTopicReadyWithName(ChannelTopic()),
						ChannelAddressable(&env),
						StatusProbeSucceeded,
						StatusChannelSubscribers(),
						WithChannelDeadLetterSinkURI(ServiceURL),
					),
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
			Name: "Reconciled failed - no subscription (no cons group) - probe " + prober.StatusNotReady.String(),
			Objects: []runtime.Object{
				NewChannel(),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Ingress: &contract.Ingress{
								Host: receiver.Host(ChannelNamespace, ChannelName),
							},
						},
					},
				}),
			},
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantCreates: []runtime.Object{
				NewConfigMapWithBinaryData(&env, nil),
				NewPerChannelService(&env),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigParsed,
						StatusConfigMapUpdatedReady(&env),
						StatusTopicReadyWithName(ChannelTopic()),
						StatusProbeFailed(prober.StatusNotReady),
						StatusChannelSubscribers(),
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			OtherTestData: map[string]interface{}{
				testProber: probertesting.MockProber(prober.StatusNotReady),
			},
		},
		{
			Name: "Reconciled failed - no subscription (no cons group) - probe " + prober.StatusUnknown.String(),
			Objects: []runtime.Object{
				NewChannel(),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Ingress: &contract.Ingress{
								Host: receiver.Host(ChannelNamespace, ChannelName),
							},
						},
					},
				}),
			},
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantCreates: []runtime.Object{
				NewConfigMapWithBinaryData(&env, nil),
				NewPerChannelService(&env),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigParsed,
						StatusConfigMapUpdatedReady(&env),
						StatusTopicReadyWithName(ChannelTopic()),
						StatusProbeFailed(prober.StatusUnknown),
						StatusChannelSubscribers(),
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			OtherTestData: map[string]interface{}{
				testProber: probertesting.MockProber(prober.StatusUnknown),
			},
		},
		{
			Name: "Reconciled normal - with single fresh subscriber - cg unknown - no auth",
			Objects: []runtime.Object{
				NewChannel(WithSubscribers(Subscriber1(WithFreshSubscriber))),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				NewConfigMapWithBinaryData(&env, nil),
			},
			Key: testKey,
			WantCreates: []runtime.Object{
				NewPerChannelService(&env),
				NewConsumerGroup(
					WithConsumerGroupName(Subscription1UUID),
					WithConsumerGroupNamespace(ChannelNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(NewChannel())),
					WithConsumerGroupMetaLabels(OwnerAsChannelLabel),
					WithConsumerGroupLabels(ConsumerSubscription1Label),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(ChannelTopic()),
						ConsumerConfigs(
							ConsumerGroupIdConfig(consumerGroup(NewChannel(), GetSubscriberSpec(Subscriber1(WithFreshSubscriber)))),
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
						),
						ConsumerDelivery(NewConsumerSpecDelivery(internals.Ordered)),
						ConsumerSubscriber(NewConsumerSpecSubscriber(Subscription1URI)),
					)),
				),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Ingress: &contract.Ingress{
								Host: receiver.Host(ChannelNamespace, ChannelName),
							},
						},
					},
				}),
			},
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigParsed,
						StatusConfigMapUpdatedReady(&env),
						StatusTopicReadyWithName(ChannelTopic()),
						ChannelAddressable(&env),
						StatusProbeSucceeded,
						WithSubscribers(Subscriber1(WithUnknownSubscriber)),
						StatusChannelSubscribersUnknown(),
					),
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
			Name: "Reconciled normal - with single unready subscriber - cg unknown - no auth",
			Objects: []runtime.Object{
				NewChannel(WithSubscribers(Subscriber1(WithUnreadySubscriber))),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				NewConfigMapWithBinaryData(&env, nil),
			},
			Key: testKey,
			WantCreates: []runtime.Object{
				NewPerChannelService(&env),
				NewConsumerGroup(
					WithConsumerGroupName(Subscription1UUID),
					WithConsumerGroupNamespace(ChannelNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(NewChannel())),
					WithConsumerGroupMetaLabels(OwnerAsChannelLabel),
					WithConsumerGroupLabels(ConsumerSubscription1Label),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(ChannelTopic()),
						ConsumerConfigs(
							ConsumerGroupIdConfig(consumerGroup(NewChannel(), GetSubscriberSpec(Subscriber1(WithFreshSubscriber)))),
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
						),
						ConsumerDelivery(NewConsumerSpecDelivery(internals.Ordered)),
						ConsumerSubscriber(NewConsumerSpecSubscriber(Subscription1URI)),
					)),
				),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Ingress: &contract.Ingress{
								Host: receiver.Host(ChannelNamespace, ChannelName),
							},
						},
					},
				}),
			},
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigParsed,
						StatusConfigMapUpdatedReady(&env),
						StatusTopicReadyWithName(ChannelTopic()),
						ChannelAddressable(&env),
						StatusProbeSucceeded,
						WithSubscribers(Subscriber1(WithUnknownSubscriber)),
						StatusChannelSubscribersUnknown(),
					),
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
			Name: "Reconciled normal - with single ready subscriber - cg unknown - no auth",
			Objects: []runtime.Object{
				NewChannel(WithSubscribers(Subscriber1())),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				NewConfigMapWithBinaryData(&env, nil),
			},
			Key: testKey,
			WantCreates: []runtime.Object{
				NewPerChannelService(&env),
				NewConsumerGroup(
					WithConsumerGroupName(Subscription1UUID),
					WithConsumerGroupNamespace(ChannelNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(NewChannel())),
					WithConsumerGroupMetaLabels(OwnerAsChannelLabel),
					WithConsumerGroupLabels(ConsumerSubscription1Label),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(ChannelTopic()),
						ConsumerConfigs(
							ConsumerGroupIdConfig(consumerGroup(NewChannel(), GetSubscriberSpec(Subscriber1(WithFreshSubscriber)))),
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
						),
						ConsumerDelivery(NewConsumerSpecDelivery(internals.Ordered)),
						ConsumerSubscriber(NewConsumerSpecSubscriber(Subscription1URI)),
					)),
				),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Ingress: &contract.Ingress{
								Host: receiver.Host(ChannelNamespace, ChannelName),
							},
						},
					},
				}),
			},
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigParsed,
						StatusConfigMapUpdatedReady(&env),
						StatusTopicReadyWithName(ChannelTopic()),
						ChannelAddressable(&env),
						StatusProbeSucceeded,
						WithSubscribers(Subscriber1(WithUnknownSubscriber)),
						StatusChannelSubscribersUnknown(),
					),
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
			Name: "Reconciled normal - with single fresh subscriber - existing ready cg with update - no auth",
			Objects: []runtime.Object{
				NewChannel(WithSubscribers(Subscriber1(WithFreshSubscriber))),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				NewConfigMapWithBinaryData(&env, nil),
				NewConsumerGroup(
					WithConsumerGroupName(Subscription1UUID),
					WithConsumerGroupNamespace(ChannelNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(NewChannel())),
					WithConsumerGroupMetaLabels(OwnerAsChannelLabel),
					ConsumerGroupReady,
				),
			},
			Key: testKey,
			WantCreates: []runtime.Object{
				NewPerChannelService(&env),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Ingress: &contract.Ingress{
								Host: receiver.Host(ChannelNamespace, ChannelName),
							},
						},
					},
				}),
				{
					Object: NewConsumerGroup(
						WithConsumerGroupName(Subscription1UUID),
						WithConsumerGroupNamespace(ChannelNamespace),
						WithConsumerGroupOwnerRef(kmeta.NewControllerRef(NewChannel())),
						WithConsumerGroupMetaLabels(OwnerAsChannelLabel),
						WithConsumerGroupLabels(ConsumerSubscription1Label),
						ConsumerGroupConsumerSpec(NewConsumerSpec(
							ConsumerTopics(ChannelTopic()),
							ConsumerConfigs(
								ConsumerGroupIdConfig(consumerGroup(NewChannel(), GetSubscriberSpec(Subscriber1(WithFreshSubscriber)))),
								ConsumerBootstrapServersConfig(ChannelBootstrapServers),
							),
							ConsumerDelivery(NewConsumerSpecDelivery(internals.Ordered)),
							ConsumerSubscriber(NewConsumerSpecSubscriber(Subscription1URI)),
						)),
						ConsumerGroupReady,
					),
				},
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigParsed,
						StatusConfigMapUpdatedReady(&env),
						StatusTopicReadyWithName(ChannelTopic()),
						ChannelAddressable(&env),
						WithSubscribers(Subscriber1(WithFreshSubscriber)),
						StatusChannelSubscribers(),
						StatusProbeSucceeded,
					),
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
			Name: "Reconciled normal - with single fresh subscriber - existing cg but failed - no auth",
			Objects: []runtime.Object{
				NewChannel(WithSubscribers(Subscriber1(WithFreshSubscriber))),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				NewConfigMapWithBinaryData(&env, nil),
				NewConsumerGroup(
					WithConsumerGroupName(Subscription1UUID),
					WithConsumerGroupNamespace(ChannelNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(NewChannel())),
					WithConsumerGroupMetaLabels(OwnerAsChannelLabel),
					WithConsumerGroupLabels(ConsumerSubscription1Label),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(ChannelTopic()),
						ConsumerConfigs(
							ConsumerGroupIdConfig(consumerGroup(NewChannel(), GetSubscriberSpec(Subscriber1(WithFreshSubscriber)))),
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
						),
						ConsumerDelivery(NewConsumerSpecDelivery(internals.Ordered)),
						ConsumerSubscriber(NewConsumerSpecSubscriber(Subscription1URI)),
					)),
					WithConsumerGroupFailed("failed to reconcile consumer group,", "internal error"),
				),
			},
			Key: testKey,
			WantCreates: []runtime.Object{
				NewPerChannelService(&env),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigParsed,
						StatusConfigMapUpdatedReady(&env),
						StatusTopicReadyWithName(ChannelTopic()),
						ChannelAddressable(&env),
						StatusProbeSucceeded,
						WithSubscribers(Subscriber1(WithUnreadySubscriber)),
						StatusChannelSubscribersUnknown(),
					),
				},
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Ingress: &contract.Ingress{
								Host: receiver.Host(ChannelNamespace, ChannelName),
							},
						},
					},
				}),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
		},
		{
			Name: "Reconciled normal - with two fresh subscribers - no auth",
			Objects: []runtime.Object{
				NewChannel(
					WithSubscribers(Subscriber1(WithFreshSubscriber)),
					WithSubscribers(Subscriber2(WithFreshSubscriber)),
				),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				NewConfigMapWithBinaryData(&env, nil),
			},
			Key: testKey,
			WantCreates: []runtime.Object{
				NewPerChannelService(&env),
				NewConsumerGroup(
					WithConsumerGroupName(Subscription1UUID),
					WithConsumerGroupNamespace(ChannelNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(NewChannel())),
					WithConsumerGroupMetaLabels(OwnerAsChannelLabel),
					WithConsumerGroupLabels(ConsumerSubscription1Label),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(ChannelTopic()),
						ConsumerConfigs(
							ConsumerGroupIdConfig(consumerGroup(NewChannel(), GetSubscriberSpec(Subscriber1(WithFreshSubscriber)))),
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
						),
						ConsumerDelivery(NewConsumerSpecDelivery(internals.Ordered)),
						ConsumerSubscriber(NewConsumerSpecSubscriber(Subscription1URI)),
					)),
				),
				NewConsumerGroup(
					WithConsumerGroupName(Subscription2UUID),
					WithConsumerGroupNamespace(ChannelNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(NewChannel())),
					WithConsumerGroupMetaLabels(OwnerAsChannelLabel),
					WithConsumerGroupLabels(ConsumerSubscription2Label),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(ChannelTopic()),
						ConsumerConfigs(
							ConsumerGroupIdConfig(consumerGroup(NewChannel(), GetSubscriberSpec(Subscriber2(WithFreshSubscriber)))),
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
						),
						ConsumerDelivery(NewConsumerSpecDelivery(internals.Ordered)),
						ConsumerSubscriber(NewConsumerSpecSubscriber(Subscription2URI)),
					)),
				),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Ingress: &contract.Ingress{
								Host: receiver.Host(ChannelNamespace, ChannelName),
							},
						},
					},
				}),
			},
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigParsed,
						StatusConfigMapUpdatedReady(&env),
						StatusTopicReadyWithName(ChannelTopic()),
						ChannelAddressable(&env),
						StatusProbeSucceeded,
						WithSubscribers(Subscriber1(WithUnknownSubscriber), Subscriber2(WithUnknownSubscriber)),
						StatusChannelSubscribersUnknown(),
					),
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
			Name: "Reconciled normal - with one fresh subscriber and one removed subscriber (cg removal needed)",
			Objects: []runtime.Object{
				NewChannel(
					WithSubscribers(Subscriber1(WithFreshSubscriber)),
				),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				NewConfigMapWithBinaryData(&env, nil),
				NewConsumerGroup(
					WithConsumerGroupName(Subscription2UUID), //removed subscriber
					WithConsumerGroupNamespace(ChannelNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(NewChannel())),
					WithConsumerGroupMetaLabels(OwnerAsChannelLabel),
					WithConsumerGroupLabels(ConsumerSubscription2Label),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(ChannelTopic()),
						ConsumerConfigs(
							ConsumerGroupIdConfig(consumerGroup(NewChannel(), GetSubscriberSpec(Subscriber2(WithFreshSubscriber)))),
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
						),
						ConsumerDelivery(NewConsumerSpecDelivery(internals.Ordered)),
						ConsumerSubscriber(NewConsumerSpecSubscriber(Subscription2URI)),
					)),
				),
			},
			Key: testKey,
			WantCreates: []runtime.Object{
				NewPerChannelService(&env),
				NewConsumerGroup(
					WithConsumerGroupName(Subscription1UUID),
					WithConsumerGroupNamespace(ChannelNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(NewChannel())),
					WithConsumerGroupMetaLabels(OwnerAsChannelLabel),
					WithConsumerGroupLabels(ConsumerSubscription1Label),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(ChannelTopic()),
						ConsumerConfigs(
							ConsumerGroupIdConfig(consumerGroup(NewChannel(), GetSubscriberSpec(Subscriber1(WithFreshSubscriber)))),
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
						),
						ConsumerDelivery(NewConsumerSpecDelivery(internals.Ordered)),
						ConsumerSubscriber(NewConsumerSpecSubscriber(Subscription1URI)),
					)),
				),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Ingress: &contract.Ingress{
								Host: receiver.Host(ChannelNamespace, ChannelName),
							},
						},
					},
				}),
			},
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigParsed,
						StatusConfigMapUpdatedReady(&env),
						StatusTopicReadyWithName(ChannelTopic()),
						ChannelAddressable(&env),
						StatusProbeSucceeded,
						WithSubscribers(Subscriber1(WithUnknownSubscriber)),
						StatusChannelSubscribersUnknown(),
					),
				},
			},
			WantDeletes: []clientgotesting.DeleteActionImpl{
				{
					ActionImpl: clientgotesting.ActionImpl{
						Namespace: ChannelNamespace,
						Resource: schema.GroupVersionResource{
							Group:    kafkainternals.SchemeGroupVersion.Group,
							Version:  kafkainternals.SchemeGroupVersion.Version,
							Resource: "consumergroups",
						},
					},
					Name: Subscription2UUID,
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
			Name: "channel configmap not resolved",
			Objects: []runtime.Object{
				NewChannel(),
			},
			Key:                     testKey,
			WantErr:                 true,
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigNotParsed(fmt.Sprintf(`failed to get configmap %s/%s: configmap %q not found`, system.Namespace(), DefaultEnv.GeneralConfigMapName, DefaultEnv.GeneralConfigMapName)),
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
				Eventf(
					corev1.EventTypeWarning,
					"InternalError",
					fmt.Sprintf(`failed to get contract configuration: failed to get configmap %s/%s: configmap %q not found`, system.Namespace(), DefaultEnv.GeneralConfigMapName, DefaultEnv.GeneralConfigMapName),
				),
			},
		},
		{
			Name: "channel configmap does not have bootstrap servers",
			Objects: []runtime.Object{
				NewChannel(),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					"foo": "bar",
				}),
			},
			Key:     testKey,
			WantErr: true,
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigNotParsed("unable to get bootstrapServers from configmap: invalid configuration bootstrapServers: [] - ConfigMap data: map[foo:bar]"),
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
				Eventf(
					corev1.EventTypeWarning,
					"InternalError",
					"failed to get contract configuration: unable to get bootstrapServers from configmap: invalid configuration bootstrapServers: [] - ConfigMap data: map[foo:bar]",
				),
			},
		},
		{
			Name: "channel configmap has blank bootstrap servers",
			Objects: []runtime.Object{
				NewChannel(),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: "",
				}),
			},
			Key:                     testKey,
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantErr:                 true,
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigNotParsed("unable to get bootstrapServers from configmap: invalid configuration bootstrapServers: [] - ConfigMap data: map[bootstrap.servers:]"),
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
				Eventf(
					corev1.EventTypeWarning,
					"InternalError",
					"failed to get contract configuration: unable to get bootstrapServers from configmap: invalid configuration bootstrapServers: [] - ConfigMap data: map[bootstrap.servers:]",
				),
			},
		},
		{
			Name: "Channel spec is used properly",
			Objects: []runtime.Object{
				NewChannel(
					WithNumPartitions(3),
					WithReplicationFactor(4),
					WithRetentionDuration("1000"),
				),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
			},
			OtherTestData: map[string]interface{}{
				TestExpectedDataNumPartitions: int32(3),
				TestExpectedReplicationFactor: int16(4),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Ingress: &contract.Ingress{
								Host: receiver.Host(ChannelNamespace, ChannelName),
							},
						},
					},
				}),
			},
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantCreates: []runtime.Object{
				NewConfigMapWithBinaryData(&env, nil),
				NewPerChannelService(&env),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithNumPartitions(3),
						WithReplicationFactor(4),
						WithRetentionDuration("1000"),
						WithInitKafkaChannelConditions,
						StatusConfigParsed,
						StatusConfigMapUpdatedReady(&env),
						StatusTopicReadyWithName(ChannelTopic()),
						ChannelAddressable(&env),
						StatusProbeSucceeded,
						StatusChannelSubscribers(),
					),
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
			Name: "Reconciled normal - with single fresh subscriber - with auth - PlainText",
			Objects: []runtime.Object{
				NewChannel(WithSubscribers(Subscriber1(WithFreshSubscriber))),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
					security.AuthSecretNameKey:         "secret-1",
					security.AuthSecretNamespaceKey:    "ns-1",
				}),
				NewConfigMapWithBinaryData(&env, nil),
				NewSSLSecret("ns-1", "secret-1"),
				NewConsumerGroup(
					WithConsumerGroupName(Subscription1UUID),
					WithConsumerGroupNamespace(ChannelNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(NewChannel())),
					WithConsumerGroupMetaLabels(OwnerAsChannelLabel),
					WithConsumerGroupLabels(ConsumerSubscription1Label),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(ChannelTopic()),
						ConsumerConfigs(
							ConsumerGroupIdConfig(consumerGroup(NewChannel(), GetSubscriberSpec(Subscriber1(WithFreshSubscriber)))),
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
						),
						ConsumerDelivery(NewConsumerSpecDelivery(internals.Ordered)),
						ConsumerSubscriber(NewConsumerSpecSubscriber(Subscription1URI)),
					)),
					ConsumerGroupReady,
				),
			},
			Key: testKey,
			WantCreates: []runtime.Object{
				NewPerChannelService(&env),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Auth: &contract.Resource_AuthSecret{
								AuthSecret: &contract.Reference{
									Uuid:      SecretUUID,
									Namespace: "ns-1",
									Name:      "secret-1",
									Version:   SecretResourceVersion,
								},
							},
							Ingress: &contract.Ingress{
								Host: receiver.Host(ChannelNamespace, ChannelName),
							},
						},
					},
				}),
			},
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigParsed,
						StatusConfigMapUpdatedReady(&env),
						StatusTopicReadyWithName(ChannelTopic()),
						ChannelAddressable(&env),
						WithSubscribers(Subscriber1(WithFreshSubscriber)),
						StatusProbeSucceeded,
						StatusChannelSubscribers(),
					),
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
			Name: "Create contract configmap when it does not exist",
			Objects: []runtime.Object{
				NewChannel(),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Ingress: &contract.Ingress{
								Host: receiver.Host(ChannelNamespace, ChannelName),
							},
						},
					},
				}),
			},
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantCreates: []runtime.Object{
				NewConfigMapWithBinaryData(&env, nil),
				NewPerChannelService(&env),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigParsed,
						StatusConfigMapUpdatedReady(&env),
						StatusTopicReadyWithName(ChannelTopic()),
						ChannelAddressable(&env),
						StatusProbeSucceeded,
						StatusChannelSubscribers(),
					),
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
			Name: "Do not create contract configmap when it exists",
			Objects: []runtime.Object{
				NewChannel(),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				NewConfigMapWithBinaryData(&env, nil),
			},
			Key: testKey,
			WantCreates: []runtime.Object{
				NewPerChannelService(&env),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Ingress: &contract.Ingress{
								Host: receiver.Host(ChannelNamespace, ChannelName),
							},
						},
					},
				}),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigParsed,
						StatusConfigMapUpdatedReady(&env),
						StatusTopicReadyWithName(ChannelTopic()),
						ChannelAddressable(&env),
						StatusProbeSucceeded,
						StatusChannelSubscribers(),
					),
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
			Name: "Corrupt contract in configmap",
			Objects: []runtime.Object{
				NewChannel(),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				NewConfigMapWithBinaryData(&env, []byte("corrupt")),
			},
			Key:                     testKey,
			WantErr:                 true,
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigParsed,
						StatusTopicReadyWithName(ChannelTopic()),
						StatusConfigMapNotUpdatedReady(
							"Failed to get contract data from ConfigMap: knative-eventing/kafka-channel-channels-subscriptions",
							"failed to unmarshal contract: 'corrupt'",
						),
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
				Eventf(
					corev1.EventTypeWarning,
					"InternalError",
					"failed to get broker and triggers data from config map knative-eventing/kafka-channel-channels-subscriptions: failed to unmarshal contract: 'corrupt'",
				),
			},
		},
	}

	table.Test(t, NewFactory(&env, func(ctx context.Context, listers *Listers, env *config.Env, row *TableRow) controller.Reconciler {

		proberMock := probertesting.MockProber(prober.StatusReady)
		if p, ok := row.OtherTestData[testProber]; ok {
			proberMock = p.(prober.Prober)
		}

		numPartitions := int32(1)
		if v, ok := row.OtherTestData[TestExpectedDataNumPartitions]; ok {
			numPartitions = v.(int32)
		}

		replicationFactor := int16(1)
		if v, ok := row.OtherTestData[TestExpectedReplicationFactor]; ok {
			replicationFactor = v.(int16)
		}

		reconciler := &Reconciler{
			Reconciler: &base.Reconciler{
				KubeClient:                  kubeclient.Get(ctx),
				PodLister:                   listers.GetPodLister(),
				SecretLister:                listers.GetSecretLister(),
				DataPlaneConfigMapNamespace: env.DataPlaneConfigMapNamespace,
				DataPlaneConfigMapName:      env.DataPlaneConfigMapName,
				DataPlaneConfigFormat:       env.DataPlaneConfigFormat,
				DataPlaneNamespace:          env.SystemNamespace,
				DispatcherLabel:             base.ChannelDispatcherLabel,
				ReceiverLabel:               base.ChannelReceiverLabel,
			},
			Env: env,
			NewKafkaClient: func(addrs []string, config *sarama.Config) (sarama.Client, error) {
				return &kafkatesting.MockKafkaClient{}, nil
			},
			NewKafkaClusterAdminClient: func(_ []string, _ *sarama.Config) (sarama.ClusterAdmin, error) {
				return &kafkatesting.MockKafkaClusterAdmin{
					ExpectedTopicName: ChannelTopic(),
					ExpectedTopicDetail: sarama.TopicDetail{
						NumPartitions:     numPartitions,
						ReplicationFactor: replicationFactor,
					},
					T: t,
				}, nil
			},
			ConfigMapLister:     listers.GetConfigMapLister(),
			ServiceLister:       listers.GetServiceLister(),
			ConsumerGroupLister: listers.GetConsumerGroupLister(),
			InternalsClient:     fakeconsumergroupinformer.Get(ctx),
			Prober:              proberMock,
			IngressHost:         network.GetServiceHostname(env.IngressName, env.SystemNamespace),
		}
		reconciler.ConfigMapTracker = &FakeTracker{}
		reconciler.SecretTracker = &FakeTracker{}

		r := messagingv1beta1kafkachannelreconciler.NewReconciler(
			ctx,
			logging.FromContext(ctx),
			fakeeventingkafkaclient.Get(ctx),
			listers.GetKafkaChannelLister(),
			controller.GetEventRecorder(ctx),
			reconciler,
		)
		return r
	}))
}

func StatusChannelSubscribers() KRShapedOption {
	return func(obj duckv1.KRShaped) {
		ch := obj.(*messagingv1beta.KafkaChannel)
		ch.GetConditionSet().Manage(ch.GetStatus()).MarkTrue(KafkaChannelConditionSubscribersReady)
	}
}

func StatusChannelSubscribersUnknown() KRShapedOption {
	return func(obj duckv1.KRShaped) {
		ch := obj.(*messagingv1beta.KafkaChannel)
		ch.GetConditionSet().Manage(ch.GetStatus()).MarkUnknown(KafkaChannelConditionSubscribersReady, "all subscribers not ready", "failed to reconcile consumer group")
	}
}

func StatusChannelSubscribersFailed(reason string, msg string) KRShapedOption {
	return func(obj duckv1.KRShaped) {
		ch := obj.(*messagingv1beta.KafkaChannel)
		ch.GetConditionSet().Manage(ch.GetStatus()).MarkFalse(KafkaChannelConditionSubscribersReady, reason, msg)
	}
}

func patchFinalizers() clientgotesting.PatchActionImpl {
	action := clientgotesting.PatchActionImpl{}
	action.Name = ChannelName
	action.Namespace = ChannelNamespace
	patch := `{"metadata":{"finalizers":["` + finalizerName + `"],"resourceVersion":""}}`
	action.Patch = []byte(patch)
	return action
}
