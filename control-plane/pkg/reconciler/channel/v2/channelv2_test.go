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
	"strconv"
	"testing"
	"text/template"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/IBM/sarama"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgotesting "k8s.io/client-go/testing"
	"k8s.io/utils/pointer"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/eventing/pkg/apis/feature"
	"knative.dev/eventing/pkg/eventingtls/eventingtlstesting"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	kubeclient "knative.dev/pkg/client/injection/kube/client/fake"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/kmeta"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/network"
	. "knative.dev/pkg/reconciler/testing"
	"knative.dev/pkg/system"

	apisconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/config"
	kafkasource "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/sources/v1beta1"
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

	messagingv1beta "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/messaging/v1beta1"
	fakeeventingkafkaclient "knative.dev/eventing-kafka-broker/control-plane/pkg/client/injection/client/fake"
	messagingv1beta1kafkachannelreconciler "knative.dev/eventing-kafka-broker/control-plane/pkg/client/injection/reconciler/messaging/v1beta1/kafkachannel"

	"github.com/rickb777/date/period"
	internalscg "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing/v1alpha1"
	kafkainternals "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing/v1alpha1"
	fakeconsumergroupinformer "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/client/fake"
	eventingrekttesting "knative.dev/eventing/pkg/reconciler/testing/v1"
)

const (
	testProber = "testProber"

	finalizerName                 = "kafkachannels.messaging.knative.dev"
	TestExpectedDataNumPartitions = "TestExpectedDataNumPartitions"
	TestExpectedReplicationFactor = "TestExpectedReplicationFactor"
	TestExpectedRetentionDuration = "TestExpectedRetentionDuration"

	kafkaFeatureFlags = "kafka-feature-flags"
)

var finalizerUpdatedEvent = Eventf(
	corev1.EventTypeNormal,
	"FinalizerUpdate",
	fmt.Sprintf(`Updated %q finalizers`, ChannelName),
)

var customChannelTopicTemplate = customTemplate()

var DefaultEnv = &config.Env{
	DataPlaneConfigMapNamespace: "knative-eventing",
	ContractConfigMapName:       "kafka-channel-channels-subscriptions",
	GeneralConfigMapName:        "kafka-channel-config",
	IngressName:                 "kafka-channel-ingress",
	SystemNamespace:             "knative-eventing",
	ContractConfigMapFormat:     base.Json,
}

var (
	testCaCerts = string(eventingtlstesting.CA)
)

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
				NewConfigMapWithTextData(env.SystemNamespace, DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
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
				NewConfigMapWithTextData(env.SystemNamespace, DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
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
				NewConfigMapWithTextData(env.SystemNamespace, DefaultEnv.GeneralConfigMapName, map[string]string{
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
				}, env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
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
				NewConfigMapWithTextData(env.SystemNamespace, DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				ChannelReceiverPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
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
			},
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantCreates: []runtime.Object{
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
				NewPerChannelService(&env),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigParsed,
						StatusConfigMapUpdatedReady(&env),
						WithChannelTopicStatusAnnotation(ChannelTopic()),
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
				NewConfigMapWithTextData(env.SystemNamespace, DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				ChannelReceiverPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
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
			},
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantCreates: []runtime.Object{
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
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
						WithChannelTopicStatusAnnotation(ChannelTopic()),
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
				NewConfigMapWithTextData(env.SystemNamespace, DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
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
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
				NewPerChannelService(&env),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigParsed,
						StatusConfigMapUpdatedReady(&env),
						WithChannelTopicStatusAnnotation(ChannelTopic()),
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
				NewConfigMapWithTextData(env.SystemNamespace, DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
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
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
				NewPerChannelService(&env),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigParsed,
						StatusConfigMapUpdatedReady(&env),
						WithChannelTopicStatusAnnotation(ChannelTopic()),
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
				NewChannel(
					WithSubscribers(Subscriber1(WithFreshSubscriber))),
				NewConfigMapWithTextData(env.SystemNamespace, DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
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
						ConsumerDelivery(NewConsumerSpecDelivery(kafkasource.Ordered)),
						ConsumerSubscriber(NewConsumerSpecSubscriber(Subscription1URI)),
					)),
				),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
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
						WithChannelTopicStatusAnnotation(ChannelTopic()),
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
				NewChannel(
					WithSubscribers(Subscriber1(WithUnreadySubscriber))),
				NewConfigMapWithTextData(env.SystemNamespace, DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
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
						ConsumerDelivery(NewConsumerSpecDelivery(kafkasource.Ordered)),
						ConsumerSubscriber(NewConsumerSpecSubscriber(Subscription1URI)),
					)),
				),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
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
						WithChannelTopicStatusAnnotation(ChannelTopic()),
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
				NewChannel(
					WithSubscribers(Subscriber1())),
				NewConfigMapWithTextData(env.SystemNamespace, DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
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
						ConsumerDelivery(NewConsumerSpecDelivery(kafkasource.Ordered)),
						ConsumerSubscriber(NewConsumerSpecSubscriber(Subscription1URI)),
					)),
				),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
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
						WithChannelTopicStatusAnnotation(ChannelTopic()),
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
				NewChannel(
					WithSubscribers(Subscriber1(WithFreshSubscriber))),
				NewConfigMapWithTextData(env.SystemNamespace, DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
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
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
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
							ConsumerDelivery(NewConsumerSpecDelivery(kafkasource.Ordered)),
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
						WithChannelTopicStatusAnnotation(ChannelTopic()),
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
				NewChannel(
					WithSubscribers(Subscriber1(WithFreshSubscriber))),
				NewConfigMapWithTextData(env.SystemNamespace, DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
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
						ConsumerDelivery(NewConsumerSpecDelivery(kafkasource.Ordered)),
						ConsumerSubscriber(NewConsumerSpecSubscriber(Subscription1URI)),
					)),
					ConsumerGroupReplicas(1),
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
						WithChannelTopicStatusAnnotation(ChannelTopic()),
						StatusTopicReadyWithName(ChannelTopic()),
						ChannelAddressable(&env),
						StatusProbeSucceeded,
						WithSubscribers(Subscriber1(WithUnreadySubscriber)),
						StatusChannelSubscribersUnknown(),
					),
				},
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
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
				NewConfigMapWithTextData(env.SystemNamespace, DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
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
						ConsumerDelivery(NewConsumerSpecDelivery(kafkasource.Ordered)),
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
						ConsumerDelivery(NewConsumerSpecDelivery(kafkasource.Ordered)),
						ConsumerSubscriber(NewConsumerSpecSubscriber(Subscription2URI)),
					)),
				),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
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
						WithChannelTopicStatusAnnotation(ChannelTopic()),
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
				NewConfigMapWithTextData(env.SystemNamespace, DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
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
						ConsumerDelivery(NewConsumerSpecDelivery(kafkasource.Ordered)),
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
						ConsumerDelivery(NewConsumerSpecDelivery(kafkasource.Ordered)),
						ConsumerSubscriber(NewConsumerSpecSubscriber(Subscription1URI)),
					)),
				),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
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
						WithChannelTopicStatusAnnotation(ChannelTopic()),
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
						StatusConfigNotParsed(fmt.Sprintf(`failed to get configmap %s/%s: configmap %q not found`, env.SystemNamespace, DefaultEnv.GeneralConfigMapName, DefaultEnv.GeneralConfigMapName)),
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
					fmt.Sprintf(`failed to get contract configuration: failed to get configmap %s/%s: configmap %q not found`, env.SystemNamespace, DefaultEnv.GeneralConfigMapName, DefaultEnv.GeneralConfigMapName),
				),
			},
		},
		{
			Name: "channel configmap does not have bootstrap servers",
			Objects: []runtime.Object{
				NewChannel(),
				NewConfigMapWithTextData(env.SystemNamespace, DefaultEnv.GeneralConfigMapName, map[string]string{
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
				NewConfigMapWithTextData(env.SystemNamespace, DefaultEnv.GeneralConfigMapName, map[string]string{
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
					WithRetentionDuration("PT10M"),
				),
				NewConfigMapWithTextData(env.SystemNamespace, DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
			},
			OtherTestData: map[string]interface{}{
				TestExpectedDataNumPartitions: int32(3),
				TestExpectedReplicationFactor: int16(4),
				TestExpectedRetentionDuration: "PT10M",
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
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
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
				NewPerChannelService(&env),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithNumPartitions(3),
						WithReplicationFactor(4),
						WithRetentionDuration("PT10M"),
						WithInitKafkaChannelConditions,
						StatusConfigParsed,
						StatusConfigMapUpdatedReady(&env),
						WithChannelTopicStatusAnnotation(ChannelTopic()),
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
			Name: "Reconciled normal - with single fresh subscriber - with auth - SSL",
			Objects: []runtime.Object{
				NewChannel(
					WithSubscribers(Subscriber1(WithFreshSubscriber))),
				NewConfigMapWithTextData(env.SystemNamespace, DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
					security.AuthSecretNameKey:         "secret-1",
					security.AuthSecretNamespaceKey:    "ns-1",
				}),
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
				NewLegacySSLSecret("ns-1", "secret-1"),
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
						ConsumerDelivery(NewConsumerSpecDelivery(kafkasource.Ordered)),
						ConsumerSubscriber(NewConsumerSpecSubscriber(Subscription1URI)),
						ConsumerAuth(&internalscg.Auth{
							SecretSpec: &internalscg.SecretSpec{
								Ref: &internalscg.SecretReference{
									Name:      "secret-1",
									Namespace: "ns-1",
								},
							},
						}),
					)),
					ConsumerGroupReplicas(1),
					ConsumerGroupReady,
				),
			},
			Key: testKey,
			WantCreates: []runtime.Object{
				NewPerChannelService(&env),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Auth: &contract.Resource_MultiAuthSecret{
								MultiAuthSecret: &contract.MultiSecretReference{
									Protocol: contract.Protocol_SSL,
									References: []*contract.SecretReference{{
										Reference: &contract.Reference{
											Uuid:      SecretUUID,
											Namespace: "ns-1",
											Name:      "secret-1",
											Version:   SecretResourceVersion,
										},
										KeyFieldReferences: []*contract.KeyFieldReference{
											{SecretKey: "user.key", Field: contract.SecretField_USER_KEY},
											{SecretKey: "user.crt", Field: contract.SecretField_USER_CRT},
											{SecretKey: "ca.crt", Field: contract.SecretField_CA_CRT},
										},
									}},
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
						WithChannelTopicStatusAnnotation(ChannelTopic()),
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
			Name: "Reconciled normal - with single fresh subscriber - with auth - SASL SSL",
			Objects: []runtime.Object{
				NewChannel(
					WithSubscribers(Subscriber1(WithFreshSubscriber))),
				//NewService(),
				NewPerChannelService(DefaultEnv),
				NewConfigMapWithTextData(env.SystemNamespace, DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
					security.AuthSecretNameKey:         "secret-1",
					security.AuthSecretNamespaceKey:    "ns-1",
				}),
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
				NewLegacySASLSSLSecret("ns-1", "secret-1"),
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
						ConsumerDelivery(NewConsumerSpecDelivery(kafkasource.Ordered)),
						ConsumerSubscriber(NewConsumerSpecSubscriber(Subscription1URI)),
						ConsumerAuth(&internalscg.Auth{
							SecretSpec: &internalscg.SecretSpec{
								Ref: &internalscg.SecretReference{
									Name:      "secret-1",
									Namespace: "ns-1",
								},
							},
						}),
					)),
					ConsumerGroupReplicas(1),
					ConsumerGroupReady,
				),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Auth: &contract.Resource_MultiAuthSecret{
								MultiAuthSecret: &contract.MultiSecretReference{
									Protocol: contract.Protocol_SASL_SSL,
									References: []*contract.SecretReference{{
										Reference: &contract.Reference{
											Uuid:      SecretUUID,
											Namespace: "ns-1",
											Name:      "secret-1",
											Version:   SecretResourceVersion,
										},
										KeyFieldReferences: []*contract.KeyFieldReference{
											{SecretKey: "user.key", Field: contract.SecretField_USER_KEY},
											{SecretKey: "user.crt", Field: contract.SecretField_USER_CRT},
											{SecretKey: "ca.crt", Field: contract.SecretField_CA_CRT},
											{SecretKey: "password", Field: contract.SecretField_PASSWORD},
											{SecretKey: "username", Field: contract.SecretField_USER},
											{SecretKey: "saslType", Field: contract.SecretField_SASL_MECHANISM},
										},
									}},
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
						WithChannelTopicStatusAnnotation(ChannelTopic()),
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
			Name: "Reconciled normal - with single fresh subscriber - with auth - SASL",
			Objects: []runtime.Object{
				NewChannel(
					WithSubscribers(Subscriber1(WithFreshSubscriber))),
				NewPerChannelService(DefaultEnv),
				NewConfigMapWithTextData(env.SystemNamespace, DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
					security.AuthSecretNameKey:         "secret-1",
					security.AuthSecretNamespaceKey:    "ns-1",
				}),
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
				NewLegacySASLSecret("ns-1", "secret-1"),
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
						ConsumerDelivery(NewConsumerSpecDelivery(kafkasource.Ordered)),
						ConsumerSubscriber(NewConsumerSpecSubscriber(Subscription1URI)),
						ConsumerAuth(&internalscg.Auth{
							SecretSpec: &internalscg.SecretSpec{
								Ref: &internalscg.SecretReference{
									Name:      "secret-1",
									Namespace: "ns-1",
								},
							},
						}),
					)),
					ConsumerGroupReplicas(1),
					ConsumerGroupReady,
				),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Auth: &contract.Resource_MultiAuthSecret{
								MultiAuthSecret: &contract.MultiSecretReference{
									Protocol: contract.Protocol_SASL_PLAINTEXT,
									References: []*contract.SecretReference{{
										Reference: &contract.Reference{
											Uuid:      SecretUUID,
											Namespace: "ns-1",
											Name:      "secret-1",
											Version:   SecretResourceVersion,
										},
										KeyFieldReferences: []*contract.KeyFieldReference{
											{SecretKey: "password", Field: contract.SecretField_PASSWORD},
											{SecretKey: "username", Field: contract.SecretField_USER},
											{SecretKey: "saslType", Field: contract.SecretField_SASL_MECHANISM},
										},
									}},
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
						WithChannelTopicStatusAnnotation(ChannelTopic()),
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
			Name: "Reconciled normal - with single fresh subscriber - with autoscaling annotations",
			Objects: []runtime.Object{
				NewChannel(

					WithSubscribers(Subscriber1(WithFreshSubscriber)),
				),
				NewConfigMapWithTextData(env.SystemNamespace, DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
				eventingrekttesting.NewSubscription(Subscription1Name, ChannelNamespace,
					eventingrekttesting.WithSubscriptionUID(Subscription1UUID),
					WithAutoscalingAnnotationsSubscription(),
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
					WithConsumerGroupAnnotations(ConsumerGroupAnnotations),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(ChannelTopic()),
						ConsumerConfigs(
							ConsumerGroupIdConfig(consumerGroup(NewChannel(), GetSubscriberSpec(Subscriber1(WithFreshSubscriber)))),
							ConsumerBootstrapServersConfig(ChannelBootstrapServers),
						),
						ConsumerDelivery(NewConsumerSpecDelivery(kafkasource.Ordered)),
						ConsumerSubscriber(NewConsumerSpecSubscriber(Subscription1URI)),
					)),
				),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
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
						WithChannelTopicStatusAnnotation(ChannelTopic()),
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
			Name: "Create contract configmap when it does not exist",
			Objects: []runtime.Object{
				NewChannel(),
				NewConfigMapWithTextData(env.SystemNamespace, DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
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
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
				NewPerChannelService(&env),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigParsed,
						StatusConfigMapUpdatedReady(&env),
						WithChannelTopicStatusAnnotation(ChannelTopic()),
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
				NewConfigMapWithTextData(env.SystemNamespace, DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
			},
			Key: testKey,
			WantCreates: []runtime.Object{
				NewPerChannelService(&env),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
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
						WithChannelTopicStatusAnnotation(ChannelTopic()),
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
				NewConfigMapWithTextData(env.SystemNamespace, DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, []byte("corrupt")),
			},
			Key:                     testKey,
			WantErr:                 true,
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigParsed,
						WithChannelTopicStatusAnnotation(ChannelTopic()),
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
		{
			Name: "Reconciled normal - with custom template",
			Objects: []runtime.Object{
				NewChannel(
					WithChannelDelivery(&eventingduck.DeliverySpec{
						DeadLetterSink: ServiceDestination,
						Retry:          pointer.Int32(5),
					}),
				),
				NewConfigMapWithTextData(env.SystemNamespace, DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				ChannelReceiverPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{CustomTopic(customChannelTopicTemplate)},
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
			},
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantCreates: []runtime.Object{
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
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
						WithChannelTopicStatusAnnotation(CustomTopic(customChannelTopicTemplate)),
						StatusTopicReadyWithName(CustomTopic(customChannelTopicTemplate)),
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
			OtherTestData: map[string]interface{}{
				kafkaFeatureFlags: newKafkaFeaturesConfigFromMap(&corev1.ConfigMap{
					Data: map[string]string{
						"channels.topic.template": "custom-channel-template.{{ .Namespace}}.{{ .Name }}",
					},
				}),
			},
		},
		{
			Name: "Reconciled normal - TLS Permissive",
			Objects: []runtime.Object{
				NewChannel(
					WithChannelDelivery(&eventingduck.DeliverySpec{
						DeadLetterSink: ServiceDestination,
						Retry:          pointer.Int32(5),
					}),
				),
				NewConfigMapWithTextData(env.SystemNamespace, DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				ChannelReceiverPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				makeTLSSecret(),
			},
			Key: testKey,
			Ctx: feature.ToContext(context.Background(), feature.Flags{
				feature.TransportEncryption: feature.Permissive,
			}),
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
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
			},
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantCreates: []runtime.Object{
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
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
						WithChannelTopicStatusAnnotation(ChannelTopic()),
						StatusTopicReadyWithName(ChannelTopic()),
						ChannelAddressable(&env),
						StatusProbeSucceeded,
						StatusChannelSubscribers(),
						WithChannelDeadLetterSinkURI(ServiceURL),
						WithChannelAddresses([]duckv1.Addressable{
							{
								Name:    pointer.String("https"),
								URL:     httpsURL(ChannelServiceName, ChannelNamespace),
								CACerts: pointer.String(testCaCerts),
							},
							{
								Name: pointer.String("http"),
								URL:  ChannelAddress(),
							},
						}),
						WithChannelAddress(duckv1.Addressable{
							Name: pointer.String("http"),
							URL:  ChannelAddress(),
						}),
						WithChannelAddessable(),
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
			Name: "Reconciled normal - TLS Strict",
			Objects: []runtime.Object{
				NewChannel(
					WithChannelDelivery(&eventingduck.DeliverySpec{
						DeadLetterSink: ServiceDestination,
						Retry:          pointer.Int32(5),
					}),
				),
				NewConfigMapWithTextData(env.SystemNamespace, DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				ChannelReceiverPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				makeTLSSecret(),
			},
			Key: testKey,
			Ctx: feature.ToContext(context.Background(), feature.Flags{
				feature.TransportEncryption: feature.Strict,
			}),
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
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
			},
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantCreates: []runtime.Object{
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
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
						WithChannelTopicStatusAnnotation(ChannelTopic()),
						StatusTopicReadyWithName(ChannelTopic()),
						ChannelAddressable(&env),
						StatusProbeSucceeded,
						StatusChannelSubscribers(),
						WithChannelDeadLetterSinkURI(ServiceURL),
						WithChannelAddresses([]duckv1.Addressable{
							{
								Name:    pointer.String("https"),
								URL:     httpsURL(ChannelServiceName, ChannelNamespace),
								CACerts: pointer.String(testCaCerts),
							},
						}),
						WithChannelAddress(duckv1.Addressable{
							Name:    pointer.String("https"),
							URL:     httpsURL(ChannelServiceName, ChannelNamespace),
							CACerts: pointer.String(testCaCerts),
						}),
						WithChannelAddessable(),
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
	}

	table.Test(t, NewFactory(&env, func(ctx context.Context, listers *Listers, env *config.Env, row *TableRow) controller.Reconciler {
		proberMock := probertesting.MockProber(prober.StatusReady)
		if p, ok := row.OtherTestData[testProber]; ok {
			proberMock = p.(prober.Prober)
		}

		var featureFlags *apisconfig.KafkaFeatureFlags
		if v, ok := row.OtherTestData[kafkaFeatureFlags]; ok {
			featureFlags = v.(*apisconfig.KafkaFeatureFlags)
		} else {
			featureFlags = apisconfig.DefaultFeaturesConfig()
		}

		numPartitions := int32(1)
		if v, ok := row.OtherTestData[TestExpectedDataNumPartitions]; ok {
			numPartitions = v.(int32)
		}

		replicationFactor := int16(1)
		if v, ok := row.OtherTestData[TestExpectedReplicationFactor]; ok {
			replicationFactor = v.(int16)
		}

		retentionDuration := messagingv1beta.DefaultRetentionDuration
		if v, ok := row.OtherTestData[TestExpectedRetentionDuration]; ok {
			retentionPeriod, err := period.Parse(v.(string))
			if err != nil {
				t.Errorf("couldn't parse retention duration: %s", err.Error())
			}
			retentionDuration, _ = retentionPeriod.Duration()
		}

		retentionMillisString := strconv.FormatInt(retentionDuration.Milliseconds(), 10)

		expectedTopicName, err := featureFlags.ExecuteChannelsTopicTemplate(metav1.ObjectMeta{Name: ChannelName, Namespace: ChannelNamespace, UID: ChannelUUID})
		if err != nil {
			panic("failed to create expected topic name")
		}

		reconciler := &Reconciler{
			Reconciler: &base.Reconciler{
				KubeClient:                  kubeclient.Get(ctx),
				PodLister:                   listers.GetPodLister(),
				SecretLister:                listers.GetSecretLister(),
				DataPlaneConfigMapNamespace: env.DataPlaneConfigMapNamespace,
				ContractConfigMapName:       env.ContractConfigMapName,
				ContractConfigMapFormat:     env.ContractConfigMapFormat,
				DataPlaneNamespace:          env.SystemNamespace,
				DispatcherLabel:             base.ChannelDispatcherLabel,
				ReceiverLabel:               base.ChannelReceiverLabel,
			},
			Env: env,
			NewKafkaClusterAdminClient: func(_ []string, _ *sarama.Config) (sarama.ClusterAdmin, error) {
				return &kafkatesting.MockKafkaClusterAdmin{
					ExpectedTopicName: expectedTopicName,
					ExpectedTopicDetail: sarama.TopicDetail{
						NumPartitions:     numPartitions,
						ReplicationFactor: replicationFactor,
						ConfigEntries: map[string]*string{
							messagingv1beta.KafkaTopicConfigRetentionMs: &retentionMillisString,
						},
					},
					T: t,
				}, nil
			},
			ConfigMapLister:     listers.GetConfigMapLister(),
			ServiceLister:       listers.GetServiceLister(),
			SubscriptionLister:  listers.GetSubscriptionLister(),
			ConsumerGroupLister: listers.GetConsumerGroupLister(),
			InternalsClient:     fakeconsumergroupinformer.Get(ctx),
			Prober:              proberMock,
			IngressHost:         network.GetServiceHostname(env.IngressName, env.SystemNamespace),
			KafkaFeatureFlags:   featureFlags,
		}
		reconciler.Tracker = &FakeTracker{}
		reconciler.Tracker = &FakeTracker{}

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

func customTemplate() *template.Template {
	channelsTemplate, _ := template.New("channels.topic.template").Parse("custom-channel-template.{{ .Namespace }}.{{ .Name }}")
	return channelsTemplate
}

func newKafkaFeaturesConfigFromMap(cm *corev1.ConfigMap) *apisconfig.KafkaFeatureFlags {
	featureFlags, err := apisconfig.NewFeaturesConfigFromMap(cm)
	if err != nil {
		panic("failed to create kafka features from config map")
	}
	return featureFlags
}

func makeTLSSecret() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: system.Namespace(),
			Name:      kafkaChannelTLSSecretName,
		},
		Data: map[string][]byte{
			"ca.crt": []byte(testCaCerts),
		},
		Type: corev1.SecretTypeTLS,
	}
}

func httpsURL(name string, namespace string) *apis.URL {
	return &apis.URL{
		Scheme: "https",
		Host:   network.GetServiceHostname(DefaultEnv.IngressName, DefaultEnv.SystemNamespace),
		Path:   fmt.Sprintf("/%s/%s", namespace, name),
	}
}
