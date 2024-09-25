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
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"

	sources "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/sources/v1beta1"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"

	internalscg "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing/v1alpha1"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	brokerreconciler "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/broker"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgotesting "k8s.io/client-go/testing"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1"
	eventingclient "knative.dev/eventing/pkg/client/injection/client/fake"
	triggerreconciler "knative.dev/eventing/pkg/client/injection/reconciler/eventing/v1/trigger"
	reconcilertesting "knative.dev/eventing/pkg/reconciler/testing/v1"
	"knative.dev/pkg/apis"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/kmeta"
	"knative.dev/pkg/logging"
	. "knative.dev/pkg/reconciler/testing"

	. "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/testing"

	fakeconsumergroupinformer "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/client/fake"
)

const (
	// name of the trigger under test
	triggerName = "test-trigger"
	// namespace of the trigger under test
	triggerNamespace = "test-namespace"

	bootstrapServers = "kafka-1:9092,kafka-2:9093"

	consumerGroupId = "knative-trigger-test-namespace-test-trigger"
)

var DefaultEnv = &config.Env{
	DataPlaneConfigMapNamespace:  "knative-eventing",
	DataPlaneConfigConfigMapName: "config-kafka-broker-data-plane",
	ContractConfigMapFormat:      base.Json,
	GeneralConfigMapName:         "kafka-broker-config",
	SystemNamespace:              "knative-eventing",
}

var (
	url = &apis.URL{
		Scheme: "http",
		Host:   "localhost",
		Path:   "/path",
	}

	exponential = eventingduck.BackoffPolicyExponential

	finalizerUpdatedEvent = Eventf(
		corev1.EventTypeNormal,
		"FinalizerUpdate",
		fmt.Sprintf(`Updated %q finalizers`, TriggerName),
	)
)

func TestReconcileKind(t *testing.T) {

	env := DefaultEnv

	testKey := fmt.Sprintf("%s/%s", triggerNamespace, triggerName)

	table := TableTest{
		{
			Name: "Reconciled normal",
			Objects: []runtime.Object{
				NewBroker(
					BrokerReady,
					WithTopicStatusAnnotation(BrokerTopic()),
					WithBootstrapServerStatusAnnotation(bootstrapServers),
				),
				newTrigger(),
				DataPlaneConfigMap(env.DataPlaneConfigMapNamespace, env.DataPlaneConfigConfigMapName, brokerreconciler.ConsumerConfigKey,
					DataPlaneConfigInitialOffset(brokerreconciler.ConsumerConfigKey, sources.OffsetLatest),
				),
			},
			Key: testKey,
			WantCreates: []runtime.Object{
				NewConsumerGroup(
					WithConsumerGroupName(consumerGroupId),
					WithConsumerGroupNamespace(triggerNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(newTrigger())),
					WithConsumerGroupMetaLabels(OwnerAsTriggerLabel),
					WithConsumerGroupLabels(ConsumerTriggerLabel),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(BrokerTopics[0]),
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(bootstrapServers),
							ConsumerGroupIdConfig(consumerGroupId),
						),
						ConsumerDelivery(NewConsumerSpecDelivery(sources.Unordered, ConsumerInitialOffset(sources.OffsetLatest))),
						ConsumerFilters(NewConsumerSpecFilters()),
						ConsumerReply(ConsumerTopicReply()),
					)),
					withBrokerTopLevelResourceRef(),
				),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
						reconcilertesting.WithTriggerSubscribed(),
						reconcilertesting.WithTriggerBrokerReady(),
						withTriggerSubscriberResolvedSucceeded(),
						withTriggerStatusGroupIdAnnotation(consumerGroupId),
						reconcilertesting.WithTriggerDependencyUnknown("failed to reconcile consumer group", "consumer group is not ready"),
						withDeadLetterSinkURI(""),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - fallback to broker delivery",
			Objects: []runtime.Object{
				NewBroker(
					BrokerReady,
					WithRetry(pointer.Int32(10), &exponential, pointer.String("PT2S")),
					WithTopicStatusAnnotation(BrokerTopic()),
					WithBootstrapServerStatusAnnotation(bootstrapServers),
				),
				DataPlaneConfigMap(env.DataPlaneConfigMapNamespace, env.DataPlaneConfigConfigMapName, brokerreconciler.ConsumerConfigKey,
					DataPlaneConfigInitialOffset(brokerreconciler.ConsumerConfigKey, sources.OffsetLatest),
				),
				newTrigger(),
			},
			Key: testKey,
			WantCreates: []runtime.Object{
				NewConsumerGroup(
					WithConsumerGroupName(consumerGroupId),
					WithConsumerGroupNamespace(triggerNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(newTrigger())),
					WithConsumerGroupMetaLabels(OwnerAsTriggerLabel),
					WithConsumerGroupLabels(ConsumerTriggerLabel),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(BrokerTopics[0]),
						ConsumerConfigs(
							ConsumerGroupIdConfig(consumerGroupId),
							ConsumerBootstrapServersConfig(bootstrapServers),
						),
						ConsumerDelivery(NewConsumerSpecDelivery(
							sources.Unordered,
							NewConsumerRetry(10),
							NewConsumerBackoffDelay("PT2S"),
							NewConsumerBackoffPolicy(eventingduck.BackoffPolicyExponential),
							ConsumerInitialOffset(sources.OffsetLatest),
						)),
						ConsumerFilters(NewConsumerSpecFilters()),
						ConsumerReply(ConsumerTopicReply()),
					)),
					withBrokerTopLevelResourceRef(),
				),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
						reconcilertesting.WithTriggerSubscribed(),
						reconcilertesting.WithTriggerBrokerReady(),
						withTriggerSubscriberResolvedSucceeded(),
						withTriggerStatusGroupIdAnnotation(consumerGroupId),
						reconcilertesting.WithTriggerDependencyUnknown("failed to reconcile consumer group", "consumer group is not ready"),
						withDeadLetterSinkURI(""),
					),
				},
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
		},
		{
			Name: "Reconciled normal - Trigger with ordered delivery",
			Objects: []runtime.Object{
				NewBroker(
					BrokerReady,
					WithTopicStatusAnnotation(BrokerTopic()),
					WithBootstrapServerStatusAnnotation(bootstrapServers),
				),
				DataPlaneConfigMap(env.DataPlaneConfigMapNamespace, env.DataPlaneConfigConfigMapName, brokerreconciler.ConsumerConfigKey,
					DataPlaneConfigInitialOffset(brokerreconciler.ConsumerConfigKey, sources.OffsetLatest),
				),
				newTrigger(reconcilertesting.WithAnnotation(deliveryOrderAnnotation, string(sources.Ordered))),
			},
			Key: testKey,
			WantCreates: []runtime.Object{
				NewConsumerGroup(
					WithConsumerGroupName(consumerGroupId),
					WithConsumerGroupNamespace(triggerNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(newTrigger())),
					WithConsumerGroupMetaLabels(OwnerAsTriggerLabel),
					WithConsumerGroupLabels(ConsumerTriggerLabel),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(BrokerTopics[0]),
						ConsumerConfigs(
							ConsumerGroupIdConfig(consumerGroupId),
							ConsumerBootstrapServersConfig(bootstrapServers),
						),
						ConsumerDelivery(NewConsumerSpecDelivery(sources.Ordered, ConsumerInitialOffset(sources.OffsetLatest))),
						ConsumerFilters(NewConsumerSpecFilters()),
						ConsumerReply(ConsumerTopicReply()),
					)),
					withBrokerTopLevelResourceRef(),
				),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
						reconcilertesting.WithTriggerSubscribed(),
						reconcilertesting.WithTriggerBrokerReady(),
						withTriggerSubscriberResolvedSucceeded(),
						withTriggerStatusGroupIdAnnotation(consumerGroupId),
						reconcilertesting.WithTriggerDependencyUnknown("failed to reconcile consumer group", "consumer group is not ready"),
						withDeadLetterSinkURI(""),
						reconcilertesting.WithAnnotation(deliveryOrderAnnotation, string(sources.Ordered)),
					),
				},
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
		},
		{
			Name: "Reconciled normal - Trigger with unordered delivery",
			Objects: []runtime.Object{
				NewBroker(
					BrokerReady,
					WithTopicStatusAnnotation(BrokerTopic()),
					WithBootstrapServerStatusAnnotation(bootstrapServers),
				),
				DataPlaneConfigMap(env.DataPlaneConfigMapNamespace, env.DataPlaneConfigConfigMapName, brokerreconciler.ConsumerConfigKey,
					DataPlaneConfigInitialOffset(brokerreconciler.ConsumerConfigKey, sources.OffsetLatest),
				),
				newTrigger(reconcilertesting.WithAnnotation(deliveryOrderAnnotation, string(sources.Unordered))),
			},
			Key: testKey,
			WantCreates: []runtime.Object{
				NewConsumerGroup(
					WithConsumerGroupName(consumerGroupId),
					WithConsumerGroupNamespace(triggerNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(newTrigger())),
					WithConsumerGroupMetaLabels(OwnerAsTriggerLabel),
					WithConsumerGroupLabels(ConsumerTriggerLabel),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(BrokerTopics[0]),
						ConsumerConfigs(
							ConsumerGroupIdConfig(consumerGroupId),
							ConsumerBootstrapServersConfig(bootstrapServers),
						),
						ConsumerDelivery(NewConsumerSpecDelivery(sources.Unordered, ConsumerInitialOffset(sources.OffsetLatest))),
						ConsumerFilters(NewConsumerSpecFilters()),
						ConsumerReply(ConsumerTopicReply()),
					)),
					withBrokerTopLevelResourceRef(),
				),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
						reconcilertesting.WithTriggerSubscribed(),
						reconcilertesting.WithTriggerBrokerReady(),
						withTriggerSubscriberResolvedSucceeded(),
						withTriggerStatusGroupIdAnnotation(consumerGroupId),
						reconcilertesting.WithTriggerDependencyUnknown("failed to reconcile consumer group", "consumer group is not ready"),
						withDeadLetterSinkURI(""),
						reconcilertesting.WithAnnotation(deliveryOrderAnnotation, string(sources.Unordered)),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - Trigger with invalid delivery",
			Objects: []runtime.Object{
				NewBroker(
					BrokerReady,
					WithTopicStatusAnnotation(BrokerTopic()),
					WithBootstrapServerStatusAnnotation(bootstrapServers),
				),
				DataPlaneConfigMap(env.DataPlaneConfigMapNamespace, env.DataPlaneConfigConfigMapName, brokerreconciler.ConsumerConfigKey,
					DataPlaneConfigInitialOffset(brokerreconciler.ConsumerConfigKey, sources.OffsetLatest),
				),
				newTrigger(reconcilertesting.WithAnnotation(deliveryOrderAnnotation, "invalid")),
			},
			Key:     testKey,
			WantErr: true,
			WantEvents: []string{
				finalizerUpdatedEvent,
				Eventf(
					corev1.EventTypeWarning,
					"InternalError",
					fmt.Sprintf(
						"invalid annotation %s value: invalid. Allowed values [ \"ordered\" | \"unordered\" ]",
						deliveryOrderAnnotation,
					),
				),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
						reconcilertesting.WithTriggerBrokerReady(),
						reconcilertesting.WithTriggerDependencyFailed("failed to reconcile consumer group", "invalid annotation kafka.eventing.knative.dev/delivery.order value: invalid. Allowed values [ \"ordered\" | \"unordered\" ]"),
						reconcilertesting.WithAnnotation(deliveryOrderAnnotation, "invalid"),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - existing cg with update",
			Objects: []runtime.Object{
				NewBroker(
					BrokerReady,
					WithTopicStatusAnnotation(BrokerTopic()),
					WithBootstrapServerStatusAnnotation(bootstrapServers),
				),
				DataPlaneConfigMap(env.DataPlaneConfigMapNamespace, env.DataPlaneConfigConfigMapName, brokerreconciler.ConsumerConfigKey,
					DataPlaneConfigInitialOffset(brokerreconciler.ConsumerConfigKey, sources.OffsetLatest),
				),
				newTrigger(),
				NewConsumerGroup(
					WithConsumerGroupName(consumerGroupId),
					WithConsumerGroupNamespace(triggerNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(newTrigger())),
					WithConsumerGroupMetaLabels(OwnerAsTriggerLabel),
					WithConsumerGroupLabels(ConsumerTriggerLabel),
					ConsumerGroupReady,
				),
			},
			Key:         testKey,
			WantCreates: []runtime.Object{},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewConsumerGroup(
						WithConsumerGroupName(consumerGroupId),
						WithConsumerGroupNamespace(triggerNamespace),
						WithConsumerGroupOwnerRef(kmeta.NewControllerRef(newTrigger())),
						WithConsumerGroupMetaLabels(OwnerAsTriggerLabel),
						WithConsumerGroupLabels(ConsumerTriggerLabel),
						ConsumerGroupConsumerSpec(NewConsumerSpec(
							ConsumerTopics(BrokerTopics[0]),
							ConsumerConfigs(
								ConsumerGroupIdConfig(consumerGroupId),
								ConsumerBootstrapServersConfig(bootstrapServers),
							),
							ConsumerDelivery(NewConsumerSpecDelivery(sources.Unordered, ConsumerInitialOffset(sources.OffsetLatest))),
							ConsumerFilters(NewConsumerSpecFilters()),
							ConsumerReply(ConsumerTopicReply()),
						)),
						ConsumerGroupReady,
						withBrokerTopLevelResourceRef(),
					),
				},
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
						reconcilertesting.WithTriggerSubscribed(),
						reconcilertesting.WithTriggerBrokerReady(),
						withTriggerSubscriberResolvedSucceeded(),
						withTriggerStatusGroupIdAnnotation(consumerGroupId),
						reconcilertesting.WithTriggerDependencyReady(),
						withDeadLetterSinkURI(""),
					),
				},
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
		},
		{
			Name: "Reconciled normal - existing cg with autoscaling annotations",
			Objects: []runtime.Object{
				NewBroker(
					BrokerReady,
					WithTopicStatusAnnotation(BrokerTopic()),
					WithBootstrapServerStatusAnnotation(bootstrapServers),
				),
				DataPlaneConfigMap(env.DataPlaneConfigMapNamespace, env.DataPlaneConfigConfigMapName, brokerreconciler.ConsumerConfigKey,
					DataPlaneConfigInitialOffset(brokerreconciler.ConsumerConfigKey, sources.OffsetLatest),
				),
				newTrigger(
					WithAutoscalingAnnotationsTrigger(),
				),
				NewConsumerGroup(
					WithConsumerGroupName(consumerGroupId),
					WithConsumerGroupNamespace(triggerNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(newTrigger())),
					WithConsumerGroupMetaLabels(OwnerAsTriggerLabel),
					WithConsumerGroupLabels(ConsumerTriggerLabel),
					ConsumerGroupReady,
					withBrokerTopLevelResourceRef(),
				),
			},
			Key:         testKey,
			WantCreates: []runtime.Object{},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewConsumerGroup(
						WithConsumerGroupName(consumerGroupId),
						WithConsumerGroupNamespace(triggerNamespace),
						WithConsumerGroupOwnerRef(kmeta.NewControllerRef(newTrigger())),
						WithConsumerGroupMetaLabels(OwnerAsTriggerLabel),
						WithConsumerGroupLabels(ConsumerTriggerLabel),
						WithConsumerGroupAnnotations(ConsumerGroupAnnotations),
						ConsumerGroupConsumerSpec(NewConsumerSpec(
							ConsumerTopics(BrokerTopics[0]),
							ConsumerConfigs(
								ConsumerGroupIdConfig(consumerGroupId),
								ConsumerBootstrapServersConfig(bootstrapServers),
							),
							ConsumerDelivery(NewConsumerSpecDelivery(sources.Unordered, ConsumerInitialOffset(sources.OffsetLatest))),
							ConsumerFilters(NewConsumerSpecFilters()),
							ConsumerReply(ConsumerTopicReply()),
						)),
						ConsumerGroupReady,
						withBrokerTopLevelResourceRef(),
					),
				},
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
						reconcilertesting.WithTriggerSubscribed(),
						reconcilertesting.WithTriggerBrokerReady(),
						withTriggerSubscriberResolvedSucceeded(),
						withTriggerStatusGroupIdAnnotation(consumerGroupId),
						reconcilertesting.WithTriggerDependencyReady(),
						withDeadLetterSinkURI(""),
						WithAutoscalingAnnotationsTrigger(),
					),
				},
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
		},
		{
			Name: "Reconciled normal - existing cg with update but not ready",
			Objects: []runtime.Object{
				NewBroker(
					BrokerReady,
					WithTopicStatusAnnotation(BrokerTopic()),
					WithBootstrapServerStatusAnnotation(bootstrapServers),
				),
				DataPlaneConfigMap(env.DataPlaneConfigMapNamespace, env.DataPlaneConfigConfigMapName, brokerreconciler.ConsumerConfigKey,
					DataPlaneConfigInitialOffset(brokerreconciler.ConsumerConfigKey, sources.OffsetLatest),
				),
				newTrigger(),
				NewConsumerGroup(
					WithConsumerGroupName(consumerGroupId),
					WithConsumerGroupNamespace(triggerNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(newTrigger())),
					WithConsumerGroupMetaLabels(OwnerAsTriggerLabel),
					WithConsumerGroupLabels(ConsumerTriggerLabel),
					withBrokerTopLevelResourceRef(),
				),
			},
			Key:         testKey,
			WantCreates: []runtime.Object{},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewConsumerGroup(
						WithConsumerGroupName(consumerGroupId),
						WithConsumerGroupNamespace(triggerNamespace),
						WithConsumerGroupOwnerRef(kmeta.NewControllerRef(newTrigger())),
						WithConsumerGroupMetaLabels(OwnerAsTriggerLabel),
						WithConsumerGroupLabels(ConsumerTriggerLabel),
						ConsumerGroupConsumerSpec(NewConsumerSpec(
							ConsumerTopics(BrokerTopics[0]),
							ConsumerConfigs(
								ConsumerGroupIdConfig(consumerGroupId),
								ConsumerBootstrapServersConfig(bootstrapServers),
							),
							ConsumerDelivery(NewConsumerSpecDelivery(sources.Unordered, ConsumerInitialOffset(sources.OffsetLatest))),
							ConsumerFilters(NewConsumerSpecFilters()),
							ConsumerReply(ConsumerTopicReply()),
						)),
						withBrokerTopLevelResourceRef(),
					),
				},
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
						reconcilertesting.WithTriggerSubscribed(),
						reconcilertesting.WithTriggerBrokerReady(),
						withTriggerSubscriberResolvedSucceeded(),
						withTriggerStatusGroupIdAnnotation(consumerGroupId),
						reconcilertesting.WithTriggerDependencyUnknown("failed to reconcile consumer group", "consumer group is not ready"),
						withDeadLetterSinkURI(""),
					),
				},
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
		},
		{
			Name: "Reconciled normal - with auth - SASL",
			Objects: []runtime.Object{
				NewBroker(
					BrokerReady,
					WithTopicStatusAnnotation(BrokerTopic()),
					WithBootstrapServerStatusAnnotation(bootstrapServers),
					WithSecretStatusAnnotation("secret-1"),
				),
				DataPlaneConfigMap(env.DataPlaneConfigMapNamespace, env.DataPlaneConfigConfigMapName, brokerreconciler.ConsumerConfigKey,
					DataPlaneConfigInitialOffset(brokerreconciler.ConsumerConfigKey, sources.OffsetLatest),
				),
				newTrigger(),
				NewConsumerGroup(
					WithConsumerGroupName(consumerGroupId),
					WithConsumerGroupNamespace(triggerNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(newTrigger())),
					WithConsumerGroupMetaLabels(OwnerAsTriggerLabel),
					WithConsumerGroupLabels(ConsumerTriggerLabel),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(ChannelTopic()),
						ConsumerConfigs(
							ConsumerGroupIdConfig(consumerGroupId),
							ConsumerBootstrapServersConfig(bootstrapServers),
						),
						ConsumerDelivery(NewConsumerSpecDelivery(sources.Ordered)),
						ConsumerSubscriber(NewConsumerSpecSubscriber(Subscription1URI)),
						ConsumerAuth(&internalscg.Auth{
							SecretSpec: &internalscg.SecretSpec{
								Ref: &internalscg.SecretReference{
									Name:      "secret-1",
									Namespace: ConfigMapNamespace,
								},
							},
						}),
					)),
					withBrokerTopLevelResourceRef(),
				),
				NewLegacySASLSecret(ConfigMapNamespace, "secret-1"),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewConsumerGroup(
						WithConsumerGroupName(consumerGroupId),
						WithConsumerGroupNamespace(triggerNamespace),
						WithConsumerGroupOwnerRef(kmeta.NewControllerRef(newTrigger())),
						WithConsumerGroupMetaLabels(OwnerAsTriggerLabel),
						WithConsumerGroupLabels(ConsumerTriggerLabel),
						ConsumerGroupConsumerSpec(NewConsumerSpec(
							ConsumerTopics(BrokerTopics[0]),
							ConsumerConfigs(
								ConsumerGroupIdConfig(consumerGroupId),
								ConsumerBootstrapServersConfig(bootstrapServers),
							),
							ConsumerDelivery(NewConsumerSpecDelivery(sources.Unordered, ConsumerInitialOffset(sources.OffsetLatest))),
							ConsumerFilters(NewConsumerSpecFilters()),
							ConsumerReply(ConsumerTopicReply()),
							ConsumerAuth(&internalscg.Auth{
								SecretSpec: &internalscg.SecretSpec{
									Ref: &internalscg.SecretReference{
										Name:      "secret-1",
										Namespace: ConfigMapNamespace,
									},
								},
							}),
						)),
						withBrokerTopLevelResourceRef(),
					),
				},
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
						reconcilertesting.WithTriggerSubscribed(),
						reconcilertesting.WithTriggerBrokerReady(),
						withTriggerSubscriberResolvedSucceeded(),
						withTriggerStatusGroupIdAnnotation(consumerGroupId),
						reconcilertesting.WithTriggerDependencyUnknown("failed to reconcile consumer group", "consumer group is not ready"),
						withDeadLetterSinkURI(""),
					),
				},
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
		},
		{
			Name: "Reconciled normal - existing cg without update",
			Objects: []runtime.Object{
				NewBroker(
					BrokerReady,
					WithTopicStatusAnnotation(BrokerTopic()),
					WithBootstrapServerStatusAnnotation(bootstrapServers),
				),
				DataPlaneConfigMap(env.DataPlaneConfigMapNamespace, env.DataPlaneConfigConfigMapName, brokerreconciler.ConsumerConfigKey,
					DataPlaneConfigInitialOffset(brokerreconciler.ConsumerConfigKey, sources.OffsetLatest),
				),
				newTrigger(),
				NewConsumerGroup(
					WithConsumerGroupName(consumerGroupId),
					WithConsumerGroupNamespace(triggerNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(newTrigger())),
					WithConsumerGroupMetaLabels(OwnerAsTriggerLabel),
					WithConsumerGroupLabels(ConsumerTriggerLabel),
					WithConsumerGroupAnnotations(ConsumerGroupAnnotations),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(BrokerTopics[0]),
						ConsumerConfigs(
							ConsumerGroupIdConfig(consumerGroupId),
							ConsumerBootstrapServersConfig(bootstrapServers),
						),
						ConsumerDelivery(NewConsumerSpecDelivery(sources.Unordered, ConsumerInitialOffset(sources.OffsetLatest))),
						ConsumerFilters(NewConsumerSpecFilters()),
						ConsumerReply(ConsumerTopicReply()),
					)),
					ConsumerGroupReady,
					ConsumerGroupReplicas(1),
					withBrokerTopLevelResourceRef(),
				),
			},
			Key: testKey,
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
						reconcilertesting.WithTriggerSubscribed(),
						reconcilertesting.WithTriggerBrokerReady(),
						withTriggerSubscriberResolvedSucceeded(),
						withTriggerStatusGroupIdAnnotation(consumerGroupId),
						reconcilertesting.WithTriggerDependencyReady(),
						withDeadLetterSinkURI(""),
					),
				},
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
		},
		{
			Name: "Reconciled normal - existing cg without update but not ready",
			Objects: []runtime.Object{
				NewBroker(
					BrokerReady,
					WithTopicStatusAnnotation(BrokerTopic()),
					WithBootstrapServerStatusAnnotation(bootstrapServers),
				),
				DataPlaneConfigMap(env.DataPlaneConfigMapNamespace, env.DataPlaneConfigConfigMapName, brokerreconciler.ConsumerConfigKey,
					DataPlaneConfigInitialOffset(brokerreconciler.ConsumerConfigKey, sources.OffsetLatest),
				),
				newTrigger(),
				NewConsumerGroup(
					WithConsumerGroupName(consumerGroupId),
					WithConsumerGroupNamespace(triggerNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(newTrigger())),
					WithConsumerGroupMetaLabels(OwnerAsTriggerLabel),
					WithConsumerGroupLabels(ConsumerTriggerLabel),
					WithConsumerGroupAnnotations(ConsumerGroupAnnotations),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(BrokerTopics[0]),
						ConsumerConfigs(
							ConsumerGroupIdConfig(consumerGroupId),
							ConsumerBootstrapServersConfig(bootstrapServers),
						),
						ConsumerDelivery(NewConsumerSpecDelivery(sources.Unordered, ConsumerInitialOffset(sources.OffsetLatest))),
						ConsumerFilters(NewConsumerSpecFilters()),
						ConsumerReply(ConsumerTopicReply()),
					)),
					ConsumerGroupReplicas(1),
					withBrokerTopLevelResourceRef(),
				),
			},
			Key: testKey,
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
						reconcilertesting.WithTriggerSubscribed(),
						reconcilertesting.WithTriggerBrokerReady(),
						withTriggerSubscriberResolvedSucceeded(),
						withTriggerStatusGroupIdAnnotation(consumerGroupId),
						reconcilertesting.WithTriggerDependencyUnknown("failed to reconcile consumer group", "consumer group is not ready"),
						withDeadLetterSinkURI(""),
					),
				},
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
		},
		{
			Name: "Reconciled normal - existing cg but failed",
			Objects: []runtime.Object{
				NewBroker(
					BrokerReady,
					WithTopicStatusAnnotation(BrokerTopic()),
					WithBootstrapServerStatusAnnotation(bootstrapServers),
				),
				DataPlaneConfigMap(env.DataPlaneConfigMapNamespace, env.DataPlaneConfigConfigMapName, brokerreconciler.ConsumerConfigKey,
					DataPlaneConfigInitialOffset(brokerreconciler.ConsumerConfigKey, sources.OffsetLatest),
				),
				newTrigger(),
				NewConsumerGroup(
					WithConsumerGroupName(consumerGroupId),
					WithConsumerGroupNamespace(triggerNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(newTrigger())),
					WithConsumerGroupMetaLabels(OwnerAsTriggerLabel),
					WithConsumerGroupLabels(ConsumerTriggerLabel),
					WithConsumerGroupAnnotations(ConsumerGroupAnnotations),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(BrokerTopics[0]),
						ConsumerConfigs(
							ConsumerGroupIdConfig(consumerGroupId),
							ConsumerBootstrapServersConfig(bootstrapServers),
						),
						ConsumerDelivery(NewConsumerSpecDelivery(sources.Unordered, ConsumerInitialOffset(sources.OffsetLatest))),
						ConsumerFilters(NewConsumerSpecFilters()),
						ConsumerReply(ConsumerTopicReply()),
					)),
					WithConsumerGroupFailed("failed", "failed"),
					ConsumerGroupReplicas(1),
					withBrokerTopLevelResourceRef(),
				),
			},
			Key: testKey,
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
						reconcilertesting.WithTriggerSubscribed(),
						reconcilertesting.WithTriggerBrokerReady(),
						withTriggerSubscriberResolvedSucceeded(),
						withTriggerStatusGroupIdAnnotation(consumerGroupId),
						reconcilertesting.WithTriggerDependencyFailed("failed", "failed"),
						withDeadLetterSinkURI(""),
					),
				},
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
		},
		{
			Name: "Reconciled normal with dead letter sink uri",
			Objects: []runtime.Object{
				NewBroker(
					BrokerReady,
					WithTopicStatusAnnotation(BrokerTopic()),
					WithBootstrapServerStatusAnnotation(bootstrapServers),
				),
				DataPlaneConfigMap(env.DataPlaneConfigMapNamespace, env.DataPlaneConfigConfigMapName, brokerreconciler.ConsumerConfigKey,
					DataPlaneConfigInitialOffset(brokerreconciler.ConsumerConfigKey, sources.OffsetLatest),
				),
				newTrigger(),
				NewConsumerGroup(
					WithConsumerGroupName(consumerGroupId),
					WithConsumerGroupNamespace(triggerNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(newTrigger())),
					WithConsumerGroupMetaLabels(OwnerAsTriggerLabel),
					WithConsumerGroupLabels(ConsumerTriggerLabel),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(BrokerTopics[0]),
						ConsumerConfigs(
							ConsumerGroupIdConfig(consumerGroupId),
							ConsumerBootstrapServersConfig(bootstrapServers),
						),
						ConsumerDelivery(NewConsumerSpecDelivery(sources.Unordered, ConsumerInitialOffset(sources.OffsetLatest))),
						ConsumerFilters(NewConsumerSpecFilters()),
						ConsumerReply(ConsumerTopicReply()),
					)),
					WithDeadLetterSinkURI(url.String()),
					ConsumerGroupReplicas(1),
					withBrokerTopLevelResourceRef(),
				),
			},
			Key: testKey,
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
						reconcilertesting.WithTriggerSubscribed(),
						reconcilertesting.WithTriggerBrokerReady(),
						withTriggerSubscriberResolvedSucceeded(),
						withTriggerStatusGroupIdAnnotation(consumerGroupId),
						reconcilertesting.WithTriggerDependencyUnknown("failed to reconcile consumer group", "consumer group is not ready"),
						withDeadLetterSinkURI(url.String()),
					),
				},
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
		},
		{
			Name: "Broker not found",
			Objects: []runtime.Object{
				newTrigger(),
			},
			Key:     testKey,
			WantErr: true,
			WantEvents: []string{
				finalizerUpdatedEvent,
				Eventf(
					corev1.EventTypeWarning,
					"InternalError",
					fmt.Sprintf(
						"failed to get broker: brokers.eventing.knative.dev \"%s\" not found",
						BrokerName,
					),
				),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
		},
		{
			Name: "Broker not ready",
			Objects: []runtime.Object{
				newTrigger(),
				NewBroker(
					func(v *eventing.Broker) { v.Status.InitializeConditions() },
					StatusBrokerConfigNotParsed("wrong"),
				),
			},
			Key: testKey,
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
					),
				},
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
		},
		{
			Name: "Broker deleted",
			Objects: []runtime.Object{
				newTrigger(),
				NewDeletedBroker(),
			},
			Key: testKey,
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerBrokerNotConfigured(),
						reconcilertesting.WithTriggerOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
					),
				},
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
		},
		{
			Name: "Don't reconcile trigger associated with a broker with a different broker class",
			Objects: []runtime.Object{
				newTrigger(),
				NewBroker(func(b *eventing.Broker) {
					b.Annotations = map[string]string{
						eventing.BrokerClassAnnotationKey: "MTChannelBasedBroker",
					}
				}),
			},
			Key: testKey,
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
					),
				},
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
		},
		{
			Name: "Reconciled reusing old consumergroup naming",
			Objects: []runtime.Object{
				NewBroker(
					BrokerReady,
					WithTopicStatusAnnotation(BrokerTopic()),
					WithBootstrapServerStatusAnnotation(bootstrapServers),
				),
				DataPlaneConfigMap(env.DataPlaneConfigMapNamespace, env.DataPlaneConfigConfigMapName, brokerreconciler.ConsumerConfigKey,
					DataPlaneConfigInitialOffset(brokerreconciler.ConsumerConfigKey, sources.OffsetLatest),
				),
				newTrigger(),
				NewConsumerGroup(
					WithConsumerGroupName(TriggerUUID),
					WithConsumerGroupNamespace(triggerNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(newTrigger())),
					WithConsumerGroupMetaLabels(OwnerAsTriggerLabel),
					WithConsumerGroupLabels(ConsumerTriggerLabel),
					ConsumerGroupReady,
					withBrokerTopLevelResourceRef(),
				),
			},
			Key:         testKey,
			WantCreates: []runtime.Object{},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewConsumerGroup(
						WithConsumerGroupName(TriggerUUID),
						WithConsumerGroupNamespace(triggerNamespace),
						WithConsumerGroupOwnerRef(kmeta.NewControllerRef(newTrigger())),
						WithConsumerGroupMetaLabels(OwnerAsTriggerLabel),
						WithConsumerGroupLabels(ConsumerTriggerLabel),
						ConsumerGroupConsumerSpec(NewConsumerSpec(
							ConsumerTopics(BrokerTopics[0]),
							ConsumerConfigs(
								ConsumerGroupIdConfig(TriggerUUID),
								ConsumerBootstrapServersConfig(bootstrapServers),
							),
							ConsumerDelivery(NewConsumerSpecDelivery(sources.Unordered, ConsumerInitialOffset(sources.OffsetLatest))),
							ConsumerFilters(NewConsumerSpecFilters()),
							ConsumerReply(ConsumerTopicReply()),
						)),
						ConsumerGroupReady,
						withBrokerTopLevelResourceRef(),
					),
				},
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
						reconcilertesting.WithTriggerSubscribed(),
						reconcilertesting.WithTriggerBrokerReady(),
						withTriggerSubscriberResolvedSucceeded(),
						withTriggerStatusGroupIdAnnotation(TriggerUUID),
						reconcilertesting.WithTriggerDependencyReady(),
						withDeadLetterSinkURI(""),
					),
				},
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
		},
		{
			Name: "Finalized normal",
			Objects: []runtime.Object{
				NewBroker(
					BrokerReady,
					WithTopicStatusAnnotation(BrokerTopic()),
					WithBootstrapServerStatusAnnotation(bootstrapServers),
				),
				DataPlaneConfigMap(env.DataPlaneConfigMapNamespace, env.DataPlaneConfigConfigMapName, brokerreconciler.ConsumerConfigKey,
					DataPlaneConfigInitialOffset(brokerreconciler.ConsumerConfigKey, sources.OffsetLatest),
				),
				newTrigger(func(trigger *eventing.Trigger) {
					trigger.DeletionTimestamp = &metav1.Time{Time: time.Time{}.AddDate(1999, 1, 3)}
					trigger.Finalizers = []string{FinalizerName}
				}),
				NewConsumerGroup(
					WithConsumerGroupName(consumerGroupId),
					WithConsumerGroupNamespace(triggerNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(newTrigger())),
					WithConsumerGroupMetaLabels(OwnerAsTriggerLabel),
					WithConsumerGroupLabels(ConsumerTriggerLabel),
					WithConsumerGroupAnnotations(ConsumerGroupAnnotations),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(BrokerTopics[0]),
						ConsumerConfigs(
							ConsumerGroupIdConfig(consumerGroupId),
							ConsumerBootstrapServersConfig(bootstrapServers),
						),
						ConsumerDelivery(NewConsumerSpecDelivery(sources.Unordered, ConsumerInitialOffset(sources.OffsetLatest))),
						ConsumerFilters(NewConsumerSpecFilters()),
						ConsumerReply(ConsumerTopicReply()),
					)),
					ConsumerGroupReady,
					ConsumerGroupReplicas(1),
					withBrokerTopLevelResourceRef(),
				),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				removeFinalizers(),
			},
		},
	}

	table.Test(t, NewFactory(env, func(ctx context.Context, listers *Listers, env *config.Env, row *TableRow) controller.Reconciler {
		logger := logging.FromContext(ctx)

		reconciler := &Reconciler{
			BrokerLister:         listers.GetBrokerLister(),
			ConfigMapLister:      listers.GetConfigMapLister(),
			ServiceAccountLister: listers.GetServiceAccountLister(),
			EventingClient:       eventingclient.Get(ctx),
			Env:                  env,
			ConsumerGroupLister:  listers.GetConsumerGroupLister(),
			InternalsClient:      fakeconsumergroupinformer.Get(ctx),
			SecretLister:         listers.GetSecretLister(),
			KubeClient:           kubeclient.Get(ctx),
		}

		return triggerreconciler.NewReconciler(
			ctx,
			logger,
			eventingclient.Get(ctx),
			listers.GetTriggerLister(),
			controller.GetEventRecorder(ctx),
			reconciler,
			controller.Options{FinalizerName: FinalizerName},
		)
	}))
}

func newTrigger(options ...reconcilertesting.TriggerOption) *eventing.Trigger {
	return reconcilertesting.NewTrigger(
		triggerName,
		triggerNamespace,
		BrokerName,
		append(
			options,
			//reconcilertesting.WithTriggerSubscriberURI(ServiceURL),
			func(t *eventing.Trigger) {
				t.UID = TriggerUUID
			},
		)...,
	)
}

func withDeadLetterSinkURI(uri string) func(trigger *eventing.Trigger) {
	return func(trigger *eventing.Trigger) {
		u, err := apis.ParseURL(uri)
		if err != nil {
			panic(err)
		}
		trigger.Status.DeadLetterSinkURI = u
		trigger.Status.MarkDeadLetterSinkResolvedSucceeded()
	}
}

func withTriggerSubscriberResolvedSucceeded() func(*eventing.Trigger) {
	return func(t *eventing.Trigger) {
		t.GetConditionSet().Manage(&t.Status).MarkTrue(
			eventing.TriggerConditionSubscriberResolved,
		)
	}
}

func withTriggerStatusGroupIdAnnotation(groupId string) func(*eventing.Trigger) {
	return func(t *eventing.Trigger) {
		if t.Status.Annotations == nil {
			t.Status.Annotations = make(map[string]string, 1)
		}
		t.Status.Annotations[kafka.GroupIdAnnotation] = groupId
	}
}

func patchFinalizers() clientgotesting.PatchActionImpl {
	action := clientgotesting.PatchActionImpl{}
	action.Name = TriggerName
	action.Namespace = TriggerNamespace
	patch := `{"metadata":{"finalizers":["` + FinalizerName + `"],"resourceVersion":""}}`
	action.Patch = []byte(patch)
	return action
}

func removeFinalizers() clientgotesting.PatchActionImpl {
	action := clientgotesting.PatchActionImpl{}
	action.Name = TriggerName
	action.Namespace = TriggerNamespace
	patch := `{"metadata":{"finalizers":[],"resourceVersion":""}}`
	action.Patch = []byte(patch)
	return action
}

func withBrokerTopLevelResourceRef() ConsumerGroupOption {
	return WithTopLevelResourceRef(&corev1.ObjectReference{
		APIVersion: eventing.SchemeGroupVersion.String(),
		Kind:       "Broker",
		Namespace:  BrokerNamespace,
		Name:       BrokerName,
		UID:        BrokerUUID,
	})
}
