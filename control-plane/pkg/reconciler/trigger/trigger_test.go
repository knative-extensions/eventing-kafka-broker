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

package trigger

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgotesting "k8s.io/client-go/testing"
	"k8s.io/utils/pointer"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	kubeclient "knative.dev/pkg/client/injection/kube/client/fake"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	. "knative.dev/pkg/reconciler/testing"
	"knative.dev/pkg/resolver"
	"knative.dev/pkg/tracker"

	apisconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/config"
	sources "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/sources/v1beta1"

	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1"
	v1 "knative.dev/eventing/pkg/apis/eventing/v1"
	"knative.dev/eventing/pkg/apis/feature"
	"knative.dev/eventing/pkg/auth"
	eventingclient "knative.dev/eventing/pkg/client/injection/client/fake"
	triggerreconciler "knative.dev/eventing/pkg/client/injection/reconciler/eventing/v1/trigger"
	"knative.dev/eventing/pkg/eventingtls/eventingtlstesting"
	reconcilertesting "knative.dev/eventing/pkg/reconciler/testing/v1"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	kafkatesting "knative.dev/eventing-kafka-broker/control-plane/pkg/kafka/testing"
	brokerreconciler "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/broker"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/receiver"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	. "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/testing"
)

var DefaultEnv = &config.Env{
	DataPlaneConfigMapNamespace:  "knative-eventing",
	ContractConfigMapName:        "kafka-broker-brokers-triggers",
	GeneralConfigMapName:         "kafka-broker-config",
	DataPlaneConfigConfigMapName: "config-kafka-broker-data-plane",
	IngressName:                  "kafka-broker-ingress",
	SystemNamespace:              "knative-eventing",
	ContractConfigMapFormat:      base.Json,
	DefaultBackoffDelayMs:        1000,
}

var (
	finalizerUpdatedEvent = Eventf(
		corev1.EventTypeNormal,
		"FinalizerUpdate",
		fmt.Sprintf(`Updated %q finalizers`, TriggerName),
	)

	url = &apis.URL{
		Scheme: "http",
		Host:   "localhost",
		Path:   "/path",
	}

	exponential = eventingduck.BackoffPolicyExponential

	bootstrapServers = "kafka-1:9092,kafka-2:9093"
)

const (
	kafkaFeatureFlags = "kafka-feature-flags"

	triggerConsumerGroup = "knative-trigger-test-namespace-test-trigger"
)

type EgressBuilder struct {
	*contract.Egress
}

func (b EgressBuilder) build(useDialectedFilters bool) *contract.Egress {
	if useDialectedFilters {
		b.Filter = nil
	} else {
		b.DialectedFilter = nil
	}
	return b.Egress
}

func TestTriggerReconciler(t *testing.T) {
	eventing.RegisterAlternateBrokerConditionSet(base.IngressConditionSet)

	t.Parallel()

	for _, f := range Formats {
		triggerReconciliation(t, f, *DefaultEnv, false)
	}
}

func TestTriggerWithNewFiltersReconciler(t *testing.T) {
	eventing.RegisterAlternateBrokerConditionSet(base.IngressConditionSet)

	t.Parallel()

	for _, f := range Formats {
		triggerReconciliation(t, f, *DefaultEnv, true)
	}
}

func triggerReconciliation(t *testing.T, format string, env config.Env, useNewFilters bool) {

	testKey := fmt.Sprintf("%s/%s", TriggerNamespace, TriggerName)

	env.ContractConfigMapFormat = format

	table := TableTest{
		{
			Name: "Reconciled normal - no existing Trigger",
			Objects: []runtime.Object{
				NewBroker(
					BrokerReady,
					WithTopicStatusAnnotation(BrokerTopic()),
					WithBootstrapServerStatusAnnotation(bootstrapServers),
				),
				newTrigger(),
				NewService(),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     BrokerUUID,
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
						},
					},
				}, env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat),
				BrokerDispatcherPod(env.SystemNamespace, nil),
				DataPlaneConfigMap(env.DataPlaneConfigMapNamespace, env.DataPlaneConfigConfigMapName, brokerreconciler.ConsumerConfigKey,
					DataPlaneConfigInitialOffset(brokerreconciler.ConsumerConfigKey, sources.OffsetLatest),
				),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     BrokerUUID,
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							Egresses: []*contract.Egress{
								{
									Destination:   ServiceURL,
									ConsumerGroup: triggerConsumerGroup,
									Uid:           TriggerUUID,
									Reference:     TriggerReference(),
								},
							},
						},
					},
					Generation: 1,
				}),
				BrokerDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
						reconcilertesting.WithTriggerSubscribed(),
						withSubscriberURI,
						reconcilertesting.WithTriggerDependencyReady(),
						reconcilertesting.WithTriggerBrokerReady(),
						withTriggerSubscriberResolvedSucceeded(contract.DeliveryOrder_UNORDERED),
						withTriggerStatusGroupIdAnnotation(triggerConsumerGroup),
						reconcilertesting.WithTriggerDeadLetterSinkNotConfigured(),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - with Broker DLS",
			Objects: []runtime.Object{
				NewBroker(
					BrokerReady,
					WithDelivery(),
					WithTopicStatusAnnotation(BrokerTopic()),
					WithBootstrapServerStatusAnnotation(bootstrapServers),
				),
				newTrigger(),
				NewService(),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     BrokerUUID,
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
						},
					},
				}, env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat),
				BrokerDispatcherPod(env.SystemNamespace, nil),
				DataPlaneConfigMap(env.DataPlaneConfigMapNamespace, env.DataPlaneConfigConfigMapName, brokerreconciler.ConsumerConfigKey,
					DataPlaneConfigInitialOffset(brokerreconciler.ConsumerConfigKey, sources.OffsetEarliest),
				),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     BrokerUUID,
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							Egresses: []*contract.Egress{
								{
									Destination:   ServiceURL,
									ConsumerGroup: triggerConsumerGroup,
									Uid:           TriggerUUID,
									EgressConfig:  &contract.EgressConfig{DeadLetter: ServiceURL},
									Reference:     TriggerReference(),
								},
							},
						},
					},
					Generation: 1,
				}),
				BrokerDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
						reconcilertesting.WithTriggerSubscribed(),
						withSubscriberURI,
						reconcilertesting.WithTriggerDependencyReady(),
						reconcilertesting.WithTriggerBrokerReady(),
						withTriggerSubscriberResolvedSucceeded(contract.DeliveryOrder_UNORDERED),
						withTriggerStatusGroupIdAnnotation(triggerConsumerGroup),
						withDeadLetterSinkURI(ServiceURL),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - with Trigger DLS",
			Objects: []runtime.Object{
				NewBroker(
					BrokerReady,
					WithTopicStatusAnnotation(BrokerTopic()),
					WithBootstrapServerStatusAnnotation(bootstrapServers),
				),
				newTrigger(withDelivery),
				NewService(),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     BrokerUUID,
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
						},
					},
				}, env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat),
				BrokerDispatcherPod(env.SystemNamespace, nil),
				DataPlaneConfigMap(env.DataPlaneConfigMapNamespace, env.DataPlaneConfigConfigMapName, brokerreconciler.ConsumerConfigKey,
					DataPlaneConfigInitialOffset(brokerreconciler.ConsumerConfigKey, sources.OffsetLatest),
				),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     BrokerUUID,
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							Egresses: []*contract.Egress{
								{
									Destination:   ServiceURL,
									ConsumerGroup: triggerConsumerGroup,
									Uid:           TriggerUUID,
									Reference:     TriggerReference(),
									EgressConfig: &contract.EgressConfig{
										DeadLetter:    url.String(),
										Retry:         3,
										BackoffPolicy: contract.BackoffPolicy_Exponential,
										BackoffDelay:  uint64(time.Second.Milliseconds()),
										Timeout:       uint64((time.Second * 2).Milliseconds()),
									},
								},
							},
						},
					},
					Generation: 1,
				}),
				BrokerDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						withDelivery,
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
						reconcilertesting.WithTriggerSubscribed(),
						withSubscriberURI,
						reconcilertesting.WithTriggerDependencyReady(),
						reconcilertesting.WithTriggerBrokerReady(),
						withTriggerSubscriberResolvedSucceeded(contract.DeliveryOrder_UNORDERED),
						withTriggerStatusGroupIdAnnotation(triggerConsumerGroup),
						withDeadLetterSinkURI(url.String()),
					),
				},
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
				newTrigger(reconcilertesting.WithAnnotation(deliveryOrderAnnotation, string(sources.Ordered))),
				NewService(),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     BrokerUUID,
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
						},
					},
				}, env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat),
				BrokerDispatcherPod(env.SystemNamespace, nil),
				DataPlaneConfigMap(env.DataPlaneConfigMapNamespace, env.DataPlaneConfigConfigMapName, brokerreconciler.ConsumerConfigKey,
					DataPlaneConfigInitialOffset(brokerreconciler.ConsumerConfigKey, sources.OffsetLatest),
				),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     BrokerUUID,
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							Egresses: []*contract.Egress{
								{
									Destination:   ServiceURL,
									ConsumerGroup: triggerConsumerGroup,
									Uid:           TriggerUUID,
									Reference:     TriggerReference(),
									DeliveryOrder: contract.DeliveryOrder_ORDERED,
								},
							},
						},
					},
					Generation: 1,
				}),
				BrokerDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
						reconcilertesting.WithTriggerSubscribed(),
						withSubscriberURI,
						reconcilertesting.WithTriggerDependencyReady(),
						reconcilertesting.WithTriggerBrokerReady(),
						withTriggerSubscriberResolvedSucceeded(contract.DeliveryOrder_ORDERED),
						withTriggerStatusGroupIdAnnotation(triggerConsumerGroup),
						reconcilertesting.WithAnnotation(deliveryOrderAnnotation, string(sources.Ordered)),
						reconcilertesting.WithTriggerDeadLetterSinkNotConfigured(),
					),
				},
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
				newTrigger(reconcilertesting.WithAnnotation(deliveryOrderAnnotation, string(sources.Unordered))),
				NewService(),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     BrokerUUID,
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
						},
					},
				}, env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat),
				BrokerDispatcherPod(env.SystemNamespace, nil),
				DataPlaneConfigMap(env.DataPlaneConfigMapNamespace, env.DataPlaneConfigConfigMapName, brokerreconciler.ConsumerConfigKey,
					DataPlaneConfigInitialOffset(brokerreconciler.ConsumerConfigKey, sources.OffsetLatest),
				),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     BrokerUUID,
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							Egresses: []*contract.Egress{
								{
									Destination:   ServiceURL,
									ConsumerGroup: triggerConsumerGroup,
									Uid:           TriggerUUID,
									Reference:     TriggerReference(),
									DeliveryOrder: contract.DeliveryOrder_UNORDERED,
								},
							},
						},
					},
					Generation: 1,
				}),
				BrokerDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
						reconcilertesting.WithTriggerSubscribed(),
						withSubscriberURI,
						reconcilertesting.WithTriggerDependencyReady(),
						reconcilertesting.WithTriggerBrokerReady(),
						withTriggerSubscriberResolvedSucceeded(contract.DeliveryOrder_UNORDERED),
						withTriggerStatusGroupIdAnnotation(triggerConsumerGroup),
						reconcilertesting.WithAnnotation(deliveryOrderAnnotation, string(sources.Unordered)),
						reconcilertesting.WithTriggerDeadLetterSinkNotConfigured(),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - Trigger delivery",
			Objects: []runtime.Object{
				NewBroker(
					BrokerReady,
					WithTopicStatusAnnotation(BrokerTopic()),
					WithBootstrapServerStatusAnnotation(bootstrapServers),
				),
				newTrigger(withDelivery),
				NewService(),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     BrokerUUID,
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
						},
					},
				}, env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat),
				BrokerDispatcherPod(env.SystemNamespace, nil),
				DataPlaneConfigMap(env.DataPlaneConfigMapNamespace, env.DataPlaneConfigConfigMapName, brokerreconciler.ConsumerConfigKey,
					DataPlaneConfigInitialOffset(brokerreconciler.ConsumerConfigKey, sources.OffsetLatest),
				),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     BrokerUUID,
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							Egresses: []*contract.Egress{
								{
									Destination:   ServiceURL,
									ConsumerGroup: triggerConsumerGroup,
									Uid:           TriggerUUID,
									Reference:     TriggerReference(),
									EgressConfig: &contract.EgressConfig{
										DeadLetter:    url.String(),
										Retry:         3,
										BackoffPolicy: contract.BackoffPolicy_Exponential,
										BackoffDelay:  uint64(time.Second.Milliseconds()),
										Timeout:       uint64((time.Second * 2).Milliseconds()),
									},
								},
							},
						},
					},
					Generation: 1,
				}),
				BrokerDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						withDelivery,
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
						reconcilertesting.WithTriggerSubscribed(),
						withSubscriberURI,
						reconcilertesting.WithTriggerDependencyReady(),
						reconcilertesting.WithTriggerBrokerReady(),
						withTriggerSubscriberResolvedSucceeded(contract.DeliveryOrder_UNORDERED),
						withTriggerStatusGroupIdAnnotation(triggerConsumerGroup),
						reconcilertesting.WithTriggerDeadLetterSinkResolvedSucceeded(),
						reconcilertesting.WithTriggerStatusDeadLetterSinkURI(duckv1.Addressable{
							URL: &apis.URL{
								Scheme: "http",
								Host:   "localhost",
								Path:   "/path",
							},
						}),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - with existing Triggers and Brokers",
			Objects: []runtime.Object{
				NewBroker(
					BrokerReady,
					WithTopicStatusAnnotation(BrokerTopic()),
					WithBootstrapServerStatusAnnotation(bootstrapServers),
				),
				newTrigger(),
				NewService(),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     BrokerUUID + "z",
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							Egresses: []*contract.Egress{
								{
									Destination:   ServiceURL,
									ConsumerGroup: triggerConsumerGroup,
									Uid:           TriggerUUID,
									Reference:     TriggerReference(),
								},
							},
						},
						{
							Uid:     BrokerUUID,
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							Egresses: []*contract.Egress{
								EgressBuilder{
									&contract.Egress{
										Filter: &contract.Filter{Attributes: map[string]string{
											"source": "source_value",
										}},
										DialectedFilter: []*contract.DialectedFilter{
											{Filter: &contract.DialectedFilter_Exact{
												Exact: &contract.Exact{
													Attributes: map[string]string{
														"source": "source_value",
													},
												},
											}},
										},
										Destination:   ServiceURL,
										ConsumerGroup: triggerConsumerGroup + "a",
										Uid:           TriggerUUID + "a",
									},
								}.build(useNewFilters),
								EgressBuilder{
									&contract.Egress{
										Filter: &contract.Filter{Attributes: map[string]string{
											"source": "source_value",
										}},
										DialectedFilter: []*contract.DialectedFilter{
											{Filter: &contract.DialectedFilter_Exact{
												Exact: &contract.Exact{
													Attributes: map[string]string{
														"source": "source_value",
													},
												},
											}},
										},
										Destination:   ServiceURL,
										ConsumerGroup: triggerConsumerGroup,
										Uid:           TriggerUUID,
										Reference:     TriggerReference(),
									},
								}.build(useNewFilters),
							},
						},
					},
					Generation: 2,
				}, env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat),
				BrokerDispatcherPod(env.SystemNamespace, nil),
				DataPlaneConfigMap(env.DataPlaneConfigMapNamespace, env.DataPlaneConfigConfigMapName, brokerreconciler.ConsumerConfigKey,
					DataPlaneConfigInitialOffset(brokerreconciler.ConsumerConfigKey, sources.OffsetLatest),
				),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     BrokerUUID + "z",
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							Egresses: []*contract.Egress{
								{
									Destination:   ServiceURL,
									ConsumerGroup: triggerConsumerGroup,
									Uid:           TriggerUUID,
									Reference:     TriggerReference(),
								},
							},
						},
						{
							Uid:     BrokerUUID,
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							Egresses: []*contract.Egress{
								EgressBuilder{
									&contract.Egress{
										Filter: &contract.Filter{Attributes: map[string]string{
											"source": "source_value",
										}},
										DialectedFilter: []*contract.DialectedFilter{
											{Filter: &contract.DialectedFilter_Exact{
												Exact: &contract.Exact{
													Attributes: map[string]string{
														"source": "source_value",
													},
												},
											}},
										},
										Destination:   ServiceURL,
										ConsumerGroup: triggerConsumerGroup + "a",
										Uid:           TriggerUUID + "a",
									},
								}.build(useNewFilters),
								{
									Destination:   ServiceURL,
									ConsumerGroup: triggerConsumerGroup,
									Uid:           TriggerUUID,
									Reference:     TriggerReference(),
								},
							},
						},
					},
					Generation: 3,
				}),
				BrokerDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "3",
				}),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
						reconcilertesting.WithTriggerSubscribed(),
						withSubscriberURI,
						reconcilertesting.WithTriggerDependencyReady(),
						reconcilertesting.WithTriggerBrokerReady(),
						withTriggerSubscriberResolvedSucceeded(contract.DeliveryOrder_UNORDERED),
						withTriggerStatusGroupIdAnnotation(triggerConsumerGroup),
						reconcilertesting.WithTriggerDeadLetterSinkNotConfigured(),
					),
				},
			},
		},
		{
			Name: "Config map not found - broker not found in config map",
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
				NewService(),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			Key:                     testKey,
			WantErr:                 true,
			SkipNamespaceValidation: true, // WantCreates compare the broker namespace with configmap namespace, so skip it
			WantCreates: []runtime.Object{
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
				Eventf(
					corev1.EventTypeWarning,
					"InternalError",
					fmt.Sprintf(
						"broker not found in data plane config map %s",
						env.DataPlaneConfigMapAsString(),
					),
				),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
						reconcilertesting.WithTriggerBrokerFailed(
							"Broker not found in data plane map",
							fmt.Sprintf("config map: %s", env.DataPlaneConfigMapAsString()),
						),
						withTriggerStatusGroupIdAnnotation(triggerConsumerGroup),
					),
				},
			},
		},
		{
			Name: "Empty data plane config map",
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
				NewService(),
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			Key:     testKey,
			WantErr: true,
			WantEvents: []string{
				finalizerUpdatedEvent,
				Eventf(
					corev1.EventTypeWarning,
					"InternalError",
					fmt.Sprintf(
						"broker not found in data plane config map %s",
						env.DataPlaneConfigMapAsString(),
					),
				),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
						reconcilertesting.WithTriggerBrokerFailed(
							"Broker not found in data plane map",
							fmt.Sprintf("config map: %s", env.DataPlaneConfigMapAsString()),
						),
						withTriggerStatusGroupIdAnnotation(triggerConsumerGroup),
					),
				},
			},
		},
		{
			Name: "Broker not found, no broker in config map",
			Objects: []runtime.Object{
				newTrigger(),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
					),
				},
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
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
						reconcilertesting.WithTriggerBrokerFailed("wrong", ""),
					),
				},
			},
		},
		{
			Name: "Broker deleted, no broker in config map",
			Objects: []runtime.Object{
				newTrigger(),
				NewDeletedBroker(),
				NewConfigMapFromContract(&contract.Contract{
					Generation: 8,
				}, env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
					),
				},
			},
		},
		{
			Name: "Broker deleted, no trigger in config map",
			Objects: []runtime.Object{
				newTrigger(),
				NewDeletedBroker(),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:    BrokerUUID,
							Topics: []string{BrokerTopic()},
						},
					},
					Generation: 8,
				}, env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
					),
				},
			},
		},
		{
			Name: "Broker deleted, trigger in config map",
			Objects: []runtime.Object{
				newTrigger(),
				NewDeletedBroker(),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:    BrokerUUID,
							Topics: []string{BrokerTopic()},
							Egresses: []*contract.Egress{
								{
									Destination:   ServiceURL,
									ConsumerGroup: triggerConsumerGroup + "a",
									Uid:           TriggerUUID + "a",
								},
								{
									Destination:   ServiceURL,
									ConsumerGroup: triggerConsumerGroup,
									Uid:           TriggerUUID,
									Reference:     TriggerReference(),
								},
							},
						},
					},
					Generation: 8,
				}, env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat),
				BrokerDispatcherPod(env.SystemNamespace, nil),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:    BrokerUUID,
							Topics: []string{BrokerTopic()},
							Egresses: []*contract.Egress{
								{
									Destination:   ServiceURL,
									ConsumerGroup: triggerConsumerGroup + "a",
									Uid:           TriggerUUID + "a",
								},
							},
						},
					},
					Generation: 9,
				}),
				BrokerDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "9",
				}),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - no op",
			Objects: []runtime.Object{
				NewBroker(
					BrokerReady,
					WithTopicStatusAnnotation(BrokerTopic()),
					WithBootstrapServerStatusAnnotation(bootstrapServers),
				),
				newTrigger(
					withFilters(
						map[string]string{"type": "type1"},
						[]eventing.SubscriptionsAPIFilter{
							{
								Exact: map[string]string{
									"type": "type1",
								},
							},
						},
						useNewFilters,
					),
				),
				NewService(),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     BrokerUUID,
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							Egresses: []*contract.Egress{
								EgressBuilder{
									&contract.Egress{
										Filter: &contract.Filter{Attributes: map[string]string{
											"type": "type1",
										}},
										DialectedFilter: []*contract.DialectedFilter{
											{Filter: &contract.DialectedFilter_Exact{
												Exact: &contract.Exact{
													Attributes: map[string]string{
														"type": "type1",
													},
												},
											}},
										},
										Destination:   ServiceURL,
										ConsumerGroup: triggerConsumerGroup,
										Uid:           TriggerUUID,
										Reference:     TriggerReference(),
									},
								}.build(useNewFilters),
							},
						},
					},
				}, env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat),
				BrokerDispatcherPod(env.SystemNamespace, nil),
				DataPlaneConfigMap(env.DataPlaneConfigMapNamespace, env.DataPlaneConfigConfigMapName, brokerreconciler.ConsumerConfigKey,
					DataPlaneConfigInitialOffset(brokerreconciler.ConsumerConfigKey, sources.OffsetLatest),
				),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						withFilters(
							map[string]string{"type": "type1"},
							[]eventing.SubscriptionsAPIFilter{
								{
									Exact: map[string]string{
										"type": "type1",
									},
								},
							},
							useNewFilters,
						),
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
						reconcilertesting.WithTriggerSubscribed(),
						withSubscriberURI,
						reconcilertesting.WithTriggerDependencyReady(),
						reconcilertesting.WithTriggerBrokerReady(),
						withTriggerSubscriberResolvedSucceeded(contract.DeliveryOrder_UNORDERED),
						withTriggerStatusGroupIdAnnotation(triggerConsumerGroup),
						reconcilertesting.WithTriggerDeadLetterSinkNotConfigured(),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - many Triggers - start",
			Objects: []runtime.Object{
				NewBroker(
					BrokerReady,
					WithTopicStatusAnnotation(BrokerTopic()),
					WithBootstrapServerStatusAnnotation(bootstrapServers),
				),
				newTrigger(
					withFilters(
						map[string]string{"type": "type1"},
						[]eventing.SubscriptionsAPIFilter{
							{
								Exact: map[string]string{
									"type": "type1",
								},
							},
						},
						useNewFilters,
					),
				),
				NewService(),
				DataPlaneConfigMap(env.DataPlaneConfigMapNamespace, env.DataPlaneConfigConfigMapName, brokerreconciler.ConsumerConfigKey,
					DataPlaneConfigInitialOffset(brokerreconciler.ConsumerConfigKey, sources.OffsetLatest),
				),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     BrokerUUID,
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							Egresses: []*contract.Egress{
								{
									Destination:   ServiceURL,
									ConsumerGroup: triggerConsumerGroup,
									Uid:           TriggerUUID,
									Reference:     TriggerReference(),
								},
								{
									Destination:   "http://example.com/1",
									ConsumerGroup: "1",
									Uid:           "1",
								},
								EgressBuilder{
									&contract.Egress{
										Destination:   "http://example.com/2",
										ConsumerGroup: "2",
										Uid:           "2",
										Filter: &contract.Filter{Attributes: map[string]string{
											"source": "source2",
										}},
										DialectedFilter: []*contract.DialectedFilter{
											{Filter: &contract.DialectedFilter_Exact{
												Exact: &contract.Exact{
													Attributes: map[string]string{
														"source": "source2",
													},
												},
											}},
										},
									},
								}.build(useNewFilters),
								EgressBuilder{
									&contract.Egress{
										Destination:   "http://example.com/3",
										ConsumerGroup: "3",
										Uid:           "3",
										Filter: &contract.Filter{Attributes: map[string]string{
											"source": "source3",
										}},
										DialectedFilter: []*contract.DialectedFilter{
											{Filter: &contract.DialectedFilter_Exact{
												Exact: &contract.Exact{
													Attributes: map[string]string{
														"source": "source3",
													},
												},
											}},
										},
									},
								}.build(useNewFilters),
							},
						},
						{
							Uid:     BrokerUUID + "a",
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							Egresses: []*contract.Egress{
								{
									Destination:   ServiceURL,
									ConsumerGroup: triggerConsumerGroup,
									Uid:           TriggerUUID,
								},
								{
									Destination:   "http://example.com/1",
									ConsumerGroup: "1",
									Uid:           "1",
								},
								EgressBuilder{
									&contract.Egress{
										Destination:   "http://example.com/2",
										ConsumerGroup: "2",
										Uid:           "2",
										Filter: &contract.Filter{Attributes: map[string]string{
											"source": "source2",
										}},
										DialectedFilter: []*contract.DialectedFilter{
											{Filter: &contract.DialectedFilter_Exact{
												Exact: &contract.Exact{
													Attributes: map[string]string{
														"source": "source2",
													},
												},
											}},
										},
									},
								}.build(useNewFilters),
								EgressBuilder{
									&contract.Egress{
										Destination:   "http://example.com/3",
										ConsumerGroup: "3",
										Uid:           "3",
										Filter: &contract.Filter{Attributes: map[string]string{
											"source": "source3",
										}},
										DialectedFilter: []*contract.DialectedFilter{
											{Filter: &contract.DialectedFilter_Exact{
												Exact: &contract.Exact{
													Attributes: map[string]string{
														"source": "source3",
													},
												},
											}},
										},
									},
								}.build(useNewFilters),
							},
						},
					},
				}, env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat),
				BrokerDispatcherPod(env.SystemNamespace, nil),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     BrokerUUID,
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							Egresses: []*contract.Egress{
								EgressBuilder{
									&contract.Egress{
										Destination:   ServiceURL,
										ConsumerGroup: triggerConsumerGroup,
										Uid:           TriggerUUID,
										Reference:     TriggerReference(),
										Filter: &contract.Filter{Attributes: map[string]string{
											"type": "type1",
										}},
										DialectedFilter: []*contract.DialectedFilter{
											{Filter: &contract.DialectedFilter_Exact{
												Exact: &contract.Exact{
													Attributes: map[string]string{
														"type": "type1",
													},
												},
											}},
										},
									},
								}.build(useNewFilters),
								{
									Destination:   "http://example.com/1",
									ConsumerGroup: "1",
									Uid:           "1",
								},
								EgressBuilder{
									&contract.Egress{
										Destination:   "http://example.com/2",
										ConsumerGroup: "2",
										Uid:           "2",
										Filter: &contract.Filter{Attributes: map[string]string{
											"source": "source2",
										}},
										DialectedFilter: []*contract.DialectedFilter{
											{Filter: &contract.DialectedFilter_Exact{
												Exact: &contract.Exact{
													Attributes: map[string]string{
														"source": "source2",
													},
												},
											}},
										},
									},
								}.build(useNewFilters),
								EgressBuilder{
									&contract.Egress{
										Destination:   "http://example.com/3",
										ConsumerGroup: "3",
										Uid:           "3",
										Filter: &contract.Filter{Attributes: map[string]string{
											"source": "source3",
										}},
										DialectedFilter: []*contract.DialectedFilter{
											{Filter: &contract.DialectedFilter_Exact{
												Exact: &contract.Exact{
													Attributes: map[string]string{
														"source": "source3",
													},
												},
											}},
										},
									},
								}.build(useNewFilters),
							},
						},
						{
							Uid:     BrokerUUID + "a",
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							Egresses: []*contract.Egress{
								{
									Destination:   ServiceURL,
									ConsumerGroup: triggerConsumerGroup,
									Uid:           TriggerUUID,
								},
								{
									Destination:   "http://example.com/1",
									ConsumerGroup: "1",
									Uid:           "1",
								},
								EgressBuilder{
									&contract.Egress{
										Destination:   "http://example.com/2",
										ConsumerGroup: "2",
										Uid:           "2",
										Filter: &contract.Filter{Attributes: map[string]string{
											"source": "source2",
										}},
										DialectedFilter: []*contract.DialectedFilter{
											{Filter: &contract.DialectedFilter_Exact{
												Exact: &contract.Exact{
													Attributes: map[string]string{
														"source": "source2",
													},
												},
											}},
										},
									},
								}.build(useNewFilters),
								EgressBuilder{
									&contract.Egress{
										Destination:   "http://example.com/3",
										ConsumerGroup: "3",
										Uid:           "3",
										Filter: &contract.Filter{Attributes: map[string]string{
											"source": "source3",
										}},
										DialectedFilter: []*contract.DialectedFilter{
											{Filter: &contract.DialectedFilter_Exact{
												Exact: &contract.Exact{
													Attributes: map[string]string{
														"source": "source3",
													},
												},
											}},
										},
									},
								}.build(useNewFilters),
							},
						},
					},
					Generation: 1,
				}),
				BrokerDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						withFilters(
							map[string]string{"type": "type1"},
							[]eventing.SubscriptionsAPIFilter{
								{
									Exact: map[string]string{
										"type": "type1",
									},
								},
							},
							useNewFilters,
						),
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
						reconcilertesting.WithTriggerSubscribed(),
						withSubscriberURI,
						reconcilertesting.WithTriggerDependencyReady(),
						reconcilertesting.WithTriggerBrokerReady(),
						withTriggerSubscriberResolvedSucceeded(contract.DeliveryOrder_UNORDERED),
						withTriggerStatusGroupIdAnnotation(triggerConsumerGroup),
						reconcilertesting.WithTriggerDeadLetterSinkNotConfigured(),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - many Triggers - end",
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
					withFilters(
						map[string]string{"ext": "extval"},
						[]eventing.SubscriptionsAPIFilter{
							{
								Exact: map[string]string{
									"ext": "extval",
								},
							},
						},
						useNewFilters,
					),
				),
				NewService(),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     BrokerUUID + "a",
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							Egresses: []*contract.Egress{
								{
									Destination:   ServiceURL,
									ConsumerGroup: triggerConsumerGroup,
									Uid:           TriggerUUID,
								},
								{
									Destination:   "http://example.com/1",
									ConsumerGroup: "1",
									Uid:           "1",
								},
								EgressBuilder{
									&contract.Egress{
										Destination:   "http://example.com/2",
										ConsumerGroup: "2",
										Uid:           "2",
										Filter: &contract.Filter{Attributes: map[string]string{
											"source": "source2",
										}},
										DialectedFilter: []*contract.DialectedFilter{
											{Filter: &contract.DialectedFilter_Exact{
												Exact: &contract.Exact{
													Attributes: map[string]string{
														"source": "source2",
													},
												},
											}},
										},
									},
								}.build(useNewFilters),
								EgressBuilder{
									&contract.Egress{
										Destination:   "http://example.com/3",
										ConsumerGroup: "3",
										Uid:           "3",
										Filter: &contract.Filter{Attributes: map[string]string{
											"source": "source3",
										}},
										DialectedFilter: []*contract.DialectedFilter{
											{Filter: &contract.DialectedFilter_Exact{
												Exact: &contract.Exact{
													Attributes: map[string]string{
														"source": "source3",
													},
												},
											}},
										},
									},
								}.build(useNewFilters),
							},
						},
						{
							Uid:     BrokerUUID,
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							Egresses: []*contract.Egress{
								{
									Destination:   "http://example.com/1",
									ConsumerGroup: "1",
									Uid:           "1",
								},
								EgressBuilder{
									&contract.Egress{
										Destination:   "http://example.com/2",
										ConsumerGroup: "2",
										Uid:           "2",
										Filter: &contract.Filter{Attributes: map[string]string{
											"source": "source2",
										}},
										DialectedFilter: []*contract.DialectedFilter{
											{Filter: &contract.DialectedFilter_Exact{
												Exact: &contract.Exact{
													Attributes: map[string]string{
														"source": "source2",
													},
												},
											}},
										},
									},
								}.build(useNewFilters),
								EgressBuilder{
									&contract.Egress{
										Destination:   "http://example.com/3",
										ConsumerGroup: "3",
										Uid:           "3",
										Filter: &contract.Filter{Attributes: map[string]string{
											"source": "source3",
										}},
										DialectedFilter: []*contract.DialectedFilter{
											{Filter: &contract.DialectedFilter_Exact{
												Exact: &contract.Exact{
													Attributes: map[string]string{
														"source": "source3",
													},
												},
											}},
										},
									},
								}.build(useNewFilters),
							},
						},
					},
				}, env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat),
				BrokerDispatcherPod(env.SystemNamespace, nil),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     BrokerUUID + "a",
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							Egresses: []*contract.Egress{
								{
									Destination:   ServiceURL,
									ConsumerGroup: triggerConsumerGroup,
									Uid:           TriggerUUID,
								},
								{
									Destination:   "http://example.com/1",
									ConsumerGroup: "1",
									Uid:           "1",
								},
								EgressBuilder{
									&contract.Egress{
										Destination:   "http://example.com/2",
										ConsumerGroup: "2",
										Uid:           "2",
										Filter: &contract.Filter{Attributes: map[string]string{
											"source": "source2",
										}},
										DialectedFilter: []*contract.DialectedFilter{
											{Filter: &contract.DialectedFilter_Exact{
												Exact: &contract.Exact{
													Attributes: map[string]string{
														"source": "source2",
													},
												},
											}},
										},
									},
								}.build(useNewFilters),
								EgressBuilder{
									&contract.Egress{
										Destination:   "http://example.com/3",
										ConsumerGroup: "3",
										Uid:           "3",
										Filter: &contract.Filter{Attributes: map[string]string{
											"source": "source3",
										}},
										DialectedFilter: []*contract.DialectedFilter{
											{Filter: &contract.DialectedFilter_Exact{
												Exact: &contract.Exact{
													Attributes: map[string]string{
														"source": "source3",
													},
												},
											}},
										},
									},
								}.build(useNewFilters),
							},
						},
						{
							Uid:     BrokerUUID,
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							Egresses: []*contract.Egress{
								{
									Destination:   "http://example.com/1",
									ConsumerGroup: "1",
									Uid:           "1",
								},
								EgressBuilder{
									&contract.Egress{
										Destination:   "http://example.com/2",
										ConsumerGroup: "2",
										Uid:           "2",
										Filter: &contract.Filter{Attributes: map[string]string{
											"source": "source2",
										}},
										DialectedFilter: []*contract.DialectedFilter{
											{Filter: &contract.DialectedFilter_Exact{
												Exact: &contract.Exact{
													Attributes: map[string]string{
														"source": "source2",
													},
												},
											}},
										},
									},
								}.build(useNewFilters),
								EgressBuilder{
									&contract.Egress{
										Destination:   "http://example.com/3",
										ConsumerGroup: "3",
										Uid:           "3",
										Filter: &contract.Filter{Attributes: map[string]string{
											"source": "source3",
										}},
										DialectedFilter: []*contract.DialectedFilter{
											{Filter: &contract.DialectedFilter_Exact{
												Exact: &contract.Exact{
													Attributes: map[string]string{
														"source": "source3",
													},
												},
											}},
										},
									},
								}.build(useNewFilters),
								EgressBuilder{
									&contract.Egress{
										Destination:   ServiceURL,
										ConsumerGroup: triggerConsumerGroup,
										Uid:           TriggerUUID,
										Reference:     TriggerReference(),
										Filter: &contract.Filter{Attributes: map[string]string{
											"ext": "extval",
										}},
										DialectedFilter: []*contract.DialectedFilter{
											{Filter: &contract.DialectedFilter_Exact{
												Exact: &contract.Exact{
													Attributes: map[string]string{
														"ext": "extval",
													},
												},
											}},
										},
									},
								}.build(useNewFilters),
							},
						},
					},
					Generation: 1,
				}),
				BrokerDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						withFilters(
							map[string]string{"ext": "extval"},
							[]eventing.SubscriptionsAPIFilter{
								{
									Exact: map[string]string{
										"ext": "extval",
									},
								},
							},
							useNewFilters,
						),
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
						reconcilertesting.WithTriggerSubscribed(),
						withSubscriberURI,
						reconcilertesting.WithTriggerDependencyReady(),
						reconcilertesting.WithTriggerBrokerReady(),
						withTriggerSubscriberResolvedSucceeded(contract.DeliveryOrder_UNORDERED),
						withTriggerStatusGroupIdAnnotation(triggerConsumerGroup),
						reconcilertesting.WithTriggerDeadLetterSinkNotConfigured(),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - many Triggers - middle",
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
				NewService(),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     BrokerUUID + "a",
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							Egresses: []*contract.Egress{
								{
									Destination:   ServiceURL,
									ConsumerGroup: triggerConsumerGroup,
									Uid:           TriggerUUID,
								},
								{
									Destination:   "http://example.com/1",
									ConsumerGroup: "1",
									Uid:           "1",
								},
								EgressBuilder{
									&contract.Egress{
										Destination:   "http://example.com/2",
										ConsumerGroup: "2",
										Uid:           "2",
										Filter: &contract.Filter{Attributes: map[string]string{
											"source": "source2",
										}},
										DialectedFilter: []*contract.DialectedFilter{
											{Filter: &contract.DialectedFilter_Exact{
												Exact: &contract.Exact{
													Attributes: map[string]string{
														"source": "source2",
													},
												},
											}},
										},
									},
								}.build(useNewFilters),
								EgressBuilder{
									&contract.Egress{
										Destination:   "http://example.com/3",
										ConsumerGroup: "3",
										Uid:           "3",
										Filter: &contract.Filter{Attributes: map[string]string{
											"source": "source3",
										}},
										DialectedFilter: []*contract.DialectedFilter{
											{Filter: &contract.DialectedFilter_Exact{
												Exact: &contract.Exact{
													Attributes: map[string]string{
														"source": "source3",
													},
												},
											}},
										},
									},
								}.build(useNewFilters),
							},
						},
						{
							Uid:     BrokerUUID,
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							Egresses: []*contract.Egress{
								{
									Destination:   "http://example.com/1",
									ConsumerGroup: "1",
									Uid:           "1",
								},
								EgressBuilder{
									&contract.Egress{
										Destination:   "http://example.com/2",
										ConsumerGroup: "2",
										Uid:           "2",
										Filter: &contract.Filter{Attributes: map[string]string{
											"source": "source2",
										}},
										DialectedFilter: []*contract.DialectedFilter{
											{Filter: &contract.DialectedFilter_Exact{
												Exact: &contract.Exact{
													Attributes: map[string]string{
														"source": "source2",
													},
												},
											}},
										},
									},
								}.build(useNewFilters),
								EgressBuilder{
									&contract.Egress{
										Destination:   ServiceURL,
										ConsumerGroup: triggerConsumerGroup,
										Uid:           TriggerUUID,
										Filter: &contract.Filter{Attributes: map[string]string{
											"source": "source2",
										}},
										DialectedFilter: []*contract.DialectedFilter{
											{Filter: &contract.DialectedFilter_Exact{
												Exact: &contract.Exact{
													Attributes: map[string]string{
														"source": "source2",
													},
												},
											}},
										},
									},
								}.build(useNewFilters),
								EgressBuilder{
									&contract.Egress{
										Destination:   "http://example.com/3",
										ConsumerGroup: "3",
										Uid:           "3",
										Filter: &contract.Filter{Attributes: map[string]string{
											"source": "source3",
										}},
										DialectedFilter: []*contract.DialectedFilter{
											{Filter: &contract.DialectedFilter_Exact{
												Exact: &contract.Exact{
													Attributes: map[string]string{
														"source": "source3",
													},
												},
											}},
										},
									},
								}.build(useNewFilters),
							},
						},
						{
							Uid:     BrokerUUID + "a",
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							Egresses: []*contract.Egress{
								{
									Destination:   ServiceURL,
									ConsumerGroup: triggerConsumerGroup,
									Uid:           TriggerUUID,
								},
								{
									Destination:   "http://example.com/1",
									ConsumerGroup: "1",
									Uid:           "1",
								},
								EgressBuilder{
									&contract.Egress{
										Destination:   "http://example.com/2",
										ConsumerGroup: "2",
										Uid:           "2",
										Filter: &contract.Filter{Attributes: map[string]string{
											"source": "source2",
										}},
										DialectedFilter: []*contract.DialectedFilter{
											{Filter: &contract.DialectedFilter_Exact{
												Exact: &contract.Exact{
													Attributes: map[string]string{
														"source": "source2",
													},
												},
											}},
										},
									},
								}.build(useNewFilters),
								EgressBuilder{
									&contract.Egress{
										Destination:   "http://example.com/3",
										ConsumerGroup: "3",
										Uid:           "3",
										Filter: &contract.Filter{Attributes: map[string]string{
											"source": "source3",
										}},
										DialectedFilter: []*contract.DialectedFilter{
											{Filter: &contract.DialectedFilter_Exact{
												Exact: &contract.Exact{
													Attributes: map[string]string{
														"source": "source3",
													},
												},
											}},
										},
									},
								}.build(useNewFilters),
							},
						},
					},
				}, env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat),
				BrokerDispatcherPod(env.SystemNamespace, nil),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     BrokerUUID + "a",
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							Egresses: []*contract.Egress{
								{
									Destination:   ServiceURL,
									ConsumerGroup: triggerConsumerGroup,
									Uid:           TriggerUUID,
								},
								{
									Destination:   "http://example.com/1",
									ConsumerGroup: "1",
									Uid:           "1",
								},
								EgressBuilder{
									&contract.Egress{
										Destination:   "http://example.com/2",
										ConsumerGroup: "2",
										Uid:           "2",
										Filter: &contract.Filter{Attributes: map[string]string{
											"source": "source2",
										}},
										DialectedFilter: []*contract.DialectedFilter{
											{Filter: &contract.DialectedFilter_Exact{
												Exact: &contract.Exact{
													Attributes: map[string]string{
														"source": "source2",
													},
												},
											}},
										},
									},
								}.build(useNewFilters),
								EgressBuilder{
									&contract.Egress{
										Destination:   "http://example.com/3",
										ConsumerGroup: "3",
										Uid:           "3",
										Filter: &contract.Filter{Attributes: map[string]string{
											"source": "source3",
										}},
										DialectedFilter: []*contract.DialectedFilter{
											{Filter: &contract.DialectedFilter_Exact{
												Exact: &contract.Exact{
													Attributes: map[string]string{
														"source": "source3",
													},
												},
											}},
										},
									},
								}.build(useNewFilters),
							},
						},
						{
							Uid:     BrokerUUID,
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							Egresses: []*contract.Egress{
								{
									Destination:   "http://example.com/1",
									ConsumerGroup: "1",
									Uid:           "1",
								},
								EgressBuilder{
									&contract.Egress{
										Destination:   "http://example.com/2",
										ConsumerGroup: "2",
										Uid:           "2",
										Filter: &contract.Filter{Attributes: map[string]string{
											"source": "source2",
										}},
										DialectedFilter: []*contract.DialectedFilter{
											{Filter: &contract.DialectedFilter_Exact{
												Exact: &contract.Exact{
													Attributes: map[string]string{
														"source": "source2",
													},
												},
											}},
										},
									},
								}.build(useNewFilters),
								{
									Destination:   ServiceURL,
									ConsumerGroup: triggerConsumerGroup,
									Uid:           TriggerUUID,
									Reference:     TriggerReference(),
								},
								EgressBuilder{
									&contract.Egress{
										Destination:   "http://example.com/3",
										ConsumerGroup: "3",
										Uid:           "3",
										Filter: &contract.Filter{Attributes: map[string]string{
											"source": "source3",
										}},
										DialectedFilter: []*contract.DialectedFilter{
											{Filter: &contract.DialectedFilter_Exact{
												Exact: &contract.Exact{
													Attributes: map[string]string{
														"source": "source3",
													},
												},
											}},
										},
									},
								}.build(useNewFilters),
							},
						},
						{
							Uid:     BrokerUUID + "a",
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							Egresses: []*contract.Egress{
								{
									Destination:   ServiceURL,
									ConsumerGroup: triggerConsumerGroup,
									Uid:           TriggerUUID,
								},
								{
									Destination:   "http://example.com/1",
									ConsumerGroup: "1",
									Uid:           "1",
								},
								EgressBuilder{
									&contract.Egress{
										Destination:   "http://example.com/2",
										ConsumerGroup: "2",
										Uid:           "2",
										Filter: &contract.Filter{Attributes: map[string]string{
											"source": "source2",
										}},
										DialectedFilter: []*contract.DialectedFilter{
											{Filter: &contract.DialectedFilter_Exact{
												Exact: &contract.Exact{
													Attributes: map[string]string{
														"source": "source2",
													},
												},
											}},
										},
									},
								}.build(useNewFilters),
								EgressBuilder{
									&contract.Egress{
										Destination:   "http://example.com/3",
										ConsumerGroup: "3",
										Uid:           "3",
										Filter: &contract.Filter{Attributes: map[string]string{
											"source": "source3",
										}},
										DialectedFilter: []*contract.DialectedFilter{
											{Filter: &contract.DialectedFilter_Exact{
												Exact: &contract.Exact{
													Attributes: map[string]string{
														"source": "source3",
													},
												},
											}},
										},
									},
								}.build(useNewFilters),
							},
						},
					},
					Generation: 1,
				}),
				BrokerDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
						reconcilertesting.WithTriggerSubscribed(),
						withSubscriberURI,
						reconcilertesting.WithTriggerDependencyReady(),
						reconcilertesting.WithTriggerBrokerReady(),
						withTriggerSubscriberResolvedSucceeded(contract.DeliveryOrder_UNORDERED),
						withTriggerStatusGroupIdAnnotation(triggerConsumerGroup),
						reconcilertesting.WithTriggerDeadLetterSinkNotConfigured(),
					),
				},
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
			Name: "OIDC: creates OIDC service account",
			Ctx: feature.ToContext(context.Background(), feature.Flags{
				feature.OIDCAuthentication: feature.Enabled,
			}),
			Objects: []runtime.Object{
				NewBroker(
					BrokerReady,
					WithTopicStatusAnnotation(BrokerTopic()),
					WithBootstrapServerStatusAnnotation(bootstrapServers),
				),
				newTrigger(),
				NewService(),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     BrokerUUID,
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
						},
					},
				}, env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat),
				BrokerDispatcherPod(env.SystemNamespace, nil),
				DataPlaneConfigMap(env.DataPlaneConfigMapNamespace, env.DataPlaneConfigConfigMapName, brokerreconciler.ConsumerConfigKey,
					DataPlaneConfigInitialOffset(brokerreconciler.ConsumerConfigKey, sources.OffsetLatest),
				),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     BrokerUUID,
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							Egresses: []*contract.Egress{
								{
									Destination:            ServiceURL,
									ConsumerGroup:          triggerConsumerGroup,
									Uid:                    TriggerUUID,
									Reference:              TriggerReference(),
									OidcServiceAccountName: makeTriggerOIDCServiceAccount().Name,
								},
							},
						},
					},
					Generation: 1,
				}),
				BrokerDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerOIDCIdentityCreatedSucceeded(),
						reconcilertesting.WithTriggerOIDCServiceAccountName(makeTriggerOIDCServiceAccount().Name),
						reconcilertesting.WithTriggerSubscribed(),
						withSubscriberURI,
						reconcilertesting.WithTriggerDependencyReady(),
						reconcilertesting.WithTriggerBrokerReady(),
						withTriggerSubscriberResolvedSucceeded(contract.DeliveryOrder_UNORDERED),
						withTriggerStatusGroupIdAnnotation(triggerConsumerGroup),
						reconcilertesting.WithTriggerDeadLetterSinkNotConfigured(),
					),
				},
			},
			WantCreates: []runtime.Object{
				makeTriggerOIDCServiceAccount(),
			},
		},
		{
			Name: "OIDC: Trigger not ready on invalid OIDC service account",
			Ctx: feature.ToContext(context.Background(), feature.Flags{
				feature.OIDCAuthentication: feature.Enabled,
			}),
			Objects: []runtime.Object{
				NewBroker(
					BrokerReady,
					WithTopicStatusAnnotation(BrokerTopic()),
					WithBootstrapServerStatusAnnotation(bootstrapServers),
				),
				newTrigger(),
				makeTriggerOIDCServiceAccountWithoutOwnerRef(),
				NewService(),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     BrokerUUID,
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
						},
					},
				}, env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat),
				BrokerDispatcherPod(env.SystemNamespace, nil),
				DataPlaneConfigMap(env.DataPlaneConfigMapNamespace, env.DataPlaneConfigConfigMapName, brokerreconciler.ConsumerConfigKey,
					DataPlaneConfigInitialOffset(brokerreconciler.ConsumerConfigKey, sources.OffsetLatest),
				),
			},
			WantErr: true,
			Key:     testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
				Eventf(corev1.EventTypeWarning, "InternalError", fmt.Sprintf("service account %s not owned by Trigger %s", makeTriggerOIDCServiceAccountWithoutOwnerRef().Name, TriggerName)),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     BrokerUUID,
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							Egresses: []*contract.Egress{
								{
									Destination:            ServiceURL,
									ConsumerGroup:          triggerConsumerGroup,
									Uid:                    TriggerUUID,
									Reference:              TriggerReference(),
									OidcServiceAccountName: makeTriggerOIDCServiceAccount().Name,
								},
							},
						},
					},
					Generation: 1,
				}),
				BrokerDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerOIDCIdentityCreatedFailed("Unable to resolve service account for OIDC authentication", fmt.Sprintf("service account %s not owned by Trigger %s", makeTriggerOIDCServiceAccountWithoutOwnerRef().Name, TriggerName)),
						reconcilertesting.WithTriggerOIDCServiceAccountName(makeTriggerOIDCServiceAccountWithoutOwnerRef().Name),
						reconcilertesting.WithTriggerSubscribed(),
						withSubscriberURI,
						reconcilertesting.WithTriggerDependencyUnknown("", ""),
						reconcilertesting.WithTriggerBrokerReady(),
						withTriggerStatusGroupIdAnnotation(triggerConsumerGroup),
						reconcilertesting.WithTriggerDeadLetterSinkNotConfigured(),
						withTriggerSubscriberResolvedSucceeded(contract.DeliveryOrder_UNORDERED),
						reconcilertesting.WithTriggerSubscribedUnknown("", ""),
					),
				},
			},
		},
	}

	for i := range table {
		table[i].Name = table[i].Name + " - " + format
	}

	if useNewFilters {
		useTableWithFlags(t, table, &env, map[string]feature.Flag{feature.NewTriggerFilters: feature.Enabled})
	} else {
		useTable(t, table, &env)
	}
}

func withDelivery(trigger *eventing.Trigger) {
	trigger.Spec.Delivery = &eventingduck.DeliverySpec{
		DeadLetterSink: &duckv1.Destination{URI: url},
		Retry:          pointer.Int32(3),
		BackoffPolicy:  &exponential,
		BackoffDelay:   pointer.String("PT1S"),
		Timeout:        pointer.String("PT2S"),
	}
}

func withDeliveryTLS(trigger *eventing.Trigger) {
	trigger.Spec.Delivery = &eventingduck.DeliverySpec{
		DeadLetterSink: &duckv1.Destination{URI: url, CACerts: pointer.String(string(eventingtlstesting.CA))},
		Retry:          pointer.Int32(3),
		BackoffPolicy:  &exponential,
		BackoffDelay:   pointer.String("PT1S"),
		Timeout:        pointer.String("PT2S"),
	}
}

func TestTriggerFinalizer(t *testing.T) {

	t.Parallel()

	for _, f := range Formats {
		triggerFinalizer(t, f, *DefaultEnv)
	}

}

func triggerFinalizer(t *testing.T, format string, env config.Env) {

	testKey := fmt.Sprintf("%s/%s", TriggerNamespace, TriggerName)

	env.ContractConfigMapFormat = format

	table := TableTest{
		{
			Name: "Broker deleted, trigger in config map",
			Objects: []runtime.Object{
				newTrigger(),
				NewDeletedBroker(),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:    BrokerUUID,
							Topics: []string{BrokerTopic()},
							Egresses: []*contract.Egress{
								{
									Destination:   ServiceURL,
									ConsumerGroup: triggerConsumerGroup + "a",
									Uid:           TriggerUUID + "a",
								},
								{
									Destination:   ServiceURL,
									ConsumerGroup: triggerConsumerGroup,
									Uid:           TriggerUUID,
								},
							},
						},
					},
					Generation: 8,
				}, env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat),
				BrokerDispatcherPod(env.SystemNamespace, nil),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:    BrokerUUID,
							Topics: []string{BrokerTopic()},
							Egresses: []*contract.Egress{
								{
									Destination:   ServiceURL,
									ConsumerGroup: triggerConsumerGroup + "a",
									Uid:           TriggerUUID + "a",
								},
							},
						},
					},
					Generation: 9,
				}),
				BrokerDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "9",
				}),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
					),
				},
			},
		},
		{
			Name: "Broker deleted, no trigger in config map",
			Objects: []runtime.Object{
				newTrigger(),
				NewDeletedBroker(),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:    BrokerUUID,
							Topics: []string{BrokerTopic()},
						},
					},
					Generation: 8,
				}, env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
					),
				},
			},
		},
		{
			Name: "Broker deleted, no broker in config map",
			Objects: []runtime.Object{
				newTrigger(),
				NewDeletedBroker(),
				NewConfigMapFromContract(&contract.Contract{
					Generation: 8,
				}, env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - many Triggers - start",
			Objects: []runtime.Object{
				NewDeletedBroker(
					BrokerReady,
				),
				newTrigger(),
				NewService(),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     BrokerUUID,
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							Egresses: []*contract.Egress{
								{
									Destination:   ServiceURL,
									ConsumerGroup: triggerConsumerGroup,
									Uid:           TriggerUUID,
								},
								{
									Destination:   "http://example.com/1",
									ConsumerGroup: "1",
									Uid:           "1",
								},
								{
									Destination:   "http://example.com/2",
									ConsumerGroup: "2",
									Uid:           "2",
									Filter: &contract.Filter{Attributes: map[string]string{
										"source": "source2",
									}},
								},
								{
									Destination:   "http://example.com/3",
									ConsumerGroup: "3",
									Uid:           "3",
									Filter: &contract.Filter{Attributes: map[string]string{
										"source": "source3",
									}},
								},
							},
						},
						{
							Uid:     BrokerUUID + "a",
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							Egresses: []*contract.Egress{
								{
									Destination:   ServiceURL,
									ConsumerGroup: triggerConsumerGroup,
									Uid:           TriggerUUID,
								},
								{
									Destination:   "http://example.com/1",
									ConsumerGroup: "1",
									Uid:           "1",
								},
								{
									Destination:   "http://example.com/2",
									ConsumerGroup: "2",
									Uid:           "2",
									Filter: &contract.Filter{Attributes: map[string]string{
										"source": "source2",
									}},
								},
								{
									Destination:   "http://example.com/3",
									ConsumerGroup: "3",
									Uid:           "3",
									Filter: &contract.Filter{Attributes: map[string]string{
										"source": "source3",
									}},
								},
							},
						},
					},
				}, env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat),
				BrokerDispatcherPod(env.SystemNamespace, nil),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     BrokerUUID,
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							Egresses: []*contract.Egress{
								{
									Destination:   "http://example.com/3",
									ConsumerGroup: "3",
									Uid:           "3",
									Filter: &contract.Filter{Attributes: map[string]string{
										"source": "source3",
									}},
								},
								{
									Destination:   "http://example.com/1",
									ConsumerGroup: "1",
									Uid:           "1",
								},
								{
									Destination:   "http://example.com/2",
									ConsumerGroup: "2",
									Uid:           "2",
									Filter: &contract.Filter{Attributes: map[string]string{
										"source": "source2",
									}},
								},
							},
						},
						{
							Uid:     BrokerUUID + "a",
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							Egresses: []*contract.Egress{
								{
									Destination:   ServiceURL,
									ConsumerGroup: triggerConsumerGroup,
									Uid:           TriggerUUID,
								},
								{
									Destination:   "http://example.com/1",
									ConsumerGroup: "1",
									Uid:           "1",
								},
								{
									Destination:   "http://example.com/2",
									ConsumerGroup: "2",
									Uid:           "2",
									Filter: &contract.Filter{Attributes: map[string]string{
										"source": "source2",
									}},
								},
								{
									Destination:   "http://example.com/3",
									ConsumerGroup: "3",
									Uid:           "3",
									Filter: &contract.Filter{Attributes: map[string]string{
										"source": "source3",
									}},
								},
							},
						},
					},
					Generation: 1,
				}),
				BrokerDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - many Triggers - end",
			Objects: []runtime.Object{
				NewDeletedBroker(
					BrokerReady,
				),
				newTrigger(),
				NewService(),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     BrokerUUID + "a",
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							Egresses: []*contract.Egress{
								{
									Destination:   ServiceURL,
									ConsumerGroup: triggerConsumerGroup,
									Uid:           TriggerUUID,
								},
								{
									Destination:   "http://example.com/1",
									ConsumerGroup: "1",
									Uid:           "1",
								},
								{
									Destination:   "http://example.com/2",
									ConsumerGroup: "2",
									Uid:           "2",
									Filter: &contract.Filter{Attributes: map[string]string{
										"source": "source2",
									}},
								},
								{
									Destination:   "http://example.com/3",
									ConsumerGroup: "3",
									Uid:           "3",
									Filter: &contract.Filter{Attributes: map[string]string{
										"source": "source3",
									}},
								},
							},
						},
						{
							Uid:     BrokerUUID,
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							Egresses: []*contract.Egress{
								{
									Destination:   "http://example.com/1",
									ConsumerGroup: "1",
									Uid:           "1",
								},
								{
									Destination:   "http://example.com/2",
									ConsumerGroup: "2",
									Uid:           "2",
									Filter: &contract.Filter{Attributes: map[string]string{
										"source": "source2",
									}},
								},
								{
									Destination:   "http://example.com/3",
									ConsumerGroup: "3",
									Uid:           "3",
									Filter: &contract.Filter{Attributes: map[string]string{
										"source": "source3",
									}},
								},
								{
									Destination:   ServiceURL,
									ConsumerGroup: triggerConsumerGroup,
									Uid:           TriggerUUID,
								},
							},
						},
					},
				}, env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat),
				BrokerDispatcherPod(env.SystemNamespace, nil),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     BrokerUUID + "a",
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							Egresses: []*contract.Egress{
								{
									Destination:   ServiceURL,
									ConsumerGroup: triggerConsumerGroup,
									Uid:           TriggerUUID,
								},
								{
									Destination:   "http://example.com/1",
									ConsumerGroup: "1",
									Uid:           "1",
								},
								{
									Destination:   "http://example.com/2",
									ConsumerGroup: "2",
									Uid:           "2",
									Filter: &contract.Filter{Attributes: map[string]string{
										"source": "source2",
									}},
								},
								{
									Destination:   "http://example.com/3",
									ConsumerGroup: "3",
									Uid:           "3",
									Filter: &contract.Filter{Attributes: map[string]string{
										"source": "source3",
									}},
								},
							},
						},
						{
							Uid:     BrokerUUID,
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							Egresses: []*contract.Egress{
								{
									Destination:   "http://example.com/1",
									ConsumerGroup: "1",
									Uid:           "1",
								},
								{
									Destination:   "http://example.com/2",
									ConsumerGroup: "2",
									Uid:           "2",
									Filter: &contract.Filter{Attributes: map[string]string{
										"source": "source2",
									}},
								},
								{
									Destination:   "http://example.com/3",
									ConsumerGroup: "3",
									Uid:           "3",
									Filter: &contract.Filter{Attributes: map[string]string{
										"source": "source3",
									}},
								},
							},
						},
					},
					Generation: 1,
				}),
				BrokerDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - many Triggers - middle",
			Objects: []runtime.Object{
				NewDeletedBroker(
					BrokerReady,
				),
				newTrigger(),
				NewService(),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     BrokerUUID + "a",
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							Egresses: []*contract.Egress{
								{
									Destination:   ServiceURL,
									ConsumerGroup: triggerConsumerGroup,
									Uid:           TriggerUUID,
								},
								{
									Destination:   "http://example.com/1",
									ConsumerGroup: "1",
									Uid:           "1",
								},
								{
									Destination:   "http://example.com/2",
									ConsumerGroup: "2",
									Uid:           "2",
									Filter: &contract.Filter{Attributes: map[string]string{
										"source": "source2",
									}},
								},
								{
									Destination:   "http://example.com/3",
									ConsumerGroup: "3",
									Uid:           "3",
									Filter: &contract.Filter{Attributes: map[string]string{
										"source": "source3",
									}},
								},
							},
						},
						{
							Uid:     BrokerUUID,
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							Egresses: []*contract.Egress{
								{
									Destination:   "http://example.com/1",
									ConsumerGroup: "1",
									Uid:           "1",
								},
								{
									Destination:   "http://example.com/2",
									ConsumerGroup: "2",
									Uid:           "2",
									Filter: &contract.Filter{Attributes: map[string]string{
										"source": "source2",
									}},
								},
								{
									Destination:   ServiceURL,
									ConsumerGroup: triggerConsumerGroup,
									Uid:           TriggerUUID,
								},
								{
									Destination:   "http://example.com/3",
									ConsumerGroup: "3",
									Uid:           "3",
									Filter: &contract.Filter{Attributes: map[string]string{
										"source": "source3",
									}},
								},
							},
						},
						{
							Uid:     BrokerUUID + "a",
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							Egresses: []*contract.Egress{
								{
									Destination:   ServiceURL,
									ConsumerGroup: triggerConsumerGroup,
									Uid:           TriggerUUID,
								},
								{
									Destination:   "http://example.com/1",
									ConsumerGroup: "1",
									Uid:           "1",
								},
								{
									Destination:   "http://example.com/2",
									ConsumerGroup: "2",
									Uid:           "2",
									Filter: &contract.Filter{Attributes: map[string]string{
										"source": "source2",
									}},
								},
								{
									Destination:   "http://example.com/3",
									ConsumerGroup: "3",
									Uid:           "3",
									Filter: &contract.Filter{Attributes: map[string]string{
										"source": "source3",
									}},
								},
							},
						},
					},
				}, env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat),
				BrokerDispatcherPod(env.SystemNamespace, nil),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     BrokerUUID + "a",
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							Egresses: []*contract.Egress{
								{
									Destination:   ServiceURL,
									ConsumerGroup: triggerConsumerGroup,
									Uid:           TriggerUUID,
								},
								{
									Destination:   "http://example.com/1",
									ConsumerGroup: "1",
									Uid:           "1",
								},
								{
									Destination:   "http://example.com/2",
									ConsumerGroup: "2",
									Uid:           "2",
									Filter: &contract.Filter{Attributes: map[string]string{
										"source": "source2",
									}},
								},
								{
									Destination:   "http://example.com/3",
									ConsumerGroup: "3",
									Uid:           "3",
									Filter: &contract.Filter{Attributes: map[string]string{
										"source": "source3",
									}},
								},
							},
						},
						{
							Uid:     BrokerUUID,
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							Egresses: []*contract.Egress{
								{
									Destination:   "http://example.com/1",
									ConsumerGroup: "1",
									Uid:           "1",
								},
								{
									Destination:   "http://example.com/2",
									ConsumerGroup: "2",
									Uid:           "2",
									Filter: &contract.Filter{Attributes: map[string]string{
										"source": "source2",
									}},
								},
								{
									Destination:   "http://example.com/3",
									ConsumerGroup: "3",
									Uid:           "3",
									Filter: &contract.Filter{Attributes: map[string]string{
										"source": "source3",
									}},
								},
							},
						},
						{
							Uid:     BrokerUUID + "a",
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							Egresses: []*contract.Egress{
								{
									Destination:   ServiceURL,
									ConsumerGroup: triggerConsumerGroup,
									Uid:           TriggerUUID,
								},
								{
									Destination:   "http://example.com/1",
									ConsumerGroup: "1",
									Uid:           "1",
								},
								{
									Destination:   "http://example.com/2",
									ConsumerGroup: "2",
									Uid:           "2",
									Filter: &contract.Filter{Attributes: map[string]string{
										"source": "source2",
									}},
								},
								{
									Destination:   "http://example.com/3",
									ConsumerGroup: "3",
									Uid:           "3",
									Filter: &contract.Filter{Attributes: map[string]string{
										"source": "source3",
									}},
								},
							},
						},
					},
					Generation: 1,
				}),
				BrokerDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - With Subscriber CA Cert",
			Objects: []runtime.Object{
				NewBroker(
					BrokerReady,
					WithTopicStatusAnnotation(BrokerTopic()),
					WithBootstrapServerStatusAnnotation(bootstrapServers),
				),
				newTriggerWithCert(
					withDeliveryTLS,
					withDeadLetterSinkURIandCACert(url.String(), string(eventingtlstesting.CA)),
				),
				NewService(),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     BrokerUUID,
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
						},
					},
				}, env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat),
				BrokerDispatcherPod(env.SystemNamespace, nil),
				DataPlaneConfigMap(env.DataPlaneConfigMapNamespace, env.DataPlaneConfigConfigMapName, brokerreconciler.ConsumerConfigKey,
					DataPlaneConfigInitialOffset(brokerreconciler.ConsumerConfigKey, sources.OffsetLatest),
				),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     BrokerUUID,
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							Egresses: []*contract.Egress{
								{
									Destination:        ServiceURL,
									DestinationCACerts: string(eventingtlstesting.CA),
									ConsumerGroup:      triggerConsumerGroup,
									Uid:                TriggerUUID,
									Reference:          TriggerReference(),
									EgressConfig: &contract.EgressConfig{
										DeadLetter:        url.String(),
										DeadLetterCACerts: string(eventingtlstesting.CA),
										Retry:             3,
										BackoffPolicy:     contract.BackoffPolicy_Exponential,
										BackoffDelay:      uint64(time.Second.Milliseconds()),
										Timeout:           uint64((time.Second * 2).Milliseconds()),
									},
								},
							},
						},
					},
					Generation: 1,
				}),
				BrokerDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTriggerWithCert(
						withDeliveryTLS,
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerSubscribed(),
						withSubscriberURI,
						reconcilertesting.WithTriggerDependencyReady(),
						reconcilertesting.WithTriggerBrokerReady(),
						withTriggerSubscriberResolvedSucceeded(contract.DeliveryOrder_UNORDERED),
						withTriggerStatusGroupIdAnnotation(triggerConsumerGroup),
						reconcilertesting.WithTriggerDeadLetterSinkResolvedSucceeded(),
						reconcilertesting.WithTriggerOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
						reconcilertesting.WithTriggerStatusDeadLetterSinkURI(duckv1.Addressable{
							URL: &apis.URL{
								Scheme: "http",
								Host:   "localhost",
								Path:   "/path",
							},
							CACerts: pointer.String(string(eventingtlstesting.CA)),
						}),
						withDeadLetterSinkURIandCACert(url.String(), string(eventingtlstesting.CA)),
					),
				},
			},
		},
	}

	useTable(t, table, &env)
}

func useTable(t *testing.T, table TableTest, env *config.Env) {
	useTableWithFlags(t, table, env, nil)
}

func useTableWithFlags(t *testing.T, table TableTest, env *config.Env, flags feature.Flags) {

	table.Test(t, NewFactory(env, func(ctx context.Context, listers *Listers, env *config.Env, row *TableRow) controller.Reconciler {

		logger := logging.FromContext(ctx)
		ctxFlags := feature.FromContextOrDefaults(ctx)
		for k, v := range flags {
			ctxFlags[k] = v
		}

		var featureFlags *apisconfig.KafkaFeatureFlags
		if v, ok := row.OtherTestData[kafkaFeatureFlags]; ok {
			featureFlags = v.(*apisconfig.KafkaFeatureFlags)
		} else {
			featureFlags = apisconfig.DefaultFeaturesConfig()
		}

		reconciler := &Reconciler{
			Reconciler: &base.Reconciler{
				KubeClient:                   kubeclient.Get(ctx),
				PodLister:                    listers.GetPodLister(),
				SecretLister:                 listers.GetSecretLister(),
				DataPlaneConfigMapNamespace:  env.DataPlaneConfigMapNamespace,
				ContractConfigMapName:        env.ContractConfigMapName,
				ContractConfigMapFormat:      env.ContractConfigMapFormat,
				DataPlaneConfigConfigMapName: env.DataPlaneConfigConfigMapName,
				DataPlaneNamespace:           env.SystemNamespace,
				DispatcherLabel:              base.BrokerDispatcherLabel,
				ReceiverLabel:                base.BrokerReceiverLabel,
			},
			FlagsHolder: &FlagsHolder{
				Flags: ctxFlags,
			},
			BrokerLister:              listers.GetBrokerLister(),
			ConfigMapLister:           listers.GetConfigMapLister(),
			ServiceAccountLister:      listers.GetServiceAccountLister(),
			EventingClient:            eventingclient.Get(ctx),
			Resolver:                  nil,
			Env:                       env,
			BrokerClass:               kafka.BrokerClass,
			DataPlaneConfigMapLabeler: base.NoopConfigmapOption,
			KafkaFeatureFlags:         featureFlags,
			InitOffsetsFunc: func(ctx context.Context, kafkaClient sarama.Client, kafkaAdminClient sarama.ClusterAdmin, topics []string, consumerGroup string) (int32, error) {
				return 1, nil
			},
			NewKafkaClient: func(addrs []string, config *sarama.Config) (sarama.Client, error) {
				return &kafkatesting.MockKafkaClient{}, nil
			},
			NewKafkaClusterAdminClient: func(_ []string, _ *sarama.Config) (sarama.ClusterAdmin, error) {
				return &kafkatesting.MockKafkaClusterAdmin{
					ExpectedTopicName: BrokerTopic(),
					ExpectedTopics:    []string{BrokerTopic()},
					ExpectedTopicsMetadataOnDescribeTopics: []*sarama.TopicMetadata{
						{
							Err:        0,
							Name:       BrokerTopic(),
							IsInternal: false,
							Partitions: []*sarama.PartitionMetadata{{}},
						},
					},
					ExpectedConsumerGroups: []string{"e7185016-5d98-4b54-84e8-3b1cd4acc6b5"},
					ExpectedGroupDescriptionOnDescribeConsumerGroups: []*sarama.GroupDescription{
						{
							GroupId: "e7185016-5d98-4b54-84e8-3b1cd4acc6b5",
							State:   "Stable",
						},
					},
					T: t,
				}, nil
			},
		}

		reconciler.Resolver = resolver.NewURIResolverFromTracker(ctx, tracker.New(func(name types.NamespacedName) {}, 0))

		return triggerreconciler.NewReconciler(
			ctx,
			logger,
			eventingclient.Get(ctx),
			listers.GetTriggerLister(),
			controller.GetEventRecorder(ctx),
			reconciler,
			controller.Options{
				FinalizerName:     FinalizerName,
				AgentName:         ControllerAgentName,
				SkipStatusUpdates: false,
			},
		)
	}))
}

func newTrigger(options ...reconcilertesting.TriggerOption) runtime.Object {
	return reconcilertesting.NewTrigger(
		TriggerName,
		TriggerNamespace,
		BrokerName,
		append(
			options,
			reconcilertesting.WithTriggerSubscriberURI(ServiceURL),
			func(t *eventing.Trigger) {
				t.UID = TriggerUUID
			},
		)...,
	)
}

func makeTriggerOIDCServiceAccount() *corev1.ServiceAccount {
	return auth.GetOIDCServiceAccountForResource(v1.SchemeGroupVersion.WithKind("Trigger"), metav1.ObjectMeta{
		Name:      TriggerName,
		Namespace: TriggerNamespace,
		UID:       TriggerUUID,
	})
}

func makeTriggerOIDCServiceAccountWithoutOwnerRef() *corev1.ServiceAccount {
	sa := auth.GetOIDCServiceAccountForResource(v1.SchemeGroupVersion.WithKind("Trigger"), metav1.ObjectMeta{
		Name:      TriggerName,
		Namespace: TriggerNamespace,
		UID:       TriggerUUID,
	})
	sa.OwnerReferences = nil

	return sa
}
func newTriggerWithCert(options ...reconcilertesting.TriggerOption) runtime.Object {
	return reconcilertesting.NewTrigger(
		TriggerName,
		TriggerNamespace,
		BrokerName,
		append(
			options,
			WithTriggerSubscriberURIAndCert(ServiceURL),
			func(t *eventing.Trigger) {
				t.UID = TriggerUUID
			},
		)...,
	)
}

func WithTriggerSubscriberURIAndCert(rawurl string) reconcilertesting.TriggerOption {
	uri, _ := apis.ParseURL(rawurl)
	return func(t *v1.Trigger) {
		t.Spec.Subscriber = duckv1.Destination{
			URI:     uri,
			CACerts: pointer.String(string(eventingtlstesting.CA)),
		}
	}
}

func withFilters(attributes eventing.TriggerFilterAttributes, newFilters []eventing.SubscriptionsAPIFilter, withNewFilters bool) func(*eventing.Trigger) {
	return func(e *eventing.Trigger) {
		if withNewFilters {
			e.Spec.Filters = newFilters
		} else {
			e.Spec.Filter = &eventing.TriggerFilter{
				Attributes: attributes,
			}
		}
	}
}

func withSubscriberURI(trigger *eventing.Trigger) {
	u, err := apis.ParseURL(ServiceURL)
	if err != nil {
		panic(err)
	}
	trigger.Status.SubscriberURI = u
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

func withDeadLetterSinkURIandCACert(uri string, cert string) func(trigger *eventing.Trigger) {
	return func(trigger *eventing.Trigger) {
		u, err := apis.ParseURL(uri)
		if err != nil {
			panic(err)
		}
		trigger.Status.DeadLetterSinkURI = u
		trigger.Status.DeadLetterSinkCACerts = pointer.String(string(eventingtlstesting.CA))
		trigger.Status.MarkDeadLetterSinkResolvedSucceeded()
	}
}

func withTriggerSubscriberResolvedSucceeded(deliveryOrder contract.DeliveryOrder) func(*eventing.Trigger) {
	return func(t *eventing.Trigger) {
		t.GetConditionSet().Manage(&t.Status).MarkTrueWithReason(
			eventing.TriggerConditionSubscriberResolved,
			string(eventing.TriggerConditionSubscriberResolved),
			fmt.Sprintf("Subscriber will receive events with the delivery order: %s", deliveryOrder.String()),
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

func Test_deleteTrigger(t *testing.T) {
	type args struct {
		triggers []*contract.Egress
		index    int
	}
	tests := []struct {
		name string
		args args
		want []*contract.Egress
	}{
		{
			name: "first element - alone",
			args: args{
				triggers: []*contract.Egress{
					{
						ConsumerGroup: "123",
					},
				},
				index: 0,
			},
			want: nil,
		},
		{
			name: "first element - with others",
			args: args{
				triggers: []*contract.Egress{
					{
						ConsumerGroup: "123",
					},
					{
						ConsumerGroup: "1234",
					},
				},
				index: 0,
			},
			want: []*contract.Egress{
				{
					ConsumerGroup: "1234",
				},
			},
		},
		{
			name: "last element - with others",
			args: args{
				triggers: []*contract.Egress{
					{
						ConsumerGroup: "1",
					},
					{
						ConsumerGroup: "2",
					},
					{
						ConsumerGroup: "3",
					},
					{
						ConsumerGroup: "4",
					},
				},
				index: 3,
			},
			want: []*contract.Egress{
				{
					ConsumerGroup: "1",
				},
				{
					ConsumerGroup: "2",
				},
				{
					ConsumerGroup: "3",
				},
			},
		},
		{
			name: "middle element - with others",
			args: args{
				triggers: []*contract.Egress{
					{
						ConsumerGroup: "1",
					},
					{
						ConsumerGroup: "2",
					},
					{
						ConsumerGroup: "3",
					},
					{
						ConsumerGroup: "4",
					},
					{
						ConsumerGroup: "5",
					},
				},
				index: 2,
			},
			want: []*contract.Egress{
				{
					ConsumerGroup: "1",
				},
				{
					ConsumerGroup: "2",
				},
				{
					ConsumerGroup: "5",
				},
				{
					ConsumerGroup: "4",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := deleteTrigger(tt.args.triggers, tt.args.index)
			if diff := cmp.Diff(tt.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("deleteTrigger() = %v, want %v (-want +got) %s", got, tt.want, diff)
			}
		})
	}
}
