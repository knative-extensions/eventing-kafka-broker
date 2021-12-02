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

	"google.golang.org/protobuf/testing/protocmp"
	"k8s.io/utils/pointer"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/tracker"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgotesting "k8s.io/client-go/testing"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1"
	fakeeventingclient "knative.dev/eventing/pkg/client/injection/client/fake"
	triggerreconciler "knative.dev/eventing/pkg/client/injection/reconciler/eventing/v1/trigger"
	reconcilertesting "knative.dev/eventing/pkg/reconciler/testing/v1"
	"knative.dev/pkg/apis"
	kubeclient "knative.dev/pkg/client/injection/kube/client/fake"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	. "knative.dev/pkg/reconciler/testing"
	"knative.dev/pkg/resolver"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/receiver"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	. "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/testing"

	consumergroupclient "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/client/fake"
)

const (
	// name of the trigger under test
	triggerName = "test-trigger"
	// namespace of the trigger under test
	triggerNamespace = "test-namespace"
)

var DefaultEnv = &config.Env{
	DataPlaneConfigMapNamespace: "knative-eventing",
	DataPlaneConfigMapName:      "kafka-broker-brokers-triggers",
	GeneralConfigMapName:        "kafka-broker-config",
	IngressName:                 "kafka-broker-ingress",
	SystemNamespace:             "knative-eventing",
	DataPlaneConfigFormat:       base.Json,
	DefaultBackoffDelayMs:       1000,
}

var (
	finalizerUpdatedEvent = Eventf(
		corev1.EventTypeNormal,
		"FinalizerUpdate",
		fmt.Sprintf(`Updated %q finalizers`, triggerName),
	)

	url = &apis.URL{
		Scheme: "http",
		Host:   "localhost",
		Path:   "/path",
	}

	exponential = eventingduck.BackoffPolicyExponential
)

func TestTriggerReconciler(t *testing.T) {
	eventing.RegisterAlternateBrokerConditionSet(base.IngressConditionSet)

	t.Parallel()

	for _, f := range Formats {
		triggerReconciliation(t, f, *DefaultEnv)
	}
}

func triggerReconciliation(t *testing.T, format string, env config.Env) {

	testKey := fmt.Sprintf("%s/%s", triggerNamespace, triggerName)

	env.DataPlaneConfigFormat = format

	table := TableTest{
		{
			Name: "Reconciled normal - no existing Trigger",
			Objects: []runtime.Object{
				NewBroker(
					BrokerReady,
				),
				newTrigger(),
				NewService(),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     BrokerUUID,
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
						},
					},
				}, &env),
				BrokerDispatcherPod(env.SystemNamespace, nil),
				NewConsumerGroup(),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     BrokerUUID,
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
							Egresses: []*contract.Egress{
								{
									Destination:   ServiceURL,
									ConsumerGroup: TriggerUUID,
									Uid:           TriggerUUID,
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
						reconcilertesting.WithTriggerSubscribed(),
						withSubscriberURI,
						reconcilertesting.WithTriggerDependencyReady(),
						reconcilertesting.WithTriggerBrokerReady(),
						withTriggerSubscriberResolvedSucceeded(contract.DeliveryOrder_UNORDERED),
						reconcilertesting.WithTriggerDeadLetterSinkNotConfigured(),
						WithTriggerConsumerGroupReady,
					),
				},
			},
		},
	}

	for i := range table {
		table[i].Name = table[i].Name + " - " + format
	}

	useTable(t, table, &env)
}

func withDelivery(trigger *eventing.Trigger) {
	trigger.Spec.Delivery = &eventingduck.DeliverySpec{
		DeadLetterSink: &duckv1.Destination{URI: url},
		Retry:          pointer.Int32Ptr(3),
		BackoffPolicy:  &exponential,
		BackoffDelay:   pointer.StringPtr("PT1S"),
		Timeout:        pointer.StringPtr("PT2S"),
	}
}

func triggerFinalizer(t *testing.T, format string, env config.Env) {

	testKey := fmt.Sprintf("%s/%s", triggerNamespace, triggerName)

	env.DataPlaneConfigFormat = format

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
									ConsumerGroup: TriggerUUID + "a",
									Uid:           TriggerUUID + "a",
								},
								{
									Destination:   ServiceURL,
									ConsumerGroup: TriggerUUID,
									Uid:           TriggerUUID,
								},
							},
						},
					},
					Generation: 8,
				}, &env),
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
				ConfigMapUpdate(&env, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:    BrokerUUID,
							Topics: []string{BrokerTopic()},
							Egresses: []*contract.Egress{
								{
									Destination:   ServiceURL,
									ConsumerGroup: TriggerUUID + "a",
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
				}, &env),
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
				}, &env),
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
							Ingress: &contract.Ingress{IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
							Egresses: []*contract.Egress{
								{
									Destination:   ServiceURL,
									ConsumerGroup: TriggerUUID,
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
							Ingress: &contract.Ingress{IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
							Egresses: []*contract.Egress{
								{
									Destination:   ServiceURL,
									ConsumerGroup: TriggerUUID,
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
				}, &env),
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
				ConfigMapUpdate(&env, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     BrokerUUID,
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
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
							Ingress: &contract.Ingress{IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
							Egresses: []*contract.Egress{
								{
									Destination:   ServiceURL,
									ConsumerGroup: TriggerUUID,
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
							Ingress: &contract.Ingress{IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
							Egresses: []*contract.Egress{
								{
									Destination:   ServiceURL,
									ConsumerGroup: TriggerUUID,
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
							Ingress: &contract.Ingress{IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
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
									ConsumerGroup: TriggerUUID,
									Uid:           TriggerUUID,
								},
							},
						},
					},
				}, &env),
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
				ConfigMapUpdate(&env, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     BrokerUUID + "a",
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
							Egresses: []*contract.Egress{
								{
									Destination:   ServiceURL,
									ConsumerGroup: TriggerUUID,
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
							Ingress: &contract.Ingress{IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
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
							Ingress: &contract.Ingress{IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
							Egresses: []*contract.Egress{
								{
									Destination:   ServiceURL,
									ConsumerGroup: TriggerUUID,
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
							Ingress: &contract.Ingress{IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
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
									ConsumerGroup: TriggerUUID,
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
							Ingress: &contract.Ingress{IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
							Egresses: []*contract.Egress{
								{
									Destination:   ServiceURL,
									ConsumerGroup: TriggerUUID,
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
				}, &env),
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
				ConfigMapUpdate(&env, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     BrokerUUID + "a",
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
							Egresses: []*contract.Egress{
								{
									Destination:   ServiceURL,
									ConsumerGroup: TriggerUUID,
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
							Ingress: &contract.Ingress{IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
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
							Ingress: &contract.Ingress{IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
							Egresses: []*contract.Egress{
								{
									Destination:   ServiceURL,
									ConsumerGroup: TriggerUUID,
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
						WithTriggerConsumerGroupReady,
					),
				},
			},
		},
	}

	useTable(t, table, &env)
}

func useTable(t *testing.T, table TableTest, env *config.Env) {

	table.Test(t, NewFactory(env, func(ctx context.Context, listers *Listers, env *config.Env, row *TableRow) controller.Reconciler {

		logger := logging.FromContext(ctx)

		reconciler := &Reconciler{
			Reconciler: &base.Reconciler{
				KubeClient:                  kubeclient.Get(ctx),
				PodLister:                   listers.GetPodLister(),
				SecretLister:                listers.GetSecretLister(),
				DataPlaneConfigMapNamespace: env.DataPlaneConfigMapNamespace,
				DataPlaneConfigMapName:      env.DataPlaneConfigMapName,
				DataPlaneConfigFormat:       env.DataPlaneConfigFormat,
				SystemNamespace:             env.SystemNamespace,
				DispatcherLabel:             base.BrokerDispatcherLabel,
				ReceiverLabel:               base.BrokerReceiverLabel,
			},
			BrokerLister:        listers.GetBrokerLister(),
			EventingClient:      fakeeventingclient.Get(ctx),
			Resolver:            nil,
			Env:                 env,
			ConsumerGroupLister: listers.GetConsumerGroupLister(),
			InternalsClient:     consumergroupclient.Get(ctx),
		}

		reconciler.Resolver = resolver.NewURIResolverFromTracker(ctx, tracker.New(func(name types.NamespacedName) {}, 0))

		return triggerreconciler.NewReconciler(
			ctx,
			logger,
			fakeeventingclient.Get(ctx),
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
		triggerName,
		triggerNamespace,
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

func withAttributes(attributes eventing.TriggerFilterAttributes) func(*eventing.Trigger) {
	return func(e *eventing.Trigger) {
		e.Spec.Filter = &eventing.TriggerFilter{
			Attributes: attributes,
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

func withTriggerSubscriberResolvedSucceeded(deliveryOrder contract.DeliveryOrder) func(*eventing.Trigger) {
	return func(t *eventing.Trigger) {
		t.GetConditionSet().Manage(&t.Status).MarkTrueWithReason(
			eventing.TriggerConditionSubscriberResolved,
			string(eventing.TriggerConditionSubscriberResolved),
			fmt.Sprintf("Subscriber will receive events with the delivery order: %s", deliveryOrder.String()),
		)
	}
}

// WithTriggerConsumerGroupReady initializes the Triggers's conditions.
func WithTriggerConsumerGroupReady(t *eventing.Trigger) {
	t.GetConditionSet().Manage(&t.Status).MarkTrue(
		ConditionConsumerGroup,
	)
}

// WithTriggerConsumerGroupFailed marks the ConsumerGroup as failed
func WithTriggerConsumerGroupFailed(t *eventing.Trigger) {
	t.GetConditionSet().Manage(&t.Status).MarkFalse(
		ConditionConsumerGroup,
		"",
		"",
	)
}

func patchFinalizers() clientgotesting.PatchActionImpl {
	action := clientgotesting.PatchActionImpl{}
	action.Name = triggerName
	action.Namespace = triggerNamespace
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
