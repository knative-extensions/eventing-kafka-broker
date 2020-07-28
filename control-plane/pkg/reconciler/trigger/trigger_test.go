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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgotesting "k8s.io/client-go/testing"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1beta1"
	fakeeventingclient "knative.dev/eventing/pkg/client/injection/client/fake"
	triggerreconciler "knative.dev/eventing/pkg/client/injection/reconciler/eventing/v1beta1/trigger"
	"knative.dev/eventing/pkg/logging"
	reconcilertesting "knative.dev/eventing/pkg/reconciler/testing/v1beta1"
	kubeclient "knative.dev/pkg/client/injection/kube/client/fake"
	"knative.dev/pkg/controller"
	. "knative.dev/pkg/reconciler/testing"
	"knative.dev/pkg/resolver"

	coreconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/core/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/broker"
	. "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/testing"
)

const (
	// name of the trigger under test
	triggerName = "test-trigger"
	// namespace of the trigger under test
	triggerNamespace = "test-namespace"
)

var (
	finalizerUpdatedEvent = Eventf(
		corev1.EventTypeNormal,
		"FinalizerUpdate",
		fmt.Sprintf(`Updated %q finalizers`, triggerName),
	)
)

func TestTriggerReconciler(t *testing.T) {
	eventing.RegisterAlternateBrokerConditionSet(broker.ConditionSet)

	t.Parallel()

	for _, f := range Formats {
		triggerReconciliation(t, f, *DefaultConfigs)
	}
}

func triggerReconciliation(t *testing.T, format string, configs broker.Configs) {

	testKey := fmt.Sprintf("%s/%s", triggerNamespace, triggerName)

	configs.DataPlaneConfigFormat = format

	table := TableTest{
		{
			Name: "Reconciled normal - no existing Trigger",
			Objects: []runtime.Object{
				NewBroker(
					BrokerReady,
				),
				newTrigger(),
				NewService(),
				NewConfigMapFromBrokers(&coreconfig.Brokers{
					Broker: []*coreconfig.Broker{
						{
							Id:        BrokerUUID,
							Topic:     GetTopic(),
							Namespace: BrokerNamespace,
							Name:      BrokerName,
						},
					},
				}, &configs),
				NewDispatcherPod(configs.SystemNamespace, nil),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &coreconfig.Brokers{
					Broker: []*coreconfig.Broker{
						{
							Id:        BrokerUUID,
							Topic:     GetTopic(),
							Namespace: BrokerNamespace,
							Name:      BrokerName,
							Triggers: []*coreconfig.Trigger{
								{
									Destination: ServiceURL,
									Id:          TriggerUUID,
								},
							},
						},
					},
					VolumeGeneration: 1,
				}),
				DispatcherPodUpdate(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerSubscribed(),
						reconcilertesting.WithTriggerDependencyReady(),
						reconcilertesting.WithTriggerBrokerReady(),
						reconcilertesting.WithTriggerSubscriberResolvedSucceeded(),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - with existing Triggers and Brokers",
			Objects: []runtime.Object{
				NewBroker(
					BrokerReady,
				),
				newTrigger(),
				NewService(),
				NewConfigMapFromBrokers(&coreconfig.Brokers{
					Broker: []*coreconfig.Broker{
						{
							Id:        BrokerUUID + "z",
							Topic:     GetTopic(),
							Namespace: BrokerNamespace,
							Triggers: []*coreconfig.Trigger{
								{
									Destination: ServiceURL,
									Id:          TriggerUUID,
								},
							},
							Name: BrokerName,
						},
						{
							Id:        BrokerUUID,
							Topic:     GetTopic(),
							Namespace: BrokerNamespace,
							Triggers: []*coreconfig.Trigger{
								{
									Attributes: map[string]string{
										"source": "source_value",
									},
									Destination: ServiceURL,
									Id:          TriggerUUID + "a",
								},
								{
									Attributes: map[string]string{
										"source": "source_value",
									},
									Destination: ServiceURL,
									Id:          TriggerUUID,
								},
							},
							Name: BrokerName,
						},
					},
					VolumeGeneration: 2,
				}, &configs),
				NewDispatcherPod(configs.SystemNamespace, nil),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &coreconfig.Brokers{
					Broker: []*coreconfig.Broker{
						{
							Id:        BrokerUUID + "z",
							Topic:     GetTopic(),
							Namespace: BrokerNamespace,
							Triggers: []*coreconfig.Trigger{
								{
									Destination: ServiceURL,
									Id:          TriggerUUID,
								},
							},
							Name: BrokerName,
						},
						{
							Id:        BrokerUUID,
							Topic:     GetTopic(),
							Namespace: BrokerNamespace,
							Triggers: []*coreconfig.Trigger{
								{
									Attributes: map[string]string{
										"source": "source_value",
									},
									Destination: ServiceURL,
									Id:          TriggerUUID + "a",
								},
								{
									Destination: ServiceURL,
									Id:          TriggerUUID,
								},
							},
							Name: BrokerName,
						},
					},
					VolumeGeneration: 3,
				}),
				DispatcherPodUpdate(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "3",
				}),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerSubscribed(),
						reconcilertesting.WithTriggerDependencyReady(),
						reconcilertesting.WithTriggerBrokerReady(),
						reconcilertesting.WithTriggerSubscriberResolvedSucceeded(),
					),
				},
			},
		},
		{
			Name: "Config map not found - broker not found in config map",
			Objects: []runtime.Object{
				NewBroker(
					BrokerReady,
				),
				newTrigger(),
				NewService(),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			Key:     testKey,
			WantErr: true,
			SkipNamespaceValidation: true, // WantCreates compare the broker namespace with configmap namespace, so skip it
			WantCreates: []runtime.Object{
				NewConfigMap(&configs, nil),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
				Eventf(
					corev1.EventTypeWarning,
					"InternalError",
					fmt.Sprintf(
						"broker not found in data plane config map %s",
						configs.DataPlaneConfigMapAsString(),
					),
				),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerBrokerFailed(
							"Broker not found in data plane map",
							fmt.Sprintf("config map: %s", configs.DataPlaneConfigMapAsString()),
						),
					),
				},
			},
		},
		{
			Name: "Empty data plane config map",
			Objects: []runtime.Object{
				NewBroker(BrokerReady),
				newTrigger(),
				NewService(),
				NewConfigMap(&configs, nil),
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
						configs.DataPlaneConfigMapAsString(),
					),
				),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerBrokerFailed(
							"Broker not found in data plane map",
							fmt.Sprintf("config map: %s", configs.DataPlaneConfigMapAsString()),
						),
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
					),
				},
			},
		},
		{
			Name: "Broker deleted, no broker in config map",
			Objects: []runtime.Object{
				newTrigger(),
				NewDeletedBroker(),
				NewConfigMapFromBrokers(&coreconfig.Brokers{
					VolumeGeneration: 8,
				}, &configs),
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
			Name: "Broker deleted, no trigger in config map",
			Objects: []runtime.Object{
				newTrigger(),
				NewDeletedBroker(),
				NewConfigMapFromBrokers(&coreconfig.Brokers{
					Broker: []*coreconfig.Broker{
						{
							Id:    BrokerUUID,
							Topic: GetTopic(),
						},
					},
					VolumeGeneration: 8,
				}, &configs),
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
			Name: "Broker deleted, trigger in config map",
			Objects: []runtime.Object{
				newTrigger(),
				NewDeletedBroker(),
				NewConfigMapFromBrokers(&coreconfig.Brokers{
					Broker: []*coreconfig.Broker{
						{
							Id:    BrokerUUID,
							Topic: GetTopic(),
							Triggers: []*coreconfig.Trigger{
								{
									Destination: ServiceURL,
									Id:          TriggerUUID + "a",
								},
								{
									Destination: ServiceURL,
									Id:          TriggerUUID,
								},
							},
						},
					},
					VolumeGeneration: 8,
				}, &configs),
				NewDispatcherPod(configs.SystemNamespace, nil),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &coreconfig.Brokers{
					Broker: []*coreconfig.Broker{
						{
							Id:    BrokerUUID,
							Topic: GetTopic(),
							Triggers: []*coreconfig.Trigger{
								{
									Destination: ServiceURL,
									Id:          TriggerUUID + "a",
								},
							},
						},
					},
					VolumeGeneration: 9,
				}),
				DispatcherPodUpdate(configs.SystemNamespace, map[string]string{
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
	}

	for i := range table {
		table[i].Name = table[i].Name + " - " + format
	}

	useTable(t, table, &configs)
}

func TestTriggerFinalizer(t *testing.T) {

	t.Parallel()

	for _, f := range Formats {
		triggerFinalizer(t, f, *DefaultConfigs)
	}

}

func triggerFinalizer(t *testing.T, format string, configs broker.Configs) {

	testKey := fmt.Sprintf("%s/%s", triggerNamespace, triggerName)

	configs.DataPlaneConfigFormat = format

	table := TableTest{
		{
			Name: "Broker deleted, trigger in config map",
			Objects: []runtime.Object{
				newTrigger(),
				NewDeletedBroker(),
				NewConfigMapFromBrokers(&coreconfig.Brokers{
					Broker: []*coreconfig.Broker{
						{
							Id:    BrokerUUID,
							Topic: GetTopic(),
							Triggers: []*coreconfig.Trigger{
								{
									Destination: ServiceURL,
									Id:          TriggerUUID + "a",
								},
								{
									Destination: ServiceURL,
									Id:          TriggerUUID,
								},
							},
						},
					},
					VolumeGeneration: 8,
				}, &configs),
				NewDispatcherPod(configs.SystemNamespace, nil),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &coreconfig.Brokers{
					Broker: []*coreconfig.Broker{
						{
							Id:    BrokerUUID,
							Topic: GetTopic(),
							Triggers: []*coreconfig.Trigger{
								{
									Destination: ServiceURL,
									Id:          TriggerUUID + "a",
								},
							},
						},
					},
					VolumeGeneration: 9,
				}),
				DispatcherPodUpdate(configs.SystemNamespace, map[string]string{
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
				NewConfigMapFromBrokers(&coreconfig.Brokers{
					Broker: []*coreconfig.Broker{
						{
							Id:    BrokerUUID,
							Topic: GetTopic(),
						},
					},
					VolumeGeneration: 8,
				}, &configs),
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
				NewConfigMapFromBrokers(&coreconfig.Brokers{
					VolumeGeneration: 8,
				}, &configs),
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
	}

	useTable(t, table, &configs)
}

func useTable(t *testing.T, table TableTest, configs *broker.Configs) {

	table.Test(t, NewFactory(configs, func(ctx context.Context, listers *Listers, configs *broker.Configs, row *TableRow) controller.Reconciler {

		logger := logging.FromContext(ctx)

		reconciler := &Reconciler{
			Reconciler: &base.Reconciler{
				KubeClient:                  kubeclient.Get(ctx),
				PodLister:                   listers.GetPodLister(),
				DataPlaneConfigMapNamespace: configs.DataPlaneConfigMapNamespace,
				DataPlaneConfigMapName:      configs.DataPlaneConfigMapName,
				DataPlaneConfigFormat:       configs.DataPlaneConfigFormat,
				SystemNamespace:             configs.SystemNamespace,
			},
			BrokerLister: listers.GetBrokerLister(),
			Configs:      &configs.EnvConfigs,
		}

		reconciler.Resolver = resolver.NewURIResolver(ctx, func(name types.NamespacedName) {})

		return triggerreconciler.NewReconciler(
			ctx,
			logger.Sugar(),
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

func patchFinalizers() clientgotesting.PatchActionImpl {
	action := clientgotesting.PatchActionImpl{}
	action.Name = triggerName
	action.Namespace = triggerNamespace
	patch := `{"metadata":{"finalizers":["` + FinalizerName + `"],"resourceVersion":""}}`
	action.Patch = []byte(patch)
	return action
}
