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

	internals "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgotesting "k8s.io/client-go/testing"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1"
	eventingclient "knative.dev/eventing/pkg/client/injection/client/fake"
	triggerreconciler "knative.dev/eventing/pkg/client/injection/reconciler/eventing/v1/trigger"
	reconcilertesting "knative.dev/eventing/pkg/reconciler/testing/v1"
	"knative.dev/pkg/apis"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/kmeta"
	"knative.dev/pkg/logging"
	. "knative.dev/pkg/reconciler/testing"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	. "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/testing"

	fakeconsumergroupinformer "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/client/fake"
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

func TestReconcileKind(t *testing.T) {

	testKey := fmt.Sprintf("%s/%s", triggerNamespace, triggerName)

	table := TableTest{
		{
			Name: "Reconciled normal",
			Objects: []runtime.Object{
				NewBroker(
					BrokerReady,
				),
				newTrigger(),
			},
			Key: testKey,
			WantCreates: []runtime.Object{
				NewConsumerGroup(
					WithConsumerGroupName(TriggerUUID),
					WithConsumerGroupNamespace(triggerNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(newTrigger())),
					WithConsumerGroupLabels(nil),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(),
						ConsumerConfigs(
							ConsumerGroupIdConfig(TriggerUUID),
						),
						ConsumerDelivery(NewConsumerSpecDelivery(internals.Ordered)),
						ConsumerFilters(NewConsumerSpecFilters()),
					)),
				),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerBrokerReady(),
						withTriggerSubscriberResolvedSucceeded(),
						reconcilertesting.WithTriggerDependencyReady(),
						withDeadLetterSinkURI(""),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - Trigger with ordered delivery",
			Objects: []runtime.Object{
				NewBroker(
					BrokerReady,
				),
				newTrigger(reconcilertesting.WithAnnotation(deliveryOrderAnnotation, string(internals.Ordered))),
			},
			Key: testKey,
			WantCreates: []runtime.Object{
				NewConsumerGroup(
					WithConsumerGroupName(TriggerUUID),
					WithConsumerGroupNamespace(triggerNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(newTrigger())),
					WithConsumerGroupLabels(nil),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(),
						ConsumerConfigs(
							ConsumerGroupIdConfig(TriggerUUID),
						),
						ConsumerDelivery(NewConsumerSpecDelivery(internals.Ordered)),
						ConsumerFilters(NewConsumerSpecFilters()),
					)),
				),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerBrokerReady(),
						withTriggerSubscriberResolvedSucceeded(),
						reconcilertesting.WithTriggerDependencyReady(),
						withDeadLetterSinkURI(""),
						reconcilertesting.WithAnnotation(deliveryOrderAnnotation, string(internals.Ordered)),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - Trigger with unordered delivery",
			Objects: []runtime.Object{
				NewBroker(
					BrokerReady,
				),
				newTrigger(reconcilertesting.WithAnnotation(deliveryOrderAnnotation, string(internals.Unordered))),
			},
			Key: testKey,
			WantCreates: []runtime.Object{
				NewConsumerGroup(
					WithConsumerGroupName(TriggerUUID),
					WithConsumerGroupNamespace(triggerNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(newTrigger())),
					WithConsumerGroupLabels(nil),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(),
						ConsumerConfigs(
							ConsumerGroupIdConfig(TriggerUUID),
						),
						ConsumerDelivery(NewConsumerSpecDelivery(internals.Unordered)),
						ConsumerFilters(NewConsumerSpecFilters()),
					)),
				),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerBrokerReady(),
						withTriggerSubscriberResolvedSucceeded(),
						reconcilertesting.WithTriggerDependencyReady(),
						withDeadLetterSinkURI(""),
						reconcilertesting.WithAnnotation(deliveryOrderAnnotation, string(internals.Unordered)),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - Trigger with invalid delivery",
			Objects: []runtime.Object{
				NewBroker(
					BrokerReady,
				),
				newTrigger(reconcilertesting.WithAnnotation(deliveryOrderAnnotation, "invalid")),
			},
			Key:     testKey,
			WantErr: true,
			WantEvents: []string{
				Eventf(
					corev1.EventTypeWarning,
					"InternalError",
					fmt.Sprintf(
						"invalid annotation %s value: invalid. Allowed values [ \"ordered\" | \"unordered\" ]",
						deliveryOrderAnnotation,
					),
				),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
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
				),
				newTrigger(),
				NewConsumerGroup(
					WithConsumerGroupName(TriggerUUID),
					WithConsumerGroupNamespace(triggerNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(newTrigger())),
					WithConsumerGroupLabels(nil),
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
						WithConsumerGroupLabels(nil),
						ConsumerGroupConsumerSpec(NewConsumerSpec(
							ConsumerTopics(),
							ConsumerConfigs(
								ConsumerGroupIdConfig(TriggerUUID),
							),
							ConsumerDelivery(NewConsumerSpecDelivery(internals.Ordered)),
							ConsumerFilters(NewConsumerSpecFilters()),
						)),
					),
				},
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerBrokerReady(),
						withTriggerSubscriberResolvedSucceeded(),
						reconcilertesting.WithTriggerDependencyReady(),
						withDeadLetterSinkURI(""),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - existing cg without update",
			Objects: []runtime.Object{
				NewBroker(
					BrokerReady,
				),
				newTrigger(),
				NewConsumerGroup(
					WithConsumerGroupName(TriggerUUID),
					WithConsumerGroupNamespace(triggerNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(newTrigger())),
					WithConsumerGroupLabels(nil),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(),
						ConsumerConfigs(
							ConsumerGroupIdConfig(TriggerUUID),
						),
						ConsumerDelivery(NewConsumerSpecDelivery(internals.Ordered)),
						ConsumerFilters(NewConsumerSpecFilters()),
					)),
				),
			},
			Key: testKey,
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerBrokerReady(),
						withTriggerSubscriberResolvedSucceeded(),
						reconcilertesting.WithTriggerDependencyReady(),
						withDeadLetterSinkURI(""),
					),
				},
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
			Key: testKey,
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
					),
				},
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
					),
				},
			},
		},
	}

	table.Test(t, NewFactory(nil, func(ctx context.Context, listers *Listers, env *config.Env, row *TableRow) controller.Reconciler {

		logger := logging.FromContext(ctx)

		reconciler := &Reconciler{
			BrokerLister:        listers.GetBrokerLister(),
			EventingClient:      eventingclient.Get(ctx),
			Env:                 env,
			ConsumerGroupLister: listers.GetConsumerGroupLister(),
			InternalsClient:     fakeconsumergroupinformer.Get(ctx),
		}

		return triggerreconciler.NewReconciler(
			ctx,
			logger,
			eventingclient.Get(ctx),
			listers.GetTriggerLister(),
			controller.GetEventRecorder(ctx),
			reconciler,
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
