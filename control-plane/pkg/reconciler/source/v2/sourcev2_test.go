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

	"k8s.io/apimachinery/pkg/runtime"
	clientgotesting "k8s.io/client-go/testing"
	"knative.dev/pkg/kmeta"

	internals "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"

	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	. "knative.dev/pkg/reconciler/testing"

	fakeconsumergroupinformer "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/client/fake"
	sources "knative.dev/eventing-kafka/pkg/apis/sources/v1beta1"
	fakeeventingkafkaclient "knative.dev/eventing-kafka/pkg/client/injection/client/fake"
	eventingkafkasourcereconciler "knative.dev/eventing-kafka/pkg/client/injection/reconciler/sources/v1beta1/kafkasource"

	. "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/testing"
)

func TestReconcileKind(t *testing.T) {

	testKey := fmt.Sprintf("%s/%s", SourceNamespace, SourceName)

	sources.RegisterAlternateKafkaConditionSet(conditionSet)

	table := TableTest{
		{
			Name: "Reconciled normal",
			Objects: []runtime.Object{
				NewSource(),
			},
			Key: testKey,
			WantCreates: []runtime.Object{
				NewConsumerGroup(
					WithConsumerGroupName(SourceUUID),
					WithConsumerGroupNamespace(SourceNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(NewSource())),
					WithConsumerGroupLabels(nil),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(SourceTopics[0], SourceTopics[1]),
						ConsumerConfigs(
							ConsumerGroupIdConfig(SourceConsumerGroup),
							ConsumerBootstrapServersConfig(SourceBootstrapServers),
						),
						ConsumerAuth(NewConsumerSpecAuth()),
						ConsumerDelivery(NewConsumerSpecDelivery(internals.Ordered)),
						ConsumerSubscriber(NewSourceSinkReference()),
					)),
					ConsumerGroupReplicas(1),
				),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSource(
						StatusSourceConsumerGrouUnknown(),
						StatusSourceSinkResolved(""),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - ce overrides",
			Objects: []runtime.Object{
				NewSourceSinkObject(),
				NewSource(WithCloudEventOverrides(&duckv1.CloudEventOverrides{
					Extensions: map[string]string{"a": "foo", "b": "foo"},
				})),
			},
			Key: testKey,
			WantCreates: []runtime.Object{
				NewConsumerGroup(
					WithConsumerGroupName(SourceUUID),
					WithConsumerGroupNamespace(SourceNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(NewSource())),
					WithConsumerGroupLabels(nil),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(SourceTopics[0], SourceTopics[1]),
						ConsumerConfigs(
							ConsumerGroupIdConfig(SourceConsumerGroup),
							ConsumerBootstrapServersConfig(SourceBootstrapServers),
						),
						ConsumerAuth(NewConsumerSpecAuth()),
						ConsumerDelivery(NewConsumerSpecDelivery(internals.Ordered)),
						ConsumerSubscriber(NewSourceSinkReference()),
						ConsumerCloudEventOverrides(&duckv1.CloudEventOverrides{
							Extensions: map[string]string{"a": "foo", "b": "foo"},
						}),
					)),
					ConsumerGroupReplicas(1),
				),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSource(
						WithCloudEventOverrides(&duckv1.CloudEventOverrides{
							Extensions: map[string]string{"a": "foo", "b": "foo"},
						}),
						StatusSourceConsumerGrouUnknown(),
						StatusSourceSinkResolved(""),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - existing cons group with update",
			Objects: []runtime.Object{
				NewSource(),
				NewConsumerGroup(
					WithConsumerGroupName(SourceUUID),
					WithConsumerGroupNamespace(SourceNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(NewSource())),
					WithConsumerGroupLabels(nil),
					ConsumerGroupReplicas(1),
					ConsumerGroupReady,
				),
			},
			Key:         testKey,
			WantCreates: []runtime.Object{},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewConsumerGroup(
						WithConsumerGroupName(SourceUUID),
						WithConsumerGroupNamespace(SourceNamespace),
						WithConsumerGroupOwnerRef(kmeta.NewControllerRef(NewSource())),
						WithConsumerGroupLabels(nil),
						ConsumerGroupConsumerSpec(NewConsumerSpec(
							ConsumerTopics(SourceTopics[0], SourceTopics[1]),
							ConsumerConfigs(
								ConsumerGroupIdConfig(SourceConsumerGroup),
								ConsumerBootstrapServersConfig(SourceBootstrapServers),
							),
							ConsumerAuth(NewConsumerSpecAuth()),
							ConsumerDelivery(NewConsumerSpecDelivery(internals.Ordered)),
							ConsumerSubscriber(NewSourceSinkReference()),
						)),
						ConsumerGroupReplicas(1),
						ConsumerGroupReady,
					),
				},
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSource(
						StatusSourceConsumerGroup(),
						StatusSourceSinkResolved(""),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - existing cons group with update but not ready",
			Objects: []runtime.Object{
				NewSource(),
				NewConsumerGroup(
					WithConsumerGroupName(SourceUUID),
					WithConsumerGroupNamespace(SourceNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(NewSource())),
					WithConsumerGroupLabels(nil),
					ConsumerGroupReplicas(1),
				),
			},
			Key:         testKey,
			WantCreates: []runtime.Object{},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewConsumerGroup(
						WithConsumerGroupName(SourceUUID),
						WithConsumerGroupNamespace(SourceNamespace),
						WithConsumerGroupOwnerRef(kmeta.NewControllerRef(NewSource())),
						WithConsumerGroupLabels(nil),
						ConsumerGroupConsumerSpec(NewConsumerSpec(
							ConsumerTopics(SourceTopics[0], SourceTopics[1]),
							ConsumerConfigs(
								ConsumerGroupIdConfig(SourceConsumerGroup),
								ConsumerBootstrapServersConfig(SourceBootstrapServers),
							),
							ConsumerAuth(NewConsumerSpecAuth()),
							ConsumerDelivery(NewConsumerSpecDelivery(internals.Ordered)),
							ConsumerSubscriber(NewSourceSinkReference()),
						)),
						ConsumerGroupReplicas(1),
					),
				},
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSource(
						StatusSourceConsumerGrouUnknown(),
						StatusSourceSinkResolved(""),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - existing cons group without update",
			Objects: []runtime.Object{
				NewSource(),
				NewConsumerGroup(
					WithConsumerGroupName(SourceUUID),
					WithConsumerGroupNamespace(SourceNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(NewSource())),
					WithConsumerGroupLabels(nil),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(SourceTopics[0], SourceTopics[1]),
						ConsumerConfigs(
							ConsumerGroupIdConfig(SourceConsumerGroup),
							ConsumerBootstrapServersConfig(SourceBootstrapServers),
						),
						ConsumerAuth(NewConsumerSpecAuth()),
						ConsumerDelivery(NewConsumerSpecDelivery(internals.Ordered)),
						ConsumerSubscriber(NewSourceSinkReference()),
					)),
					ConsumerGroupReplicas(1),
					ConsumerGroupReady,
				),
			},
			Key:         testKey,
			WantCreates: []runtime.Object{},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSource(
						StatusSourceConsumerGroup(),
						StatusSourceSinkResolved(""),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - existing cons group without update but not ready",
			Objects: []runtime.Object{
				NewSource(),
				NewConsumerGroup(
					WithConsumerGroupName(SourceUUID),
					WithConsumerGroupNamespace(SourceNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(NewSource())),
					WithConsumerGroupLabels(nil),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(SourceTopics[0], SourceTopics[1]),
						ConsumerConfigs(
							ConsumerGroupIdConfig(SourceConsumerGroup),
							ConsumerBootstrapServersConfig(SourceBootstrapServers),
						),
						ConsumerAuth(NewConsumerSpecAuth()),
						ConsumerDelivery(NewConsumerSpecDelivery(internals.Ordered)),
						ConsumerSubscriber(NewSourceSinkReference()),
					)),
					ConsumerGroupReplicas(1),
				),
			},
			Key:         testKey,
			WantCreates: []runtime.Object{},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSource(
						StatusSourceConsumerGrouUnknown(),
						StatusSourceSinkResolved(""),
					),
				},
			},
		},
	}

	table.Test(t, NewFactory(nil, func(ctx context.Context, listers *Listers, env *config.Env, row *TableRow) controller.Reconciler {

		reconciler := &Reconciler{
			ConsumerGroupLister: listers.GetConsumerGroupLister(),
			InternalsClient:     fakeconsumergroupinformer.Get(ctx),
		}

		r := eventingkafkasourcereconciler.NewReconciler(
			ctx,
			logging.FromContext(ctx),
			fakeeventingkafkaclient.Get(ctx),
			listers.GetKafkaSourceLister(),
			controller.GetEventRecorder(ctx),
			reconciler,
		)
		return r
	}))
}

func StatusSourceConsumerGroup() KRShapedOption {
	return func(obj duckv1.KRShaped) {
		ks := obj.(*sources.KafkaSource)
		ks.GetConditionSet().Manage(ks.GetStatus()).MarkTrue(KafkaConditionConsumerGroup)
	}
}

func StatusSourceConsumerGroupFailed() KRShapedOption {
	return func(obj duckv1.KRShaped) {
		ks := obj.(*sources.KafkaSource)
		ks.GetConditionSet().Manage(ks.GetStatus()).MarkFalse(KafkaConditionConsumerGroup, "failed to reconcile consumer group", "")
	}
}

func StatusSourceConsumerGrouUnknown() KRShapedOption {
	return func(obj duckv1.KRShaped) {
		ks := obj.(*sources.KafkaSource)
		ks.GetConditionSet().Manage(ks.GetStatus()).MarkUnknown(KafkaConditionConsumerGroup, "failed to reconcile consumer group", "consumer group not ready")
	}
}
