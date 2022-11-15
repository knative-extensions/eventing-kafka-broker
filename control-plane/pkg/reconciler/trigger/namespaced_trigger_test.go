/*
 * Copyright 2022 The Knative Authors
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

	"k8s.io/apimachinery/pkg/runtime"
	clientgotesting "k8s.io/client-go/testing"
	sources "knative.dev/eventing-kafka/pkg/apis/sources/v1beta1"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/kmeta"
	"knative.dev/pkg/logging"
	. "knative.dev/pkg/reconciler/testing"

	eventing "knative.dev/eventing/pkg/apis/eventing/v1"
	eventingclient "knative.dev/eventing/pkg/client/injection/client/fake"
	triggerreconciler "knative.dev/eventing/pkg/client/injection/reconciler/eventing/v1/trigger"
	reconcilertesting "knative.dev/eventing/pkg/reconciler/testing/v1"

	internals "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing"
	fakeconsumergroupinformer "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/client/fake"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	brokerreconciler "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/broker"
	. "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/testing"
)

func TestNamespacedTriggerReconciler(t *testing.T) {
	eventing.RegisterAlternateBrokerConditionSet(base.IngressConditionSet)

	t.Parallel()

	for _, f := range Formats {
		namespacedTriggerReconciliation(t, f, *DefaultEnv)
	}
}

func namespacedTriggerReconciliation(t *testing.T, format string, env config.Env) {

	testKey := fmt.Sprintf("%s/%s", TriggerNamespace, TriggerName)

	env.ContractConfigMapFormat = format
	env.DataPlaneConfigMapNamespace = BrokerNamespace

	table := TableTest{
		{
			Name: "Reconciled normal - no existing Trigger",
			Objects: []runtime.Object{
				NewNamespacedBroker(
					BrokerReady,
					WithTopicStatusAnnotation(BrokerTopic()),
					WithBootstrapServerStatusAnnotation(bootstrapServers),
				),
				DataPlaneConfigMap(BrokerNamespace, env.DataPlaneConfigConfigMapName, brokerreconciler.ConsumerConfigKey,
					DataPlaneConfigInitialOffset(brokerreconciler.ConsumerConfigKey, sources.OffsetLatest),
				),
				newTrigger(),
				NewService(),
			},
			Key: testKey,
			WantCreates: []runtime.Object{
				NewConsumerGroup(
					WithConsumerGroupName(TriggerUUID),
					WithConsumerGroupNamespace(triggerNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(newTrigger())),
					WithConsumerGroupMetaLabels(OwnerAsTriggerLabel),
					WithConsumerGroupLabels(ConsumerTriggerLabel),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(BrokerTopics[0]),
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(bootstrapServers),
							ConsumerGroupIdConfig(TriggerUUID),
						),
						ConsumerDelivery(NewConsumerSpecDelivery(internals.Unordered, ConsumerInitialOffset(sources.OffsetLatest))),
						ConsumerFilters(NewConsumerSpecFilters()),
						ConsumerReply(ConsumerTopicReply()),
					)),
				),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: newTrigger(
						reconcilertesting.WithInitTriggerConditions,
						reconcilertesting.WithTriggerSubscribed(),
						reconcilertesting.WithTriggerBrokerReady(),
						withTriggerSubscriberResolvedSucceeded(),
						reconcilertesting.WithTriggerDependencyUnknown("failed to reconcile consumer group", "consumer group is not ready"),
						withDeadLetterSinkURI(""),
					),
				},
			},
		},
	}

	for i := range table {
		table[i].Name = table[i].Name + " - " + format
	}

	useNamespacedTable(t, table, &env)
}

func useNamespacedTable(t *testing.T, table TableTest, env *config.Env) {
	table.Test(t, NewFactory(env, func(ctx context.Context, listers *Listers, env *config.Env, row *TableRow) controller.Reconciler {

		logger := logging.FromContext(ctx)

		reconciler := &NamespacedReconciler{
			BrokerLister:        listers.GetBrokerLister(),
			ConfigMapLister:     listers.GetConfigMapLister(),
			EventingClient:      eventingclient.Get(ctx),
			Env:                 env,
			ConsumerGroupLister: listers.GetConsumerGroupLister(),
			InternalsClient:     fakeconsumergroupinformer.Get(ctx),
			SecretLister:        listers.GetSecretLister(),
			KubeClient:          kubeclient.Get(ctx),
		}

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
