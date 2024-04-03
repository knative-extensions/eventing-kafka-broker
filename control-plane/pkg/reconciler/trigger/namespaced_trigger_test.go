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

	"github.com/IBM/sarama"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgotesting "k8s.io/client-go/testing"
	sources "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/sources/v1beta1"
	kubeclient "knative.dev/pkg/client/injection/kube/client/fake"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	. "knative.dev/pkg/reconciler/testing"
	"knative.dev/pkg/resolver"
	"knative.dev/pkg/tracker"

	eventing "knative.dev/eventing/pkg/apis/eventing/v1"
	eventingclient "knative.dev/eventing/pkg/client/injection/client/fake"
	triggerreconciler "knative.dev/eventing/pkg/client/injection/reconciler/eventing/v1/trigger"
	reconcilertesting "knative.dev/eventing/pkg/reconciler/testing/v1"

	apisconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
	kafkatesting "knative.dev/eventing-kafka-broker/control-plane/pkg/kafka/testing"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/receiver"
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
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     BrokerUUID,
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
						},
					},
				}, BrokerNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat),
				BrokerDispatcherPod(BrokerNamespace, nil),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(BrokerNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
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
				BrokerDispatcherPodUpdate(BrokerNamespace, map[string]string{
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
						withTriggerStatusGroupIdAnnotation(triggerConsumerGroup),
						reconcilertesting.WithTriggerDeadLetterSinkNotConfigured(),
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
				Flags: nil,
			},
			BrokerLister:    listers.GetBrokerLister(),
			ConfigMapLister: listers.GetConfigMapLister(),
			EventingClient:  eventingclient.Get(ctx),
			Resolver:        nil,
			Env:             env,
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
			KafkaFeatureFlags: apisconfig.DefaultFeaturesConfig(),
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
