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

package channel_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/Shopify/sarama"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgotesting "k8s.io/client-go/testing"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/receiver"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	. "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/channel"
	kafkatesting "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka/testing"
	. "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/testing"
	messagingv1beta "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	fakeeventingkafkaclient "knative.dev/eventing-kafka/pkg/client/injection/client/fake"
	eventingkafkachannelreconciler "knative.dev/eventing-kafka/pkg/client/injection/reconciler/messaging/v1beta1/kafkachannel"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	. "knative.dev/pkg/reconciler/testing"
	"knative.dev/pkg/resolver"
	"knative.dev/pkg/tracker"
)

// TODO: tests with and without subscriptions
func TestReconcileKind(t *testing.T) {

	messagingv1beta.RegisterAlternateKafkaChannelConditionSet(base.IngressConditionSet)

	configs := &config.Env{
		SystemNamespace:      "cm",
		GeneralConfigMapName: "cm",
	}
	testKey := fmt.Sprintf("%s/%s", ChannelNamespace, ChannelName)

	table := TableTest{
		{
			Name: "Reconciled normal - no subscription - no auth",
			Objects: []runtime.Object{
				NewChannel(),
				NewService(),
				ChannelReceiverPod(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				ChannelDispatcherPod(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(configs, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Ingress: &contract.Ingress{
								IngressType: &contract.Ingress_Path{
									Path: receiver.Path(ChannelNamespace, ChannelName),
								},
							},
							Auth:      &contract.Resource_AbsentAuth{},
							Reference: &contract.Reference{Namespace: ChannelNamespace, Name: ChannelName},
						},
					},
				}),
				ChannelReceiverPodUpdate(configs.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
				ChannelDispatcherPodUpdate(configs.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			SkipNamespaceValidation: true, // WantCreates compare the broker namespace with configmap namespace, so skip it
			WantCreates: []runtime.Object{
				NewConfigMap(configs, nil),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
					//ChannelConfigMapUpdatedReady(&configs),
					//ChannelTopicsReady,
					//ChannelDataPlaneAvailable,
					//ChannelInitialOffsetsCommitted,
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				//patchFinalizers(),
			},
			WantEvents: []string{
				//finalizerUpdatedEvent,
			},
		},
	}

	useTable(t, table, configs)
}

func useTable(t *testing.T, table TableTest, configs *config.Env) {
	table.Test(t, NewFactory(configs, func(ctx context.Context, listers *Listers, configs *config.Env, row *TableRow) controller.Reconciler {

		var topicMetadata []*sarama.TopicMetadata
		for _, t := range SourceTopics {
			topicMetadata = append(topicMetadata, &sarama.TopicMetadata{Name: t, Partitions: []*sarama.PartitionMetadata{{}}})
		}

		reconciler := &Reconciler{
			Reconciler: &base.Reconciler{
				KubeClient:                  kubeclient.Get(ctx),
				PodLister:                   listers.GetPodLister(),
				SecretLister:                listers.GetSecretLister(),
				DataPlaneConfigMapNamespace: configs.DataPlaneConfigMapNamespace,
				DataPlaneConfigMapName:      configs.DataPlaneConfigMapName,
				DataPlaneConfigFormat:       configs.DataPlaneConfigFormat,
				SystemNamespace:             configs.SystemNamespace,
				DispatcherLabel:             base.SourceDispatcherLabel,
			},
			Env: configs,
			InitOffsetsFunc: func(ctx context.Context, kafkaClient sarama.Client, kafkaAdminClient sarama.ClusterAdmin, topics []string, consumerGroup string) (int32, error) {
				return 1, nil
			},
			NewKafkaClient: func(addrs []string, config *sarama.Config) (sarama.Client, error) {
				return &kafkatesting.MockKafkaClient{}, nil
			},
			NewKafkaClusterAdminClient: func(_ []string, _ *sarama.Config) (sarama.ClusterAdmin, error) {
				return &kafkatesting.MockKafkaClusterAdmin{
					ExpectedTopicName:                      "",
					ExpectedTopicDetail:                    sarama.TopicDetail{},
					ErrorOnCreateTopic:                     nil,
					ErrorOnDeleteTopic:                     nil,
					ExpectedClose:                          false,
					ExpectedCloseError:                     nil,
					ExpectedTopics:                         SourceTopics,
					ExpectedErrorOnDescribeTopics:          nil,
					ExpectedTopicsMetadataOnDescribeTopics: topicMetadata,
					T:                                      t,
				}, nil
			},
		}

		reconciler.ConfigMapTracker = &FakeTracker{}
		reconciler.SecretTracker = &FakeTracker{}

		reconciler.Resolver = resolver.NewURIResolverFromTracker(ctx, tracker.New(func(name types.NamespacedName) {}, 0))

		r := eventingkafkachannelreconciler.NewReconciler(
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
