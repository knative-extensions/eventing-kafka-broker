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

package source_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/Shopify/sarama"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgotesting "k8s.io/client-go/testing"
	sources "knative.dev/eventing-kafka/pkg/apis/sources/v1beta1"
	kubeclient "knative.dev/pkg/client/injection/kube/client/fake"
	"knative.dev/pkg/resolver"
	"knative.dev/pkg/tracker"

	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	. "knative.dev/pkg/reconciler/testing"

	fakeeventingkafkaclient "knative.dev/eventing-kafka/pkg/client/injection/client/fake"
	eventingkafkasourcereconciler "knative.dev/eventing-kafka/pkg/client/injection/reconciler/sources/v1beta1/kafkasource"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/broker"
	kafkatesting "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka/testing"
	. "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/source"
	. "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/testing"
)

func TestReconcileKind(t *testing.T) {

	sources.RegisterAlternateKafkaConditionSet(base.EgressConditionSet)

	configs := *DefaultConfigs
	testKey := fmt.Sprintf("%s/%s", SourceNamespace, SourceName)

	table := TableTest{
		{
			Name: "Reconciled normal - no auth",
			Objects: []runtime.Object{
				NewSourceSinkObject(),
				NewSource(),
				SourceDispatcherPod(configs.SystemNamespace, map[string]string{
					"annotation_to_preserve": "value_to_preserve",
				}),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              SourceUUID,
							Topics:           SourceTopics,
							BootstrapServers: SourceBootstrapServers,
							Egresses: []*contract.Egress{
								{
									ConsumerGroup: SourceConsumerGroup,
									Destination:   ServiceURL,
									Uid:           SourceUUID,
									EgressConfig:  &DefaultEgressConfig,
									DeliveryOrder: DefaultDeliveryOrder,
									ReplyStrategy: &contract.Egress_DiscardReply{},
								},
							},
							Auth: &contract.Resource_AbsentAuth{},
						},
					},
				}),
				SourceDispatcherPodUpdate(configs.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			SkipNamespaceValidation: true, // WantCreates compare the broker namespace with configmap namespace, so skip it
			WantCreates: []runtime.Object{
				NewConfigMap(&configs, nil),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSource(
						SourceConfigMapUpdatedReady(&configs),
						SourceTopicsReady,
						SourceDataPlaneAvailable,
					),
				},
			},
		},
		{
			Name: "Reconciled normal - key type string",
			Objects: []runtime.Object{
				NewSourceSinkObject(),
				NewSource(WithKeyType("string")),
				SourceDispatcherPod(configs.SystemNamespace, map[string]string{
					"annotation_to_preserve": "value_to_preserve",
				}),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              SourceUUID,
							Topics:           SourceTopics,
							BootstrapServers: SourceBootstrapServers,
							Egresses: []*contract.Egress{
								{
									ConsumerGroup: SourceConsumerGroup,
									Destination:   ServiceURL,
									Uid:           SourceUUID,
									EgressConfig:  &DefaultEgressConfig,
									DeliveryOrder: DefaultDeliveryOrder,
									KeyType:       contract.KeyType_String,
									ReplyStrategy: &contract.Egress_DiscardReply{},
								},
							},
							Auth: &contract.Resource_AbsentAuth{},
						},
					},
				}),
				SourceDispatcherPodUpdate(configs.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			SkipNamespaceValidation: true, // WantCreates compare the broker namespace with configmap namespace, so skip it
			WantCreates: []runtime.Object{
				NewConfigMap(&configs, nil),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSource(
						WithKeyType("string"),
						SourceConfigMapUpdatedReady(&configs),
						SourceTopicsReady,
						SourceDataPlaneAvailable,
					),
				},
			},
		},
		{
			Name: "Reconciled normal - key type int",
			Objects: []runtime.Object{
				NewSourceSinkObject(),
				NewSource(WithKeyType("int")),
				SourceDispatcherPod(configs.SystemNamespace, map[string]string{
					"annotation_to_preserve": "value_to_preserve",
				}),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              SourceUUID,
							Topics:           SourceTopics,
							BootstrapServers: SourceBootstrapServers,
							Egresses: []*contract.Egress{
								{
									ConsumerGroup: SourceConsumerGroup,
									Destination:   ServiceURL,
									Uid:           SourceUUID,
									EgressConfig:  &DefaultEgressConfig,
									DeliveryOrder: DefaultDeliveryOrder,
									KeyType:       contract.KeyType_Integer,
									ReplyStrategy: &contract.Egress_DiscardReply{},
								},
							},
							Auth: &contract.Resource_AbsentAuth{},
						},
					},
				}),
				SourceDispatcherPodUpdate(configs.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			SkipNamespaceValidation: true, // WantCreates compare the broker namespace with configmap namespace, so skip it
			WantCreates: []runtime.Object{
				NewConfigMap(&configs, nil),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSource(
						WithKeyType("int"),
						SourceConfigMapUpdatedReady(&configs),
						SourceTopicsReady,
						SourceDataPlaneAvailable,
					),
				},
			},
		},
		{
			Name: "Reconciled normal - key type byte-array",
			Objects: []runtime.Object{
				NewSourceSinkObject(),
				NewSource(WithKeyType("byte-array")),
				SourceDispatcherPod(configs.SystemNamespace, map[string]string{
					"annotation_to_preserve": "value_to_preserve",
				}),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              SourceUUID,
							Topics:           SourceTopics,
							BootstrapServers: SourceBootstrapServers,
							Egresses: []*contract.Egress{
								{
									ConsumerGroup: SourceConsumerGroup,
									Destination:   ServiceURL,
									Uid:           SourceUUID,
									EgressConfig:  &DefaultEgressConfig,
									DeliveryOrder: DefaultDeliveryOrder,
									KeyType:       contract.KeyType_ByteArray,
									ReplyStrategy: &contract.Egress_DiscardReply{},
								},
							},
							Auth: &contract.Resource_AbsentAuth{},
						},
					},
				}),
				SourceDispatcherPodUpdate(configs.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			SkipNamespaceValidation: true, // WantCreates compare the broker namespace with configmap namespace, so skip it
			WantCreates: []runtime.Object{
				NewConfigMap(&configs, nil),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSource(
						WithKeyType("byte-array"),
						SourceConfigMapUpdatedReady(&configs),
						SourceTopicsReady,
						SourceDataPlaneAvailable,
					),
				},
			},
		},
		{
			Name: "Reconciled normal - key type float",
			Objects: []runtime.Object{
				NewSourceSinkObject(),
				NewSource(WithKeyType("float")),
				SourceDispatcherPod(configs.SystemNamespace, map[string]string{
					"annotation_to_preserve": "value_to_preserve",
				}),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              SourceUUID,
							Topics:           SourceTopics,
							BootstrapServers: SourceBootstrapServers,
							Egresses: []*contract.Egress{
								{
									ConsumerGroup: SourceConsumerGroup,
									Destination:   ServiceURL,
									Uid:           SourceUUID,
									EgressConfig:  &DefaultEgressConfig,
									DeliveryOrder: DefaultDeliveryOrder,
									KeyType:       contract.KeyType_Double,
									ReplyStrategy: &contract.Egress_DiscardReply{},
								},
							},
							Auth: &contract.Resource_AbsentAuth{},
						},
					},
				}),
				SourceDispatcherPodUpdate(configs.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			SkipNamespaceValidation: true, // WantCreates compare the broker namespace with configmap namespace, so skip it
			WantCreates: []runtime.Object{
				NewConfigMap(&configs, nil),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSource(
						WithKeyType("float"),
						SourceConfigMapUpdatedReady(&configs),
						SourceTopicsReady,
						SourceDataPlaneAvailable,
					),
				},
			},
		},
	}

	table.Test(t, NewFactory(&configs, func(ctx context.Context, listers *Listers, configs *broker.Configs, row *TableRow) controller.Reconciler {

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
			Env: &configs.Env,
			ClusterAdmin: func(addrs []string, config *sarama.Config) (sarama.ClusterAdmin, error) {
				return &kafkatesting.MockKafkaClusterAdmin{
					ExpectedTopicName:                      "",
					ExpectedTopicDetail:                    sarama.TopicDetail{},
					ErrorOnCreateTopic:                     nil,
					ErrorOnDeleteTopic:                     nil,
					ExpectedClose:                          false,
					ExpectedCloseError:                     nil,
					ExpectedTopics:                         SourceTopics,
					ExpectedErrorOnDescribeTopics:          nil,
					ExpectedTopicsMetadataOnDescribeTopics: nil,
					T:                                      t,
				}, nil
			},
		}

		reconciler.ConfigMapTracker = &FakeTracker{}
		reconciler.SecretTracker = &FakeTracker{}

		reconciler.Resolver = resolver.NewURIResolverFromTracker(ctx, tracker.New(func(name types.NamespacedName) {}, 0))

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

func SourceDispatcherPodUpdate(namespace string, annotations map[string]string) clientgotesting.UpdateActionImpl {
	return clientgotesting.NewUpdateAction(
		schema.GroupVersionResource{
			Group:    "*",
			Version:  "v1",
			Resource: "Pod",
		},
		namespace,
		SourceDispatcherPod(namespace, annotations),
	)
}
