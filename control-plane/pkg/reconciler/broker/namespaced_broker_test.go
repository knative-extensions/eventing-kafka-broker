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

package broker_test // different package name due to import cycles. (broker -> testing -> broker)

import (
	"context"
	"fmt"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"
	sources "knative.dev/eventing-kafka/pkg/apis/sources/v1beta1"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	kafkatesting "knative.dev/eventing-kafka-broker/control-plane/pkg/kafka/testing"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/prober"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/prober/probertesting"

	mffake "github.com/manifestival/manifestival/fake"

	"github.com/Shopify/sarama"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgotesting "k8s.io/client-go/testing"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1"
	kubeclient "knative.dev/pkg/client/injection/kube/client/fake"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	. "knative.dev/pkg/reconciler/testing"
	"knative.dev/pkg/resolver"
	"knative.dev/pkg/tracker"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/receiver"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	. "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/broker"
	. "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/testing"
	fakeeventingclient "knative.dev/eventing/pkg/client/injection/client/fake"
	brokerreconciler "knative.dev/eventing/pkg/client/injection/reconciler/eventing/v1/broker"
	reconcilertesting "knative.dev/eventing/pkg/reconciler/testing/v1"
)

func TestNamespacedBrokerReconciler(t *testing.T) {
	eventing.RegisterAlternateBrokerConditionSet(base.IngressConditionSet)

	t.Parallel()

	for _, f := range Formats {
		namespacedBrokerReconciliation(t, f, *DefaultEnv)
	}
}

func namespacedBrokerReconciliation(t *testing.T, format string, env config.Env) {

	testKey := fmt.Sprintf("%s/%s", BrokerNamespace, BrokerName)

	env.ContractConfigMapFormat = format

	table := TableTest{
		{
			Name: "Reconciled normal",
			Objects: []runtime.Object{
				NewNamespacedBroker(
					WithBrokerConfig(
						KReference(BrokerConfig(bootstrapServers, 20, 5, WithConfigMapNamespace(BrokerNamespace))),
					),
				),
				BrokerConfig(bootstrapServers, 20, 5, WithConfigMapNamespace(BrokerNamespace)),
				DataPlaneConfigMap(SystemNamespace, env.DataPlaneConfigConfigMapName, ConsumerConfigKey,
					DataPlaneConfigInitialOffset(ConsumerConfigKey, sources.OffsetLatest),
				),
				reconcilertesting.NewConfigMap("config-tracing", SystemNamespace),
				reconcilertesting.NewConfigMap("kafka-config-logging", SystemNamespace),
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
				NewService(),
				BrokerReceiverPod(BrokerNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				BrokerDispatcherPod(BrokerNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				reconcilertesting.NewDeployment("kafka-broker-receiver", SystemNamespace),
				reconcilertesting.NewDeployment("kafka-broker-dispatcher", SystemNamespace),
				NewServiceAccount(SystemNamespace, "knative-kafka-broker-data-plane"),
				reconcilertesting.NewService("kafka-broker-ingress", SystemNamespace),
				NewClusterRoleBinding("knative-kafka-broker-data-plane",
					WithSubjectServiceAccount(SystemNamespace, "knative-kafka-broker-data-plane", "knative-kafka-broker-data-plane"),
				),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantCreates: []runtime.Object{
				NewConfigMapWithBinaryData(BrokerNamespace, env.ContractConfigMapName, nil),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(BrokerNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat,
					&contract.Contract{
						Resources: []*contract.Resource{
							{
								Uid:              BrokerUUID,
								Topics:           []string{BrokerTopic()},
								Ingress:          &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
								BootstrapServers: bootstrapServers,
								Reference:        BrokerReference(),
							},
						},
						Generation: 1,
					},
					reconcilertesting.WithConfigMapLabels(metav1.LabelSelector{MatchLabels: map[string]string{"eventing.knative.dev/namespaced": "true"}}),
					WithConfigmapOwnerRef(&metav1.OwnerReference{
						APIVersion:         eventing.SchemeGroupVersion.String(),
						Kind:               "Broker",
						Name:               BrokerName,
						UID:                BrokerUUID,
						Controller:         pointer.Bool(false),
						BlockOwnerDeletion: pointer.Bool(true),
					}),
				),
				BrokerReceiverPodUpdate(BrokerNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				BrokerDispatcherPodUpdate(BrokerNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
					"annotation_to_preserve":           "value_to_preserve",
				}),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewNamespacedBroker(
						reconcilertesting.WithInitBrokerConditions,
						WithBrokerConfig(
							KReference(BrokerConfig(bootstrapServers, 20, 5, WithConfigMapNamespace(BrokerNamespace))),
						),
						StatusBrokerConfigMapUpdatedReady(&env),
						StatusBrokerDataPlaneAvailable,
						StatusBrokerConfigParsed,
						StatusBrokerTopicReady,
						NamespacedBrokerAddressable(&env),
						StatusBrokerProbeSucceeded,
						BrokerConfigMapAnnotations(),
						WithTopicStatusAnnotation(BrokerTopic()),
					),
				},
			},
		},
	}

	for i := range table {
		table[i].Name = table[i].Name + " - " + format
	}

	useTableNamespaced(t, table, &env)
}

func TestNamespacedBrokerFinalizer(t *testing.T) {
	t.Parallel()

	for _, f := range Formats {
		namespacedBrokerFinalization(t, f, *DefaultEnv)
	}
}

func namespacedBrokerFinalization(t *testing.T, format string, env config.Env) {

	testKey := fmt.Sprintf("%s/%s", BrokerNamespace, BrokerName)

	env.ContractConfigMapFormat = format

	table := TableTest{
		{
			Name: "Reconciled normal",
			Objects: []runtime.Object{
				NewDeletedBroker(reconcilertesting.WithBrokerClass(kafka.NamespacedBrokerClass)),
				BrokerConfig(bootstrapServers, 20, 5),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     BrokerUUID,
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
						},
					},
					Generation: 1,
				}, env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat),
			},
			Key: testKey,
			WantCreates: []runtime.Object{
				NewConfigMapWithBinaryData(BrokerNamespace, env.ContractConfigMapName, nil),
			},
			OtherTestData: map[string]interface{}{
				testProber: probertesting.MockProber(prober.StatusNotReady),
			},
		},
	}

	for i := range table {
		table[i].Name = table[i].Name + " - " + format
	}

	useTableNamespaced(t, table, &env)
}

func useTableNamespaced(t *testing.T, table TableTest, env *config.Env) {

	table.Test(t, NewFactory(env, func(ctx context.Context, listers *Listers, env *config.Env, row *TableRow) controller.Reconciler {

		defaultTopicDetail := sarama.TopicDetail{
			NumPartitions:     DefaultNumPartitions,
			ReplicationFactor: DefaultReplicationFactor,
		}

		var onCreateTopicError error
		if want, ok := row.OtherTestData[wantErrorOnCreateTopic]; ok {
			onCreateTopicError = want.(error)
		}

		var onDeleteTopicError error
		if want, ok := row.OtherTestData[wantErrorOnDeleteTopic]; ok {
			onDeleteTopicError = want.(error)
		}

		expectedTopicDetail := defaultTopicDetail
		if td, ok := row.OtherTestData[ExpectedTopicDetail]; ok {
			expectedTopicDetail = td.(sarama.TopicDetail)
		}

		expectedTopicName := fmt.Sprintf("%s%s-%s", TopicPrefix, BrokerNamespace, BrokerName)
		if t, ok := row.OtherTestData[externalTopic]; ok {
			expectedTopicName = t.(string)
		}

		var metadata []*sarama.TopicMetadata
		metadata = append(metadata, &sarama.TopicMetadata{
			Name:       ExternalTopicName,
			IsInternal: false,
			Partitions: []*sarama.PartitionMetadata{{}},
		})

		proberMock := probertesting.MockProber(prober.StatusReady)
		if p, ok := row.OtherTestData[testProber]; ok {
			proberMock = p.(prober.Prober)
		}

		mfcMockClient := mffake.New()

		reconciler := &NamespacedReconciler{
			Reconciler: &base.Reconciler{
				KubeClient:                  kubeclient.Get(ctx),
				PodLister:                   listers.GetPodLister(),
				SecretLister:                listers.GetSecretLister(),
				DataPlaneConfigMapNamespace: env.DataPlaneConfigMapNamespace,
				ContractConfigMapName:       env.ContractConfigMapName,
				ContractConfigMapFormat:     env.ContractConfigMapFormat,
				DataPlaneNamespace:          env.SystemNamespace,
				DispatcherLabel:             base.BrokerDispatcherLabel,
				ReceiverLabel:               base.BrokerReceiverLabel,
			},
			ConfigMapLister:          listers.GetConfigMapLister(),
			DeploymentLister:         listers.GetDeploymentLister(),
			ServiceAccountLister:     listers.GetServiceAccountLister(),
			ServiceLister:            listers.GetServiceLister(),
			ClusterRoleBindingLister: listers.GetClusterRoleBindingLister(),
			NewKafkaClusterAdminClient: func(_ []string, _ *sarama.Config) (sarama.ClusterAdmin, error) {
				return &kafkatesting.MockKafkaClusterAdmin{
					ExpectedTopicName:                      expectedTopicName,
					ExpectedTopicDetail:                    expectedTopicDetail,
					ErrorOnCreateTopic:                     onCreateTopicError,
					ErrorOnDeleteTopic:                     onDeleteTopicError,
					ExpectedTopics:                         []string{expectedTopicName},
					ExpectedTopicsMetadataOnDescribeTopics: metadata,
					T:                                      t,
				}, nil
			},
			Env:                env,
			Prober:             proberMock,
			ManifestivalClient: mfcMockClient,
		}

		reconciler.ConfigMapTracker = &FakeTracker{}
		reconciler.SecretTracker = &FakeTracker{}

		r := brokerreconciler.NewReconciler(
			ctx,
			logging.FromContext(ctx),
			fakeeventingclient.Get(ctx),
			listers.GetBrokerLister(),
			controller.GetEventRecorder(ctx),
			reconciler,
			kafka.NamespacedBrokerClass,
		)

		reconciler.Resolver = resolver.NewURIResolverFromTracker(ctx, tracker.New(func(name types.NamespacedName) {}, 0))
		reconciler.IPsLister = prober.NewIPListerWithMapping()

		return r
	}))
}
