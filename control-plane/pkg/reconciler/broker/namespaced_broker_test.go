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

package broker_test // different package name due to import cycles. (broker -> testing -> broker)

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	mfclient "github.com/manifestival/client-go-client"
	mf "github.com/manifestival/manifestival"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	fakedynamicclient "knative.dev/pkg/injection/clients/dynamicclient/fake"

	"k8s.io/client-go/kubernetes/scheme"

	"github.com/Shopify/sarama"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	kafkatesting "knative.dev/eventing-kafka-broker/control-plane/pkg/kafka/testing"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/prober"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/prober/probertesting"
	fakeeventingclient "knative.dev/eventing/pkg/client/injection/client/fake"
	brokerreconciler "knative.dev/eventing/pkg/client/injection/reconciler/eventing/v1/broker"
	kubeclient "knative.dev/pkg/client/injection/kube/client/fake"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/resolver"
	"knative.dev/pkg/tracker"

	"k8s.io/apimachinery/pkg/runtime"
	clientgotesting "k8s.io/client-go/testing"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	. "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/broker"
	. "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/testing"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1"
	reconcilertesting "knative.dev/eventing/pkg/reconciler/testing/v1"
	. "knative.dev/pkg/reconciler/testing"
)

var IngoredFieldsForComparison = map[string]struct{}{
	"annotations":       {},
	"creationTimestamp": {},
	"data":              {},
	"labels":            {},
	"roleRef":           {},
	"spec":              {},
	"status":            {},
	"subjects":          {},
	"selector":          {},
	"strategy":          {},
	"template":          {},
}

// ignoreManifestivalMetadata is a cmp option that ignores most of the fields when doing comparison with unstructured.
// 1. This was necessary as the objects we create via Manifestival have additional metadata that we don't care.
// 2. We don't need to check if things are created exactly like they are in the YAML files.
// We basically just check for existence for these resources and also check if the namespace and owner injections
// are good.
// NOTE: This is only used for map comparison done for unstructured data and not the structs.
var ignoreManifestivalMetadata = cmpopts.IgnoreMapEntries(func(t string, _ interface{}) bool {
	_, exists := IngoredFieldsForComparison[t]
	return exists
})

func TestNamespacedBrokerReconciler(t *testing.T) {
	eventing.RegisterAlternateBrokerConditionSet(base.IngressConditionSet)

	t.Parallel()

	for _, f := range Formats {
		namespacedBrokerReconciliation(t, f, *DefaultEnv)
	}
}

func namespacedBrokerReconciliation(t *testing.T, format string, env config.Env) {

	testKey := fmt.Sprintf("%s/%s", BrokerNamespace, BrokerName)

	env.DataPlaneConfigFormat = format

	table := TableTest{
		{
			Name: "Reconciled normal - create namespaced dataplane",
			Objects: []runtime.Object{
				NewNamespacedBroker(),
			},
			Key:     testKey,
			WantErr: true,
			WantEvents: []string{
				finalizerUpdatedEvent,
				Eventf(
					corev1.EventTypeWarning,
					"InternalError",
					fmt.Sprintf("%s: %s", base.ReasonDataPlaneNotAvailable, base.MessageDataPlaneNotAvailable),
				),
			},
			WantCreates: []runtime.Object{
				toUnstructured(
					reconcilertesting.NewConfigMap(
						"config-kafka-broker-data-plane", BrokerNamespace,
						ConfigmapOwnerReferenceBroker(BrokerUUID, BrokerName, false),
					),
				),
				toUnstructured(
					NewServiceAccount(BrokerNamespace, "knative-kafka-broker-data-plane",
						ServiceAccountOwnerReferenceBroker(BrokerUUID, BrokerName, false),
					),
				),
				toUnstructured(
					NewRoleBinding(BrokerNamespace, "knative-kafka-broker-data-plane",
						RoleBindingOwnerReferenceBroker(BrokerUUID, BrokerName, false),
					),
				),
				toUnstructured(
					reconcilertesting.NewDeployment("kafka-broker-dispatcher", BrokerNamespace,
						DeploymentOwnerReferenceBroker(BrokerUUID, BrokerName, false),
					),
				),
				toUnstructured(
					reconcilertesting.NewDeployment("kafka-broker-receiver", BrokerNamespace,
						DeploymentOwnerReferenceBroker(BrokerUUID, BrokerName, false),
					),
				),
				toUnstructured(
					reconcilertesting.NewService("kafka-broker-ingress", BrokerNamespace,
						ServiceOwnerReferenceBroker(BrokerUUID, BrokerName, false),
					),
				),
				NewConfigMapWithBinaryData(BrokerNamespace, "kafka-broker-brokers-triggers",
					nil,
					ConfigmapOwnerReferenceBroker(BrokerUUID, BrokerName, false),
					reconcilertesting.WithConfigMapLabels(metav1.LabelSelector{
						MatchLabels: map[string]string{
							"eventing.knative.dev/namespaced": "true",
						},
					}),
				),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewNamespacedBroker(
						reconcilertesting.WithInitBrokerConditions,
						StatusBrokerDataPlaneNotAvailable,
					),
				},
			},
			CmpOpts: []cmp.Option{ignoreManifestivalMetadata},
		},
	}

	for i := range table {
		table[i].Name = table[i].Name + " - " + format
	}

	useNamespacedTable(t, table, &env)
}

func useNamespacedTable(t *testing.T, table TableTest, env *config.Env) {

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

		fkc := fakedynamicclient.Get(ctx)
		mfc, err := mfclient.NewUnsafeDynamicClient(fkc)
		if err != nil {
			t.Fatalf("Unable to create manifestival client: %v", err)
		}

		dataplaneManifestPath := "testdata/namespaceddataplane/broker"
		manifest, err := mf.ManifestFrom(mf.Path(dataplaneManifestPath), mf.UseClient(mfc))
		if err != nil {
			t.Errorf("Unable to load test manifest: %v", err)
		}

		reconciler := &NamespacedReconciler{
			Reconciler: &base.Reconciler{
				KubeClient:                  kubeclient.Get(ctx),
				PodLister:                   listers.GetPodLister(),
				SecretLister:                listers.GetSecretLister(),
				DataPlaneConfigMapNamespace: env.DataPlaneConfigMapNamespace,
				DataPlaneConfigMapName:      env.DataPlaneConfigMapName,
				DataPlaneConfigFormat:       env.DataPlaneConfigFormat,
				DataPlaneNamespace:          env.SystemNamespace,
				DispatcherLabel:             base.BrokerDispatcherLabel,
				ReceiverLabel:               base.BrokerReceiverLabel,
			},
			ConfigMapLister: listers.GetConfigMapLister(),
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
			Env:                   env,
			Prober:                proberMock,
			BaseDataPlaneManifest: manifest,
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

func toUnstructured(obj runtime.Object) *unstructured.Unstructured {
	resource := &unstructured.Unstructured{}
	err := scheme.Scheme.Convert(obj, resource, nil)
	if err != nil {
		panic(err)
	}
	return resource
}
