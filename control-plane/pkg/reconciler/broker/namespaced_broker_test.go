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
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	pointer "knative.dev/pkg/ptr"

	sources "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/sources/v1"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	kafkatesting "knative.dev/eventing-kafka-broker/control-plane/pkg/kafka/testing"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/prober"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/prober/probertesting"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/util"

	"github.com/IBM/sarama"
	"github.com/manifestival/client-go-client"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgotesting "k8s.io/client-go/testing"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1"
	"knative.dev/pkg/apis"
	kubeclient "knative.dev/pkg/client/injection/kube/client/fake"
	"knative.dev/pkg/controller"
	dynamicclientfake "knative.dev/pkg/injection/clients/dynamicclient/fake"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/network"
	. "knative.dev/pkg/reconciler/testing"
	"knative.dev/pkg/resolver"
	"knative.dev/pkg/tracker"

	fakeeventingclient "knative.dev/eventing/pkg/client/injection/client/fake"
	brokerreconciler "knative.dev/eventing/pkg/client/injection/reconciler/eventing/v1/broker"
	reconcilertesting "knative.dev/eventing/pkg/reconciler/testing/v1"

	duckv1 "knative.dev/pkg/apis/duck/v1"

	apisconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/receiver"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	. "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/broker"
	. "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/testing"
)

var (
	brokerNamespacedAddress = &apis.URL{
		Scheme: "http",
		Host:   network.GetServiceHostname(DefaultEnv.IngressName, BrokerNamespace),
		Path:   fmt.Sprintf("/%s/%s", BrokerNamespace, BrokerName),
	}
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
				reconcilertesting.NewNamespace(BrokerNamespace, func(ns *corev1.Namespace) {
					ns.UID = BrokerNamespaceUUID
				}),
				NewNamespacedBroker(
					WithBrokerConfig(
						KReference(BrokerConfig(bootstrapServers, 20, 5, WithConfigMapNamespace(BrokerNamespace))),
					),
				),
				BrokerConfig(bootstrapServers, 20, 5, WithConfigMapNamespace(BrokerNamespace)),
				DataPlaneConfigMap(SystemNamespace, env.DataPlaneConfigConfigMapName, ConsumerConfigKey,
					DataPlaneConfigInitialOffset(ConsumerConfigKey, sources.OffsetLatest),
				),
				reconcilertesting.NewConfigMap("config-observability", SystemNamespace),
				reconcilertesting.NewConfigMap("config-features", SystemNamespace),
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
				NewDeployment("kafka-broker-receiver", SystemNamespace),
				NewStatefulSet("kafka-broker-dispatcher", SystemNamespace),
				NewServiceAccount(SystemNamespace, "knative-kafka-broker-data-plane"),
				reconcilertesting.NewService("kafka-broker-ingress", SystemNamespace),
				NewClusterRoleBinding("knative-kafka-broker-data-plane",
					WithClusterRoleBindingSubjectServiceAccount(SystemNamespace, "knative-kafka-broker-data-plane"),
					WithClusterRoleBindingRoleRef("knative-kafka-broker-data-plane"),
				),
				NewConfigMapWithTextData(SystemNamespace, NamespacedBrokerAdditionalResourcesConfigMapName, map[string]string{
					"resources": `
- apiVersion: rbac.authorization.k8s.io/v1
  kind: ClusterRole
  metadata:
    name: test-role
    labels:
      "knative.foo": "foo-{{.Namespace}}"
    creationTimestamp: null
  rules:
    - apiGroups:
        - "v1"
      resources:
        - pods
      verbs:
        - get
- apiVersion: rbac.authorization.k8s.io/v1
  kind: RoleBinding
  metadata:
    creationTimestamp: null
    name: test-role
    namespace: {{.Namespace}}
  roleRef:
    apiGroup: rbac.authorization.k8s.io
    kind: ClusterRole
    name: test-role
  subjects:
    - kind: ServiceAccount
      name: test-sa
      namespace: not-to-be-overridden
`,
				}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantCreates: []runtime.Object{
				ToManifestivalResource(t,
					DataPlaneConfigMap(BrokerNamespace, env.DataPlaneConfigConfigMapName, ConsumerConfigKey,
						DataPlaneConfigInitialOffset(ConsumerConfigKey, sources.OffsetLatest),
					),
					WithNamespacedBrokerOwnerRef,
					WithNamespacedLabel,
				),
				ToManifestivalResource(t,
					reconcilertesting.NewConfigMap(
						"config-observability",
						BrokerNamespace,
					),
					WithNamespacedBrokerOwnerRef,
					WithNamespacedLabel,
				),
				ToManifestivalResource(t,
					reconcilertesting.NewConfigMap(
						"config-features",
						BrokerNamespace,
					),
					WithNamespacedBrokerOwnerRef,
					WithNamespacedLabel,
				),
				ToManifestivalResource(t,
					reconcilertesting.NewConfigMap(
						"kafka-config-logging",
						BrokerNamespace,
					),
					WithNamespacedBrokerOwnerRef,
					WithNamespacedLabel,
				),
				ToManifestivalResource(t,
					NewDeployment("kafka-broker-receiver", BrokerNamespace),
					WithNamespacedBrokerOwnerRef,
					WithNamespacedLabel,
				),
				ToManifestivalResource(t,
					NewStatefulSet("kafka-broker-dispatcher", BrokerNamespace),
					WithNamespacedBrokerOwnerRef,
					WithNamespacedLabel,
				),
				ToManifestivalResource(t,
					NewServiceAccount(BrokerNamespace, "knative-kafka-broker-data-plane"),
					WithNamespacedBrokerOwnerRef,
					WithNamespacedLabel,
				),
				ToManifestivalResource(t,
					reconcilertesting.NewService("kafka-broker-ingress", BrokerNamespace),
					WithNamespacedBrokerOwnerRef,
					WithNamespacedLabel,
				),
				ToManifestivalResource(t, NewRoleBinding(BrokerNamespace, "knative-kafka-broker-data-plane",
					WithRoleBindingSubjectServiceAccount(BrokerNamespace, "knative-kafka-broker-data-plane"),
					WithRoleBindingClusterRoleRef("knative-kafka-broker-data-plane"),
				),
					WithNamespacedBrokerOwnerRef,
					WithNamespacedLabel,
				),
				ToManifestivalResource(t, NewClusterRole("test-role",
					WithClusterRoleLabel("knative.foo", "foo-"+BrokerNamespace),
					WithClusterRoleRules(rbacv1.PolicyRule{
						APIGroups: []string{"v1"},
						Resources: []string{"pods"},
						Verbs:     []string{"get"},
					})),
					WithNamespacedBrokerNamespaceAsOwnerRef,
					WithNamespacedLabel,
				),
				ToManifestivalResource(t, NewRoleBinding(BrokerNamespace, "test-role",
					WithRoleBindingSubjectServiceAccount("not-to-be-overridden", "test-sa"),
					WithRoleBindingClusterRoleRef("test-role")),
					WithNamespacedBrokerOwnerRef,
					WithNamespacedLabel,
				),
				NewConfigMapWithBinaryData(BrokerNamespace, env.ContractConfigMapName, nil,
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
								FeatureFlags:     FeatureFlagsETAutocreate(false),
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
						WithBrokerAddresses([]duckv1.Addressable{
							{
								Name: pointer.String("http"),
								URL:  brokerNamespacedAddress,
							},
						}),
						WithBrokerAddress(duckv1.Addressable{
							Name: pointer.String("http"),
							URL:  brokerNamespacedAddress,
						}),
						WithBrokerAddessable(),
						reconcilertesting.WithBrokerEventPoliciesReadyBecauseOIDCDisabled(),
					),
				},
			},
			// true since we're creating cluster scoped resources and namespace validation is checking if the namespace
			// of the broker is the same with the namespace of the cluster scoped resource (which is nil)
			SkipNamespaceValidation: true,
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
				reconcilertesting.NewNamespace(BrokerNamespace, func(ns *corev1.Namespace) {
					ns.UID = BrokerNamespaceUUID
				}),
				NewDeletedBroker(
					WithTopicStatusAnnotation(BrokerTopic()),
					reconcilertesting.WithBrokerClass(kafka.NamespacedBrokerClass),
				),
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
				reconcilertesting.NewConfigMap(env.DataPlaneConfigConfigMapName, SystemNamespace),
				reconcilertesting.NewConfigMap("config-observability", SystemNamespace),
				reconcilertesting.NewConfigMap("config-features", SystemNamespace),
				reconcilertesting.NewConfigMap("kafka-config-logging", SystemNamespace),
				NewDeployment("kafka-broker-receiver", SystemNamespace),
				NewStatefulSet("kafka-broker-dispatcher", SystemNamespace),
				NewServiceAccount(SystemNamespace, "knative-kafka-broker-data-plane"),
				reconcilertesting.NewService("kafka-broker-ingress", SystemNamespace),
				NewClusterRoleBinding("knative-kafka-broker-data-plane",
					WithClusterRoleBindingSubjectServiceAccount(SystemNamespace, "knative-kafka-broker-data-plane"),
				),
				NewConfigMapWithTextData(SystemNamespace, NamespacedBrokerAdditionalResourcesConfigMapName, map[string]string{
					"resources": `
- apiVersion: rbac.authorization.k8s.io/v1
  kind: ClusterRole
  metadata:
    name: test-role
    labels:
      "knative.foo": "foo-{{.Namespace}}"
    creationTimestamp: null
  rules:
    - apiGroups:
        - "v1"
      resources:
        - pods
      verbs:
        - get
`,
				}),
				NewClusterRole("test-role",
					WithClusterRoleLabel("knative.foo", "foo-"+BrokerNamespace),
					WithClusterRoleRules(rbacv1.PolicyRule{
						APIGroups: []string{"v1"},
						Resources: []string{"pods"},
						Verbs:     []string{"get"},
					})),
			},
			Key: testKey,
			WantCreates: []runtime.Object{
				NewConfigMapWithBinaryData(BrokerNamespace, env.ContractConfigMapName, nil,
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
			},
			WantDeletes: []clientgotesting.DeleteActionImpl{
				{
					ActionImpl: clientgotesting.ActionImpl{
						Resource: schema.GroupVersionResource{
							Group:    rbacv1.SchemeGroupVersion.Group,
							Version:  rbacv1.SchemeGroupVersion.Version,
							Resource: "clusterroles",
						},
					},
					Name: "test-role",
				},
			},
			OtherTestData: map[string]interface{}{
				testProber: probertesting.MockNewProber(prober.StatusNotReady),
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

		expectedTopicName, err := apisconfig.DefaultFeaturesConfig().ExecuteBrokersTopicTemplate(metav1.ObjectMeta{Namespace: BrokerNamespace, Name: BrokerName})
		require.NoError(t, err, "Failed to create broker topic name from feature flags")
		if t, ok := row.OtherTestData[externalTopic]; ok {
			expectedTopicName = t.(string)
		}

		var metadata []*sarama.TopicMetadata
		metadata = append(metadata, &sarama.TopicMetadata{
			Name:       ExternalTopicName,
			IsInternal: false,
			Partitions: []*sarama.PartitionMetadata{{}},
		})

		proberMock := probertesting.MockNewProber(prober.StatusReady)
		if p, ok := row.OtherTestData[testProber]; ok {
			proberMock = p.(prober.NewProber)
		}

		mfcMockClient, _ := client.NewUnsafeDynamicClient(dynamicclientfake.Get(ctx))

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
				Tracker:                     &FakeTracker{},
			},
			NamespaceLister:          listers.GetNamespaceLister(),
			ConfigMapLister:          listers.GetConfigMapLister(),
			StatefulSetLister:        listers.GetStatefulSetLister(),
			DeploymentLister:         listers.GetDeploymentLister(),
			BrokerLister:             listers.GetBrokerLister(),
			ServiceAccountLister:     listers.GetServiceAccountLister(),
			ServiceLister:            listers.GetServiceLister(),
			ClusterRoleBindingLister: listers.GetClusterRoleBindingLister(),
			EventPolicyLister:        listers.GetEventPolicyLister(),
			GetKafkaClusterAdmin: func(_ context.Context, _ []string, _ *corev1.Secret) (sarama.ClusterAdmin, error) {
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
			Env:                                env,
			Prober:                             proberMock,
			ManifestivalClient:                 mfcMockClient,
			DataplaneLifecycleLocksByNamespace: util.NewExpiringLockMap[string](ctx, time.Minute*30),
			KafkaFeatureFlags:                  apisconfig.DefaultFeaturesConfig(),
		}

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

func WithNamespacedBrokerOwnerRef(u *unstructured.Unstructured) {
	refs := u.GetOwnerReferences()
	if refs == nil {
		refs = []metav1.OwnerReference{}
	}
	refs = append(refs, metav1.OwnerReference{
		APIVersion:         eventing.SchemeGroupVersion.String(),
		Kind:               "Broker",
		Name:               BrokerName,
		UID:                BrokerUUID,
		Controller:         pointer.Bool(false),
		BlockOwnerDeletion: pointer.Bool(true),
	})
	u.SetOwnerReferences(refs)
}

func WithNamespacedBrokerNamespaceAsOwnerRef(u *unstructured.Unstructured) {
	refs := u.GetOwnerReferences()
	if refs == nil {
		refs = []metav1.OwnerReference{}
	}
	refs = append(refs, metav1.OwnerReference{
		APIVersion:         corev1.SchemeGroupVersion.String(),
		Kind:               "Namespace",
		Name:               BrokerNamespace,
		UID:                BrokerNamespaceUUID,
		Controller:         pointer.Bool(true),
		BlockOwnerDeletion: pointer.Bool(false),
	})
	u.SetOwnerReferences(refs)
}

func WithNamespacedLabel(u *unstructured.Unstructured) {
	labels := u.GetLabels()
	if labels == nil {
		labels = make(map[string]string)
	}
	labels[kafka.NamespacedBrokerDataplaneLabelKey] = kafka.NamespacedBrokerDataplaneLabelValue
	u.SetLabels(labels)
}

func ToManifestivalResource(t *testing.T, obj runtime.Object, mutators ...UnstructuredMutator) runtime.Object {
	m := func(u *unstructured.Unstructured) {
		annotations := u.GetAnnotations()
		if annotations == nil {
			annotations = make(map[string]string)
		}
		annotations["manifestival"] = "new"
		u.SetAnnotations(annotations)

		annotations[corev1.LastAppliedConfigAnnotation] = lastApplied(u)
		u.SetAnnotations(annotations)
	}
	mutators = append(mutators, m)
	return ToUnstructured(t, obj, mutators...)
}

// lastApplied returns a JSON string denoting the resource's state
func lastApplied(obj *unstructured.Unstructured) string {
	ann := obj.GetAnnotations()
	if len(ann) > 0 {
		delete(ann, corev1.LastAppliedConfigAnnotation)
		obj.SetAnnotations(ann)
	}
	bytes, _ := obj.MarshalJSON()
	return string(bytes)
}
