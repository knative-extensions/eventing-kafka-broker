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

package consumer

import (
	"context"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgotesting "k8s.io/client-go/testing"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/pkg/apis"
	kubeclient "knative.dev/pkg/client/injection/kube/client/fake"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	. "knative.dev/pkg/reconciler/testing"
	"knative.dev/pkg/resolver"
	"knative.dev/pkg/tracker"

	configapis "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/config"
	internals "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing"
	kafkainternals "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing/v1alpha1"
	fakekafkainternalsclient "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/client/fake"
	creconciler "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/reconciler/eventing/v1alpha1/consumer"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	. "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/testing"
)

var (
	finalizerUpdatedEvent = Eventf(
		corev1.EventTypeNormal,
		"FinalizerUpdate",
		fmt.Sprintf(`Updated %q finalizers`, ConsumerName),
	)

	defaultContractFeatureFlags = &contract.EgressFeatureFlags{
		EnableRateLimiter: false,
	}
)

func TestReconcileKind(t *testing.T) {

	testKey := fmt.Sprintf("%s/%s", ConsumerNamespace, ConsumerName)

	table := TableTest{
		{
			Name: "Reconciled normal",
			Objects: []runtime.Object{
				NewService(),
				NewConsumerGroup(ConsumerGroupOwnerRef(SourceAsOwnerReference())),
				NewDispatcherPod("p1", PodRunning()),
				NewConsumer(1,
					ConsumerUID(ConsumerUUID),
					ConsumerSpec(NewConsumerSpec(
						ConsumerTopics(SourceTopics...),
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(SourceBootstrapServers),
							ConsumerGroupIdConfig(SourceConsumerGroup),
						),
						ConsumerSubscriber(NewSourceSinkReference()),
						ConsumerVReplicas(1),
						ConsumerPlacement(kafkainternals.PodBind{PodName: "p1", PodNamespace: SystemNamespace}),
					)),
					ConsumerOwnerRef(ConsumerGroupAsOwnerRef()),
				),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			SkipNamespaceValidation: true, // WantCreates compare the broker namespace with configmap namespace, so skip it
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantCreates: []runtime.Object{
				NewConfigMapWithBinaryData(&config.Env{
					DataPlaneConfigFormat:       base.Json,
					DataPlaneConfigMapNamespace: SystemNamespace,
					DataPlaneConfigMapName:      "p1",
				}, []byte(""),
					DispatcherPodAsOwnerReference("p1"),
				),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&config.Env{
					DataPlaneConfigFormat:       base.Json,
					DataPlaneConfigMapNamespace: SystemNamespace,
					DataPlaneConfigMapName:      "p1",
				}, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ConsumerUUID,
							Topics:           SourceTopics,
							BootstrapServers: SourceBootstrapServers,
							Egresses: []*contract.Egress{{
								ConsumerGroup: SourceConsumerGroup,
								Destination:   ServiceURL,
								ReplyStrategy: nil,
								Filter:        nil,
								Uid:           ConsumerUUID,
								DeliveryOrder: contract.DeliveryOrder_UNORDERED,
								KeyType:       0,
								VReplicas:     1,
								Reference: &contract.Reference{
									Uuid:      SourceUUID,
									Namespace: ConsumerNamespace,
									Name:      SourceName,
								},
								FeatureFlags: defaultContractFeatureFlags,
							}},
							Auth:                nil,
							CloudEventOverrides: nil,
							Reference: &contract.Reference{
								Uuid:      SourceUUID,
								Namespace: ConsumerNamespace,
								Name:      SourceName,
							},
						},
					},
				},
					DispatcherPodAsOwnerReference("p1"),
				),
				{Object: NewDispatcherPod("p1",
					PodRunning(),
					PodAnnotations(map[string]string{
						base.VolumeGenerationAnnotationKey: "1",
					}),
				)},
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: func() runtime.Object {
						c := NewConsumer(1,
							ConsumerUID(ConsumerUUID),
							ConsumerSpec(NewConsumerSpec(
								ConsumerTopics(SourceTopics...),
								ConsumerConfigs(
									ConsumerBootstrapServersConfig(SourceBootstrapServers),
									ConsumerGroupIdConfig(SourceConsumerGroup),
								),
								ConsumerSubscriber(NewSourceSinkReference()),
								ConsumerVReplicas(1),
								ConsumerPlacement(kafkainternals.PodBind{PodName: "p1", PodNamespace: SystemNamespace}),
							)),
							ConsumerOwnerRef(ConsumerGroupAsOwnerRef()),
						)
						c.GetConditionSet().Manage(c.GetStatus()).InitializeConditions()
						c.MarkReconcileContractSucceeded()
						c.MarkBindSucceeded()
						c.Status.SubscriberURI, _ = apis.ParseURL(ServiceURL)
						return c
					}(),
				},
			},
		},
		{
			Name: "Reconciled normal - multiple replicas",
			Objects: []runtime.Object{
				NewService(),
				NewConsumerGroup(ConsumerGroupOwnerRef(SourceAsOwnerReference())),
				NewDispatcherPod("p1", PodRunning()),
				NewConsumer(1,
					ConsumerUID(ConsumerUUID),
					ConsumerSpec(NewConsumerSpec(
						ConsumerTopics(SourceTopics...),
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(SourceBootstrapServers),
							ConsumerGroupIdConfig(SourceConsumerGroup),
						),
						ConsumerSubscriber(NewSourceSinkReference()),
						ConsumerVReplicas(2),
						ConsumerPlacement(kafkainternals.PodBind{PodName: "p1", PodNamespace: SystemNamespace}),
					)),
					ConsumerOwnerRef(ConsumerGroupAsOwnerRef()),
				),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			SkipNamespaceValidation: true, // WantCreates compare the broker namespace with configmap namespace, so skip it
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantCreates: []runtime.Object{
				NewConfigMapWithBinaryData(&config.Env{
					DataPlaneConfigFormat:       base.Json,
					DataPlaneConfigMapNamespace: SystemNamespace,
					DataPlaneConfigMapName:      "p1",
				}, []byte(""),
					DispatcherPodAsOwnerReference("p1"),
				),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&config.Env{
					DataPlaneConfigFormat:       base.Json,
					DataPlaneConfigMapNamespace: SystemNamespace,
					DataPlaneConfigMapName:      "p1",
				}, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ConsumerUUID,
							Topics:           SourceTopics,
							BootstrapServers: SourceBootstrapServers,
							Egresses: []*contract.Egress{{
								ConsumerGroup: SourceConsumerGroup,
								Destination:   ServiceURL,
								ReplyStrategy: nil,
								Filter:        nil,
								Uid:           ConsumerUUID,
								DeliveryOrder: contract.DeliveryOrder_UNORDERED,
								KeyType:       0,
								VReplicas:     2,
								Reference: &contract.Reference{
									Uuid:      SourceUUID,
									Namespace: ConsumerNamespace,
									Name:      SourceName,
								},
								FeatureFlags: defaultContractFeatureFlags,
							}},
							Auth:                nil,
							CloudEventOverrides: nil,
							Reference: &contract.Reference{
								Uuid:      SourceUUID,
								Namespace: ConsumerNamespace,
								Name:      SourceName,
							},
						},
					},
				},
					DispatcherPodAsOwnerReference("p1"),
				),
				{Object: NewDispatcherPod("p1",
					PodRunning(),
					PodAnnotations(map[string]string{
						base.VolumeGenerationAnnotationKey: "1",
					}),
				)},
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: func() runtime.Object {
						c := NewConsumer(1,
							ConsumerUID(ConsumerUUID),
							ConsumerSpec(NewConsumerSpec(
								ConsumerTopics(SourceTopics...),
								ConsumerConfigs(
									ConsumerBootstrapServersConfig(SourceBootstrapServers),
									ConsumerGroupIdConfig(SourceConsumerGroup),
								),
								ConsumerSubscriber(NewSourceSinkReference()),
								ConsumerVReplicas(2),
								ConsumerPlacement(kafkainternals.PodBind{PodName: "p1", PodNamespace: SystemNamespace}),
							)),
							ConsumerOwnerRef(ConsumerGroupAsOwnerRef()),
						)
						c.GetConditionSet().Manage(c.GetStatus()).InitializeConditions()
						c.MarkReconcileContractSucceeded()
						c.MarkBindSucceeded()
						c.Status.SubscriberURI, _ = apis.ParseURL(ServiceURL)
						return c
					}(),
				},
			},
		},
		{
			Name: "Reconciled normal - consumer delivery",
			Objects: []runtime.Object{
				NewService(),
				NewConsumerGroup(ConsumerGroupOwnerRef(SourceAsOwnerReference())),
				NewDispatcherPod("p1", PodRunning()),
				NewConsumer(1,
					ConsumerUID(ConsumerUUID),
					ConsumerSpec(NewConsumerSpec(
						ConsumerTopics(SourceTopics...),
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(SourceBootstrapServers),
							ConsumerGroupIdConfig(SourceConsumerGroup),
						),
						ConsumerDelivery(NewConsumerSpecDelivery(internals.Ordered,
							NewConsumerSpecDeliveryDeadLetterSink(),
							NewConsumerRetry(10),
							NewConsumerBackoffPolicy(eventingduck.BackoffPolicyExponential),
							NewConsumerBackoffDelay("PT0.2S"),
							NewConsumerTimeout("PT51S"),
						)),
						ConsumerSubscriber(NewSourceSinkReference()),
						ConsumerVReplicas(1),
						ConsumerPlacement(kafkainternals.PodBind{PodName: "p1", PodNamespace: SystemNamespace}),
					)),
					ConsumerOwnerRef(ConsumerGroupAsOwnerRef()),
				),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			SkipNamespaceValidation: true, // WantCreates compare the broker namespace with configmap namespace, so skip it
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantCreates: []runtime.Object{
				NewConfigMapWithBinaryData(&config.Env{
					DataPlaneConfigFormat:       base.Json,
					DataPlaneConfigMapNamespace: SystemNamespace,
					DataPlaneConfigMapName:      "p1",
				},
					[]byte(""),
					DispatcherPodAsOwnerReference("p1"),
				),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&config.Env{
					DataPlaneConfigFormat:       base.Json,
					DataPlaneConfigMapNamespace: SystemNamespace,
					DataPlaneConfigMapName:      "p1",
				}, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ConsumerUUID,
							Topics:           SourceTopics,
							BootstrapServers: SourceBootstrapServers,
							Egresses: []*contract.Egress{{
								ConsumerGroup: SourceConsumerGroup,
								Destination:   ServiceURL,
								ReplyStrategy: nil,
								Filter:        nil,
								Uid:           ConsumerUUID,
								VReplicas:     1,
								EgressConfig: &contract.EgressConfig{
									DeadLetter:    ConsumerDeadLetterSinkURI.String(),
									Retry:         10,
									BackoffPolicy: contract.BackoffPolicy_Exponential,
									BackoffDelay:  200,
									Timeout:       51000,
								},
								DeliveryOrder: contract.DeliveryOrder_ORDERED,
								KeyType:       0,
								Reference: &contract.Reference{
									Uuid:      SourceUUID,
									Namespace: ConsumerNamespace,
									Name:      SourceName,
								},
								FeatureFlags: defaultContractFeatureFlags,
							}},
							Auth:                nil,
							CloudEventOverrides: nil,
							Reference: &contract.Reference{
								Uuid:      SourceUUID,
								Namespace: ConsumerNamespace,
								Name:      SourceName,
							},
						},
					},
				},
					DispatcherPodAsOwnerReference("p1"),
				),
				{Object: NewDispatcherPod("p1",
					PodRunning(),
					PodAnnotations(map[string]string{
						base.VolumeGenerationAnnotationKey: "1",
					}),
				)},
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: func() runtime.Object {
						c := NewConsumer(1,
							ConsumerUID(ConsumerUUID),
							ConsumerSpec(NewConsumerSpec(
								ConsumerTopics(SourceTopics...),
								ConsumerConfigs(
									ConsumerBootstrapServersConfig(SourceBootstrapServers),
									ConsumerGroupIdConfig(SourceConsumerGroup),
								),
								ConsumerDelivery(NewConsumerSpecDelivery(internals.Ordered,
									NewConsumerSpecDeliveryDeadLetterSink(),
									NewConsumerRetry(10),
									NewConsumerBackoffPolicy(eventingduck.BackoffPolicyExponential),
									NewConsumerBackoffDelay("PT0.2S"),
									NewConsumerTimeout("PT51S"),
								)),
								ConsumerSubscriber(NewSourceSinkReference()),
								ConsumerVReplicas(1),
								ConsumerPlacement(kafkainternals.PodBind{PodName: "p1", PodNamespace: SystemNamespace}),
							)),
							ConsumerOwnerRef(ConsumerGroupAsOwnerRef()),
						)
						c.GetConditionSet().Manage(c.GetStatus()).InitializeConditions()
						c.MarkReconcileContractSucceeded()
						c.MarkBindSucceeded()
						c.Status.SubscriberURI, _ = apis.ParseURL(ServiceURL)
						c.Status.DeliveryStatus.DeadLetterSinkURI = ConsumerDeadLetterSinkURI
						return c
					}(),
				},
			},
		},
		{
			Name: "Pod not found",
			Objects: []runtime.Object{
				NewService(),
				NewConsumerGroup(ConsumerGroupOwnerRef(SourceAsOwnerReference())),
				NewConsumer(1,
					ConsumerUID(ConsumerUUID),
					ConsumerSpec(NewConsumerSpec(
						ConsumerTopics(SourceTopics...),
						ConsumerConfigs(
							ConsumerBootstrapServersConfig(SourceBootstrapServers),
							ConsumerGroupIdConfig(SourceConsumerGroup),
						),
						ConsumerSubscriber(NewSourceSinkReference()),
						ConsumerVReplicas(1),
						ConsumerPlacement(kafkainternals.PodBind{PodName: "p1", PodNamespace: SystemNamespace}),
					)),
					ConsumerOwnerRef(ConsumerGroupAsOwnerRef()),
				),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			SkipNamespaceValidation: true, // WantCreates compare the broker namespace with configmap namespace, so skip it
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: func() runtime.Object {
						c := NewConsumer(1,
							ConsumerUID(ConsumerUUID),
							ConsumerSpec(NewConsumerSpec(
								ConsumerTopics(SourceTopics...),
								ConsumerConfigs(
									ConsumerBootstrapServersConfig(SourceBootstrapServers),
									ConsumerGroupIdConfig(SourceConsumerGroup),
								),
								ConsumerSubscriber(NewSourceSinkReference()),
								ConsumerVReplicas(1),
								ConsumerPlacement(kafkainternals.PodBind{PodName: "p1", PodNamespace: SystemNamespace}),
							)),
							ConsumerOwnerRef(ConsumerGroupAsOwnerRef()),
						)
						c.GetConditionSet().Manage(c.GetStatus()).InitializeConditions()
						c.MarkReconcileContractSucceeded()
						c.MarkBindInProgress()
						c.Status.SubscriberURI, _ = apis.ParseURL(ServiceURL)
						return c
					}(),
				},
			},
		},
	}

	table.Test(t, NewFactory(nil, func(ctx context.Context, listers *Listers, env *config.Env, row *TableRow) controller.Reconciler {

		r := &Reconciler{
			SerDe:               contract.FormatSerDe{Format: contract.Json},
			Resolver:            resolver.NewURIResolverFromTracker(ctx, tracker.New(func(name types.NamespacedName) {}, 0)),
			Tracker:             &FakeTracker{},
			ConsumerGroupLister: listers.GetConsumerGroupLister(),
			SecretLister:        listers.GetSecretLister(),
			PodLister:           listers.GetPodLister(),
			KubeClient:          kubeclient.Get(ctx),
			KafkaFeatureFlags:   configapis.DefaultFeaturesConfig(),
		}

		return creconciler.NewReconciler(
			ctx,
			logging.FromContext(ctx),
			fakekafkainternalsclient.Get(ctx),
			listers.GetConsumerLister(),
			controller.GetEventRecorder(ctx),
			r,
		)
	}))
}

func patchFinalizers() clientgotesting.PatchActionImpl {
	action := clientgotesting.PatchActionImpl{}
	action.Name = ConsumerName
	action.Namespace = ConsumerNamespace
	patch := `{"metadata":{"finalizers":["consumers.internal.kafka.eventing.knative.dev"],"resourceVersion":""}}`
	action.Patch = []byte(patch)
	return action
}
