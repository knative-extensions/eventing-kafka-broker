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

package source

import (
	"context"
	"fmt"
	"testing"

	"knative.dev/eventing/pkg/apis/feature"
	"knative.dev/eventing/pkg/auth"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgotesting "k8s.io/client-go/testing"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	cm "knative.dev/pkg/configmap/testing"
	"knative.dev/pkg/kmeta"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/apis/bindings/v1beta1"
	bindings "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/bindings/v1beta1"
	configapis "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/config"

	kafkainternals "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing/v1alpha1"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"

	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	. "knative.dev/pkg/reconciler/testing"

	sources "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/sources/v1beta1"
	fakeeventingkafkaclient "knative.dev/eventing-kafka-broker/control-plane/pkg/client/injection/client/fake"
	eventingkafkasourcereconciler "knative.dev/eventing-kafka-broker/control-plane/pkg/client/injection/reconciler/sources/v1beta1/kafkasource"

	fakeconsumergroupinformer "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/client/fake"

	. "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/testing"

	kedaclient "knative.dev/eventing-kafka-broker/third_party/pkg/client/injection/client/fake"

	fakekubeclient "knative.dev/pkg/client/injection/kube/client/fake"
)

const (
	finalizerName = "kafkasources.sources.knative.dev"

	enableKEDA = "enable-keda"
)

var (
	finalizerUpdatedEvent = Eventf(
		corev1.EventTypeNormal,
		"FinalizerUpdate",
		fmt.Sprintf(`Updated %q finalizers`, SourceName),
	)
)

func TestGetLabels(t *testing.T) {

	testLabels := GetLabels("testSourceName")

	wantLabels := map[string]string{
		"eventing.knative.dev/source":     "kafka-source-controller",
		"eventing.knative.dev/sourceName": "testSourceName",
	}

	eq := cmp.Equal(testLabels, wantLabels)
	if !eq {
		t.Fatalf("%v is not equal to %v", testLabels, wantLabels)
	}
}

func TestGetLabelsAsSelector(t *testing.T) {

	testLabels, err := GetLabelsAsSelector("testSourceName")
	if err != nil {
		t.Fatalf("Unable to get labels as selector")
	}
	testLabelsString := testLabels.String()

	wantLabels := "eventing.knative.dev/source=kafka-source-controller,eventing.knative.dev/sourceName=testSourceName"

	if testLabelsString != wantLabels {
		t.Fatalf("%v is not equal to %v", testLabels, wantLabels)
	}
}

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
					WithConsumerGroupFinalizer(),
					WithConsumerGroupName(SourceUUID),
					WithConsumerGroupNamespace(SourceNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(NewSource())),
					WithConsumerGroupMetaLabels(OwnerAsSourceLabel),
					WithConsumerGroupLabels(ConsumerSourceLabel),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(SourceTopics[0], SourceTopics[1]),
						ConsumerConfigs(
							ConsumerGroupIdConfig(SourceConsumerGroup),
							ConsumerBootstrapServersConfig(SourceBootstrapServers),
						),
						ConsumerAuth(NewConsumerSpecAuth()),
						ConsumerDelivery(
							NewConsumerSpecDelivery(
								sources.Ordered,
								NewConsumerTimeout("PT600S"),
								NewConsumerRetry(10),
								NewConsumerBackoffDelay("PT0.3S"),
								NewConsumerBackoffPolicy(eventingduck.BackoffPolicyExponential),
								ConsumerInitialOffset(sources.OffsetLatest),
							),
						),
						ConsumerSubscriber(NewSourceSinkReference()),
						ConsumerReply(ConsumerNoReply()),
					)),
					ConsumerGroupReplicas(1),
				),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSource(
						StatusSourceConsumerGroupUnknown(),
						StatusSourceSinkResolved(""),
						StatusSourceSelector(),
						StatusSourceOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
		},
		{
			Name: "Reconciled normal, offset earliest",
			Objects: []runtime.Object{
				NewSource(WithInitialOffset(sources.OffsetEarliest)),
			},
			Key: testKey,
			WantCreates: []runtime.Object{
				NewConsumerGroup(
					WithConsumerGroupFinalizer(),
					WithConsumerGroupName(SourceUUID),
					WithConsumerGroupNamespace(SourceNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(NewSource())),
					WithConsumerGroupMetaLabels(OwnerAsSourceLabel),
					WithConsumerGroupLabels(ConsumerSourceLabel),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(SourceTopics[0], SourceTopics[1]),
						ConsumerConfigs(
							ConsumerGroupIdConfig(SourceConsumerGroup),
							ConsumerBootstrapServersConfig(SourceBootstrapServers),
						),
						ConsumerAuth(NewConsumerSpecAuth()),
						ConsumerDelivery(
							NewConsumerSpecDelivery(
								sources.Ordered,
								NewConsumerTimeout("PT600S"),
								NewConsumerRetry(10),
								NewConsumerBackoffDelay("PT0.3S"),
								NewConsumerBackoffPolicy(eventingduck.BackoffPolicyExponential),
								ConsumerInitialOffset(sources.OffsetEarliest),
							),
						),
						ConsumerSubscriber(NewSourceSinkReference()),
						ConsumerReply(ConsumerNoReply()),
					)),
					ConsumerGroupReplicas(1),
				),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSource(
						WithInitialOffset(sources.OffsetEarliest),
						StatusSourceConsumerGroupUnknown(),
						StatusSourceSinkResolved(""),
						StatusSourceSelector(),
						StatusSourceOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
		},
		{
			Name: "Reconciled normal, offset latest",
			Objects: []runtime.Object{
				NewSource(WithInitialOffset(sources.OffsetLatest)),
			},
			Key: testKey,
			WantCreates: []runtime.Object{
				NewConsumerGroup(
					WithConsumerGroupFinalizer(),
					WithConsumerGroupName(SourceUUID),
					WithConsumerGroupNamespace(SourceNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(NewSource())),
					WithConsumerGroupMetaLabels(OwnerAsSourceLabel),
					WithConsumerGroupLabels(ConsumerSourceLabel),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(SourceTopics[0], SourceTopics[1]),
						ConsumerConfigs(
							ConsumerGroupIdConfig(SourceConsumerGroup),
							ConsumerBootstrapServersConfig(SourceBootstrapServers),
						),
						ConsumerAuth(NewConsumerSpecAuth()),
						ConsumerDelivery(
							NewConsumerSpecDelivery(
								sources.Ordered,
								NewConsumerTimeout("PT600S"),
								NewConsumerRetry(10),
								NewConsumerBackoffDelay("PT0.3S"),
								NewConsumerBackoffPolicy(eventingduck.BackoffPolicyExponential),
								ConsumerInitialOffset(sources.OffsetLatest),
							),
						),
						ConsumerSubscriber(NewSourceSinkReference()),
						ConsumerReply(ConsumerNoReply()),
					)),
					ConsumerGroupReplicas(1),
				),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSource(
						WithInitialOffset(sources.OffsetLatest),
						StatusSourceConsumerGroupUnknown(),
						StatusSourceSinkResolved(""),
						StatusSourceSelector(),
						StatusSourceOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
		},
		{
			Name: "Reconciled normal with delivery spec",
			Objects: []runtime.Object{
				NewSource(
					WithDeliverySpec(),
					WithInitialOffset(sources.OffsetLatest),
				),
			},
			Key: testKey,
			WantCreates: []runtime.Object{
				NewConsumerGroup(
					WithConsumerGroupFinalizer(),
					WithConsumerGroupName(SourceUUID),
					WithConsumerGroupNamespace(SourceNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(NewSource())),
					WithConsumerGroupMetaLabels(OwnerAsSourceLabel),
					WithConsumerGroupLabels(ConsumerSourceLabel),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(SourceTopics[0], SourceTopics[1]),
						ConsumerConfigs(
							ConsumerGroupIdConfig(SourceConsumerGroup),
							ConsumerBootstrapServersConfig(SourceBootstrapServers),
						),
						ConsumerAuth(NewConsumerSpecAuth()),
						ConsumerDelivery(
							NewConsumerSpecDelivery(
								sources.Ordered,
								NewConsumerTimeout(SourceDeliverySpecTimeout),
								NewConsumerRetry(SourceDeliverySpecRetry),
								NewConsumerBackoffDelay(SourceDeliverySpecBackoffDelay),
								NewConsumerBackoffPolicy(SourceDeliverySpecBackoffPolicy),
								NewConsumerSpecDeliveryDeadLetterSink(),
								ConsumerInitialOffset(sources.OffsetLatest),
							),
						),
						ConsumerSubscriber(NewSourceSinkReference()),
						ConsumerReply(ConsumerNoReply()),
					)),
					ConsumerGroupReplicas(1),
				),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSource(
						WithInitialOffset(sources.OffsetLatest),
						WithDeliverySpec(),
						StatusSourceConsumerGroupUnknown(),
						StatusSourceSinkResolved(""),
						StatusSourceSelector(),
						StatusSourceOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
		},
		{
			Name: "Reconciled normal, key type label",
			Objects: []runtime.Object{
				NewSource(WithKeyType("int")),
			},
			Key: testKey,
			WantCreates: []runtime.Object{
				NewConsumerGroup(
					WithConsumerGroupFinalizer(),
					WithConsumerGroupName(SourceUUID),
					WithConsumerGroupNamespace(SourceNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(NewSource())),
					WithConsumerGroupMetaLabels(OwnerAsSourceLabel),
					WithConsumerGroupLabels(ConsumerSourceLabel),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(SourceTopics[0], SourceTopics[1]),
						ConsumerConfigs(
							ConsumerGroupIdConfig(SourceConsumerGroup),
							ConsumerBootstrapServersConfig(SourceBootstrapServers),
							ConsumerKeyTypeConfig("int"),
						),
						ConsumerAuth(NewConsumerSpecAuth()),
						ConsumerDelivery(
							NewConsumerSpecDelivery(
								sources.Ordered,
								NewConsumerTimeout("PT600S"),
								NewConsumerRetry(10),
								NewConsumerBackoffDelay("PT0.3S"),
								NewConsumerBackoffPolicy(eventingduck.BackoffPolicyExponential),
								ConsumerInitialOffset(sources.OffsetLatest),
							),
						),
						ConsumerSubscriber(NewSourceSinkReference()),
						ConsumerReply(ConsumerNoReply()),
					)),
					ConsumerGroupReplicas(1),
				),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSource(
						WithKeyType("int"),
						StatusSourceConsumerGroupUnknown(),
						StatusSourceSinkResolved(""),
						StatusSourceSelector(),
						StatusSourceOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
		},
		{
			Name: "Reconciled normal with SASL with type",
			Objects: []runtime.Object{
				//NewSourceSASL(),
				&sources.KafkaSource{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: SourceNamespace,
						Name:      SourceName,
						UID:       SourceUUID,
					},
					Spec: sources.KafkaSourceSpec{
						KafkaAuthSpec: v1beta1.KafkaAuthSpec{
							BootstrapServers: []string{SourceBootstrapServers},
							Net: v1beta1.KafkaNetSpec{
								SASL: bindings.KafkaSASLSpec{
									Enable: true,
									User: bindings.SecretValueFromSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: SecretName,
											},
											Key: "user",
										},
									},
									Password: bindings.SecretValueFromSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: SecretName,
											},
											Key: "password",
										},
									},
									Type: bindings.SecretValueFromSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: SecretName,
											},
											Key: "type",
										},
									},
								},
								TLS: bindings.KafkaTLSSpec{
									Enable: true,
								},
							},
						},
						Topics:        SourceTopics,
						ConsumerGroup: SourceConsumerGroup,
						SourceSpec: duckv1.SourceSpec{
							Sink: NewSourceSinkReference(),
						},
					},
				},
			},
			Key: testKey,
			WantCreates: []runtime.Object{
				NewConsumerGroup(
					WithConsumerGroupFinalizer(),
					WithConsumerGroupName(SourceUUID),
					WithConsumerGroupNamespace(SourceNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(NewSource())),
					WithConsumerGroupMetaLabels(OwnerAsSourceLabel),
					WithConsumerGroupLabels(ConsumerSourceLabel),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(SourceTopics[0], SourceTopics[1]),
						ConsumerConfigs(
							ConsumerGroupIdConfig(SourceConsumerGroup),
							ConsumerBootstrapServersConfig(SourceBootstrapServers),
						),
						ConsumerAuth(&kafkainternals.Auth{
							NetSpec: &bindings.KafkaNetSpec{
								SASL: bindings.KafkaSASLSpec{
									Enable: true,
									User: bindings.SecretValueFromSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: SecretName,
											},
											Key: "user",
										},
									},
									Password: bindings.SecretValueFromSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: SecretName,
											},
											Key: "password",
										},
									},
									Type: bindings.SecretValueFromSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: SecretName,
											},
											Key: "type",
										},
									},
								},
								TLS: bindings.KafkaTLSSpec{
									Enable: true,
								},
							},
						}),
						ConsumerDelivery(
							NewConsumerSpecDelivery(
								sources.Ordered,
								NewConsumerTimeout("PT600S"),
								NewConsumerRetry(10),
								NewConsumerBackoffDelay("PT0.3S"),
								NewConsumerBackoffPolicy(eventingduck.BackoffPolicyExponential),
								ConsumerInitialOffset(sources.OffsetLatest),
							),
						),
						ConsumerSubscriber(NewSourceSinkReference()),
						ConsumerReply(ConsumerNoReply()),
					)),
					ConsumerGroupReplicas(1),
				),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSource(
						StatusSourceConsumerGroupUnknown(),
						StatusSourceSinkResolved(""),
						SourceNetSaslTls(true),
						StatusSourceSelector(),
						StatusSourceOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
		},
		{
			Name: "Reconciled normal with SASL without type",
			Objects: []runtime.Object{
				//NewSourceSASL(),
				&sources.KafkaSource{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: SourceNamespace,
						Name:      SourceName,
						UID:       SourceUUID,
					},
					Spec: sources.KafkaSourceSpec{
						KafkaAuthSpec: v1beta1.KafkaAuthSpec{
							BootstrapServers: []string{SourceBootstrapServers},
							Net: v1beta1.KafkaNetSpec{
								SASL: bindings.KafkaSASLSpec{
									Enable: true,
									User: bindings.SecretValueFromSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: SecretName,
											},
											Key: "user",
										},
									},
									Password: bindings.SecretValueFromSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: SecretName,
											},
											Key: "password",
										},
									},
								},
								TLS: bindings.KafkaTLSSpec{
									Enable: true,
								},
							},
						},
						Topics:        SourceTopics,
						ConsumerGroup: SourceConsumerGroup,
						SourceSpec: duckv1.SourceSpec{
							Sink: NewSourceSinkReference(),
						},
					},
				},
			},
			Key: testKey,
			WantCreates: []runtime.Object{
				NewConsumerGroup(
					WithConsumerGroupFinalizer(),
					WithConsumerGroupName(SourceUUID),
					WithConsumerGroupNamespace(SourceNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(NewSource())),
					WithConsumerGroupMetaLabels(OwnerAsSourceLabel),
					WithConsumerGroupLabels(ConsumerSourceLabel),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(SourceTopics[0], SourceTopics[1]),
						ConsumerConfigs(
							ConsumerGroupIdConfig(SourceConsumerGroup),
							ConsumerBootstrapServersConfig(SourceBootstrapServers),
						),
						ConsumerAuth(&kafkainternals.Auth{
							NetSpec: &bindings.KafkaNetSpec{
								SASL: bindings.KafkaSASLSpec{
									Enable: true,
									User: bindings.SecretValueFromSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: SecretName,
											},
											Key: "user",
										},
									},
									Password: bindings.SecretValueFromSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: SecretName,
											},
											Key: "password",
										},
									},
								},
								TLS: bindings.KafkaTLSSpec{
									Enable: true,
								},
							},
						}),
						ConsumerDelivery(
							NewConsumerSpecDelivery(
								sources.Ordered,
								NewConsumerTimeout("PT600S"),
								NewConsumerRetry(10),
								NewConsumerBackoffDelay("PT0.3S"),
								NewConsumerBackoffPolicy(eventingduck.BackoffPolicyExponential),
								ConsumerInitialOffset(sources.OffsetLatest),
							),
						),
						ConsumerSubscriber(NewSourceSinkReference()),
						ConsumerReply(ConsumerNoReply()),
					)),
					ConsumerGroupReplicas(1),
				),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSource(
						StatusSourceConsumerGroupUnknown(),
						StatusSourceSinkResolved(""),
						SourceNetSaslTls(false),
						StatusSourceSelector(),
						StatusSourceOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
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
					WithConsumerGroupFinalizer(),
					WithConsumerGroupName(SourceUUID),
					WithConsumerGroupNamespace(SourceNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(NewSource())),
					WithConsumerGroupMetaLabels(OwnerAsSourceLabel),
					WithConsumerGroupLabels(ConsumerSourceLabel),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(SourceTopics[0], SourceTopics[1]),
						ConsumerConfigs(
							ConsumerGroupIdConfig(SourceConsumerGroup),
							ConsumerBootstrapServersConfig(SourceBootstrapServers),
						),
						ConsumerAuth(NewConsumerSpecAuth()),
						ConsumerDelivery(
							NewConsumerSpecDelivery(
								sources.Ordered,
								NewConsumerTimeout("PT600S"),
								NewConsumerRetry(10),
								NewConsumerBackoffDelay("PT0.3S"),
								NewConsumerBackoffPolicy(eventingduck.BackoffPolicyExponential),
								ConsumerInitialOffset(sources.OffsetLatest),
							),
						),
						ConsumerSubscriber(NewSourceSinkReference()),
						ConsumerReply(ConsumerNoReply()),
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
						StatusSourceConsumerGroupUnknown(),
						StatusSourceSinkResolved(""),
						StatusSourceSelector(),
						StatusSourceOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
		},
		{
			Name: "Reconciled normal - with autoscaling annotations",
			Objects: []runtime.Object{
				NewSource(WithAutoscalingAnnotationsSource()),
			},
			Key: testKey,
			WantCreates: []runtime.Object{
				NewConsumerGroup(
					WithConsumerGroupFinalizer(),
					WithConsumerGroupName(SourceUUID),
					WithConsumerGroupNamespace(SourceNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(NewSource())),
					WithConsumerGroupMetaLabels(OwnerAsSourceLabel),
					WithConsumerGroupLabels(ConsumerSourceLabel),
					WithConsumerGroupAnnotations(ConsumerGroupAnnotations),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(SourceTopics[0], SourceTopics[1]),
						ConsumerConfigs(
							ConsumerGroupIdConfig(SourceConsumerGroup),
							ConsumerBootstrapServersConfig(SourceBootstrapServers),
						),
						ConsumerAuth(NewConsumerSpecAuth()),
						ConsumerDelivery(
							NewConsumerSpecDelivery(
								sources.Ordered,
								NewConsumerTimeout("PT600S"),
								NewConsumerRetry(10),
								NewConsumerBackoffDelay("PT0.3S"),
								NewConsumerBackoffPolicy(eventingduck.BackoffPolicyExponential),
								ConsumerInitialOffset(sources.OffsetLatest),
							),
						),
						ConsumerSubscriber(NewSourceSinkReference()),
						ConsumerReply(ConsumerNoReply()),
					)),
					ConsumerGroupReplicas(1),
				),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSource(
						StatusSourceConsumerGroupUnknown(),
						StatusSourceSinkResolved(""),
						StatusSourceSelector(),
						WithAutoscalingAnnotationsSource(),
						StatusSourceOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
		},
		{
			Name: "Reconciled normal - existing cg with sink update",
			Objects: []runtime.Object{
				NewSource(
					WithSourceSink(NewSourceSink2Reference()),
					WithSourceConsumers(1),
				),
				NewConsumerGroup(
					WithConsumerGroupName(SourceUUID),
					WithConsumerGroupNamespace(SourceNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(NewSource())),
					WithConsumerGroupMetaLabels(OwnerAsSourceLabel),
					WithConsumerGroupLabels(ConsumerSourceLabel),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(SourceTopics[0], SourceTopics[1]),
						ConsumerConfigs(
							ConsumerGroupIdConfig(SourceConsumerGroup),
							ConsumerBootstrapServersConfig(SourceBootstrapServers),
						),
						ConsumerAuth(NewConsumerSpecAuth()),
						ConsumerDelivery(
							NewConsumerSpecDelivery(
								sources.Ordered,
								NewConsumerTimeout("PT600S"),
								NewConsumerRetry(10),
								NewConsumerBackoffDelay("PT0.3S"),
								NewConsumerBackoffPolicy(eventingduck.BackoffPolicyExponential),
								ConsumerInitialOffset(sources.OffsetLatest),
							),
						),
						ConsumerSubscriber(NewSourceSinkReference()),
						ConsumerReply(ConsumerNoReply()),
					)),
					ConsumerGroupReplicas(1),
					ConsumerGroupReady,
				),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewConsumerGroup(
						WithConsumerGroupName(SourceUUID),
						WithConsumerGroupNamespace(SourceNamespace),
						WithConsumerGroupOwnerRef(kmeta.NewControllerRef(NewSource())),
						WithConsumerGroupMetaLabels(OwnerAsSourceLabel),
						WithConsumerGroupLabels(ConsumerSourceLabel),
						ConsumerGroupConsumerSpec(NewConsumerSpec(
							ConsumerTopics(SourceTopics[0], SourceTopics[1]),
							ConsumerConfigs(
								ConsumerGroupIdConfig(SourceConsumerGroup),
								ConsumerBootstrapServersConfig(SourceBootstrapServers),
							),
							ConsumerAuth(NewConsumerSpecAuth()),
							ConsumerDelivery(
								NewConsumerSpecDelivery(
									sources.Ordered,
									NewConsumerTimeout("PT600S"),
									NewConsumerRetry(10),
									NewConsumerBackoffDelay("PT0.3S"),
									NewConsumerBackoffPolicy(eventingduck.BackoffPolicyExponential),
									ConsumerInitialOffset(sources.OffsetLatest),
								),
							),
							ConsumerSubscriber(NewSourceSink2Reference()),
							ConsumerReply(ConsumerNoReply()),
						)),
						ConsumerGroupReady,
						ConsumerGroupReplicas(1),
					),
				},
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSource(
						WithSourceSink(NewSourceSink2Reference()),
						WithSourceConsumers(1),
						StatusSourceConsumerGroup(),
						StatusSourceSinkResolved(""),
						StatusSourceSelector(),
						StatusSourceOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
		},
		{
			Name: "Reconciled normal - existing cg with update",
			Objects: []runtime.Object{
				NewSource(WithSourceConsumers(1)),
				NewConsumerGroup(
					WithConsumerGroupName(SourceUUID),
					WithConsumerGroupNamespace(SourceNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(NewSource())),
					WithConsumerGroupMetaLabels(OwnerAsSourceLabel),
					WithConsumerGroupLabels(ConsumerSourceLabel),
					ConsumerGroupReplicas(1),
					ConsumerGroupReady,
				),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewConsumerGroup(
						WithConsumerGroupName(SourceUUID),
						WithConsumerGroupNamespace(SourceNamespace),
						WithConsumerGroupOwnerRef(kmeta.NewControllerRef(NewSource())),
						WithConsumerGroupMetaLabels(OwnerAsSourceLabel),
						WithConsumerGroupLabels(ConsumerSourceLabel),
						ConsumerGroupConsumerSpec(NewConsumerSpec(
							ConsumerTopics(SourceTopics[0], SourceTopics[1]),
							ConsumerConfigs(
								ConsumerGroupIdConfig(SourceConsumerGroup),
								ConsumerBootstrapServersConfig(SourceBootstrapServers),
							),
							ConsumerAuth(NewConsumerSpecAuth()),
							ConsumerDelivery(
								NewConsumerSpecDelivery(
									sources.Ordered,
									NewConsumerTimeout("PT600S"),
									NewConsumerRetry(10),
									NewConsumerBackoffDelay("PT0.3S"),
									NewConsumerBackoffPolicy(eventingduck.BackoffPolicyExponential),
									ConsumerInitialOffset(sources.OffsetLatest),
								),
							),
							ConsumerSubscriber(NewSourceSinkReference()),
							ConsumerReply(ConsumerNoReply()),
						)),
						ConsumerGroupReady,
						ConsumerGroupReplicas(1),
					),
				},
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSource(
						WithSourceConsumers(1),
						StatusSourceConsumerGroup(),
						StatusSourceSinkResolved(""),
						StatusSourceSelector(),
						StatusSourceOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
		},
		{
			Name: "Reconciled normal - existing cg with annotations update",
			Objects: []runtime.Object{
				NewSource(WithAutoscalingAnnotationsSource()),
				NewConsumerGroup(
					WithConsumerGroupName(SourceUUID),
					WithConsumerGroupNamespace(SourceNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(NewSource())),
					WithConsumerGroupMetaLabels(OwnerAsSourceLabel),
					WithConsumerGroupLabels(ConsumerSourceLabel),
					ConsumerGroupReplicas(1),
					ConsumerGroupReady,
				),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewConsumerGroup(
						WithConsumerGroupName(SourceUUID),
						WithConsumerGroupNamespace(SourceNamespace),
						WithConsumerGroupOwnerRef(kmeta.NewControllerRef(NewSource())),
						WithConsumerGroupMetaLabels(OwnerAsSourceLabel),
						WithConsumerGroupLabels(ConsumerSourceLabel),
						WithConsumerGroupAnnotations(ConsumerGroupAnnotations),
						ConsumerGroupConsumerSpec(NewConsumerSpec(
							ConsumerTopics(SourceTopics[0], SourceTopics[1]),
							ConsumerConfigs(
								ConsumerGroupIdConfig(SourceConsumerGroup),
								ConsumerBootstrapServersConfig(SourceBootstrapServers),
							),
							ConsumerAuth(NewConsumerSpecAuth()),
							ConsumerDelivery(
								NewConsumerSpecDelivery(
									sources.Ordered,
									NewConsumerTimeout("PT600S"),
									NewConsumerRetry(10),
									NewConsumerBackoffDelay("PT0.3S"),
									NewConsumerBackoffPolicy(eventingduck.BackoffPolicyExponential),
									ConsumerInitialOffset(sources.OffsetLatest),
								),
							),
							ConsumerSubscriber(NewSourceSinkReference()),
							ConsumerReply(ConsumerNoReply()),
						)),
						ConsumerGroupReady,
						ConsumerGroupReplicas(1),
					),
				},
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSource(
						StatusSourceConsumerGroup(),
						StatusSourceSinkResolved(""),
						StatusSourceSelector(),
						WithAutoscalingAnnotationsSource(),
						StatusSourceOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
		},
		{
			Name: "Reconciled normal - existing cg with update but not ready",
			Objects: []runtime.Object{
				NewSource(WithSourceConsumers(1)),
				NewConsumerGroup(
					WithConsumerGroupName(SourceUUID),
					WithConsumerGroupNamespace(SourceNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(NewSource())),
					WithConsumerGroupMetaLabels(OwnerAsSourceLabel),
					WithConsumerGroupLabels(ConsumerSourceLabel),
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
						WithConsumerGroupMetaLabels(OwnerAsSourceLabel),
						WithConsumerGroupLabels(ConsumerSourceLabel),
						ConsumerGroupConsumerSpec(NewConsumerSpec(
							ConsumerTopics(SourceTopics[0], SourceTopics[1]),
							ConsumerConfigs(
								ConsumerGroupIdConfig(SourceConsumerGroup),
								ConsumerBootstrapServersConfig(SourceBootstrapServers),
							),
							ConsumerAuth(NewConsumerSpecAuth()),
							ConsumerDelivery(
								NewConsumerSpecDelivery(
									sources.Ordered,
									NewConsumerTimeout("PT600S"),
									NewConsumerRetry(10),
									NewConsumerBackoffDelay("PT0.3S"),
									NewConsumerBackoffPolicy(eventingduck.BackoffPolicyExponential),
									ConsumerInitialOffset(sources.OffsetLatest),
								),
							),
							ConsumerSubscriber(NewSourceSinkReference()),
							ConsumerReply(ConsumerNoReply()),
						)),
						ConsumerGroupReplicas(1),
					),
				},
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSource(
						WithSourceConsumers(1),
						StatusSourceConsumerGroupUnknown(),
						StatusSourceSinkResolved(""),
						StatusSourceSelector(),
						StatusSourceOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
		},
		{
			Name: "Reconciled normal - existing cg without update",
			Objects: []runtime.Object{
				NewSource(WithAutoscalingAnnotationsSource()),
				NewConsumerGroup(
					WithConsumerGroupName(SourceUUID),
					WithConsumerGroupNamespace(SourceNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(NewSource())),
					WithConsumerGroupMetaLabels(OwnerAsSourceLabel),
					WithConsumerGroupLabels(ConsumerSourceLabel),
					WithConsumerGroupAnnotations(ConsumerGroupAnnotations),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(SourceTopics[0], SourceTopics[1]),
						ConsumerConfigs(
							ConsumerGroupIdConfig(SourceConsumerGroup),
							ConsumerBootstrapServersConfig(SourceBootstrapServers),
						),
						ConsumerAuth(NewConsumerSpecAuth()),
						ConsumerDelivery(
							NewConsumerSpecDelivery(
								sources.Ordered,
								NewConsumerTimeout("PT600S"),
								NewConsumerRetry(10),
								NewConsumerBackoffDelay("PT0.3S"),
								NewConsumerBackoffPolicy(eventingduck.BackoffPolicyExponential),
								ConsumerInitialOffset(sources.OffsetLatest),
							),
						),
						ConsumerSubscriber(NewSourceSinkReference()),
						ConsumerReply(ConsumerNoReply()),
					)),
					ConsumerGroupReplicas(1),
					ConsumerGroupReady,
				),
			},
			Key: testKey,
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSource(
						StatusSourceConsumerGroup(),
						StatusSourceSinkResolved(""),
						StatusSourceSelector(),
						WithAutoscalingAnnotationsSource(),
						StatusSourceOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
		},
		{
			Name: "Reconciled normal - existing cg without update but not ready",
			Objects: []runtime.Object{
				NewSource(),
				NewConsumerGroup(
					WithConsumerGroupName(SourceUUID),
					WithConsumerGroupNamespace(SourceNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(NewSource())),
					WithConsumerGroupMetaLabels(OwnerAsSourceLabel),
					WithConsumerGroupLabels(ConsumerSourceLabel),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(SourceTopics[0], SourceTopics[1]),
						ConsumerConfigs(
							ConsumerGroupIdConfig(SourceConsumerGroup),
							ConsumerBootstrapServersConfig(SourceBootstrapServers),
						),
						ConsumerAuth(NewConsumerSpecAuth()),
						ConsumerDelivery(
							NewConsumerSpecDelivery(
								sources.Ordered,
								NewConsumerTimeout("PT600S"),
								NewConsumerRetry(10),
								NewConsumerBackoffDelay("PT0.3S"),
								NewConsumerBackoffPolicy(eventingduck.BackoffPolicyExponential),
								ConsumerInitialOffset(sources.OffsetLatest),
							),
						),
						ConsumerSubscriber(NewSourceSinkReference()),
						ConsumerReply(ConsumerNoReply()),
					)),
					ConsumerGroupReplicas(1),
				),
			},
			Key:         testKey,
			WantCreates: []runtime.Object{},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSource(
						StatusSourceConsumerGroupUnknown(),
						StatusSourceSinkResolved(""),
						StatusSourceSelector(),
						StatusSourceOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
		},
		{
			Name: "Reconciled normal - existing cg but failed",
			Objects: []runtime.Object{
				NewSource(),
				NewConsumerGroup(
					WithConsumerGroupName(SourceUUID),
					WithConsumerGroupNamespace(SourceNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(NewSource())),
					WithConsumerGroupMetaLabels(OwnerAsSourceLabel),
					WithConsumerGroupLabels(ConsumerSourceLabel),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(SourceTopics[0], SourceTopics[1]),
						ConsumerConfigs(
							ConsumerGroupIdConfig(SourceConsumerGroup),
							ConsumerBootstrapServersConfig(SourceBootstrapServers),
						),
						ConsumerAuth(NewConsumerSpecAuth()),
						ConsumerDelivery(
							NewConsumerSpecDelivery(
								sources.Ordered,
								NewConsumerTimeout("PT600S"),
								NewConsumerRetry(10),
								NewConsumerBackoffDelay("PT0.3S"),
								NewConsumerBackoffPolicy(eventingduck.BackoffPolicyExponential),
								ConsumerInitialOffset(sources.OffsetLatest),
							),
						),
						ConsumerSubscriber(NewSourceSinkReference()),
						ConsumerReply(ConsumerNoReply()),
					)),
					ConsumerGroupReplicas(1),
					WithConsumerGroupFailed("failed", "failed"),
				),
			},
			Key:         testKey,
			WantCreates: []runtime.Object{},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSource(
						StatusSourceConsumerGroupFailed("failed", "failed"),
						StatusSourceSinkResolved(""),
						StatusSourceSelector(),
						StatusSourceOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
		},
		{
			Name: "Reconciled normal - existing cg with replicas set in status",
			Objects: []runtime.Object{
				NewSource(),
				NewConsumerGroup(
					WithConsumerGroupName(SourceUUID),
					WithConsumerGroupNamespace(SourceNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(NewSource())),
					WithConsumerGroupMetaLabels(OwnerAsSourceLabel),
					WithConsumerGroupLabels(ConsumerSourceLabel),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(SourceTopics[0], SourceTopics[1]),
						ConsumerConfigs(
							ConsumerGroupIdConfig(SourceConsumerGroup),
							ConsumerBootstrapServersConfig(SourceBootstrapServers),
						),
						ConsumerAuth(NewConsumerSpecAuth()),
						ConsumerDelivery(
							NewConsumerSpecDelivery(
								sources.Ordered,
								NewConsumerTimeout("PT600S"),
								NewConsumerRetry(10),
								NewConsumerBackoffDelay("PT0.3S"),
								NewConsumerBackoffPolicy(eventingduck.BackoffPolicyExponential),
								ConsumerInitialOffset(sources.OffsetLatest),
							),
						),
						ConsumerSubscriber(NewSourceSinkReference()),
						ConsumerReply(ConsumerNoReply()),
					)),
					ConsumerGroupReplicas(1),
					ConsumerGroupReplicasStatus(1),
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
						StatusSourceConsumerGroupReplicas(1),
						StatusSourceSelector(),
						StatusSourceOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
		},
		{
			Name: "Reconciled normal - update cg replicas",
			Objects: []runtime.Object{
				NewSource(WithSourceConsumers(3)),
				NewConsumerGroup(
					WithConsumerGroupName(SourceUUID),
					WithConsumerGroupNamespace(SourceNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(NewSource())),
					WithConsumerGroupMetaLabels(OwnerAsSourceLabel),
					WithConsumerGroupLabels(ConsumerSourceLabel),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(SourceTopics[0], SourceTopics[1]),
						ConsumerConfigs(
							ConsumerGroupIdConfig(SourceConsumerGroup),
							ConsumerBootstrapServersConfig(SourceBootstrapServers),
						),
						ConsumerAuth(NewConsumerSpecAuth()),
						ConsumerDelivery(
							NewConsumerSpecDelivery(
								sources.Ordered,
								NewConsumerTimeout("PT600S"),
								NewConsumerRetry(10),
								NewConsumerBackoffDelay("PT0.3S"),
								NewConsumerBackoffPolicy(eventingduck.BackoffPolicyExponential),
								ConsumerInitialOffset(sources.OffsetLatest),
							),
						),
						ConsumerSubscriber(NewSourceSinkReference()),
						ConsumerReply(ConsumerNoReply()),
					)),
					ConsumerGroupReplicas(2),
					ConsumerGroupReplicasStatus(1),
					ConsumerGroupReady,
				),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewConsumerGroup(
						WithConsumerGroupName(SourceUUID),
						WithConsumerGroupNamespace(SourceNamespace),
						WithConsumerGroupOwnerRef(kmeta.NewControllerRef(NewSource())),
						WithConsumerGroupMetaLabels(OwnerAsSourceLabel),
						WithConsumerGroupLabels(ConsumerSourceLabel),
						ConsumerGroupConsumerSpec(NewConsumerSpec(
							ConsumerTopics(SourceTopics[0], SourceTopics[1]),
							ConsumerConfigs(
								ConsumerGroupIdConfig(SourceConsumerGroup),
								ConsumerBootstrapServersConfig(SourceBootstrapServers),
							),
							ConsumerAuth(NewConsumerSpecAuth()),
							ConsumerDelivery(
								NewConsumerSpecDelivery(
									sources.Ordered,
									NewConsumerTimeout("PT600S"),
									NewConsumerRetry(10),
									NewConsumerBackoffDelay("PT0.3S"),
									NewConsumerBackoffPolicy(eventingduck.BackoffPolicyExponential),
									ConsumerInitialOffset(sources.OffsetLatest),
								),
							),
							ConsumerSubscriber(NewSourceSinkReference()),
							ConsumerReply(ConsumerNoReply()),
						)),
						ConsumerGroupReplicas(3),
						ConsumerGroupReplicasStatus(1),
						ConsumerGroupReady,
					),
				},
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSource(
						WithSourceConsumers(3),
						StatusSourceConsumerGroup(),
						StatusSourceSinkResolved(""),
						StatusSourceConsumerGroupReplicas(1),
						StatusSourceSelector(),
						StatusSourceOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
		},
		{
			Name: "Reconciled normal - ignore source replicas when KEDA is enabled",
			Objects: []runtime.Object{
				NewSource(WithSourceConsumers(3)),
				NewConsumerGroup(
					WithConsumerGroupName(SourceUUID),
					WithConsumerGroupNamespace(SourceNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(NewSource())),
					WithConsumerGroupMetaLabels(OwnerAsSourceLabel),
					WithConsumerGroupLabels(ConsumerSourceLabel),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(SourceTopics[0], SourceTopics[1]),
						ConsumerConfigs(
							ConsumerGroupIdConfig(SourceConsumerGroup),
							ConsumerBootstrapServersConfig(SourceBootstrapServers),
						),
						ConsumerAuth(NewConsumerSpecAuth()),
						ConsumerDelivery(
							NewConsumerSpecDelivery(
								sources.Ordered,
								NewConsumerTimeout("PT600S"),
								NewConsumerRetry(10),
								NewConsumerBackoffDelay("PT0.3S"),
								NewConsumerBackoffPolicy(eventingduck.BackoffPolicyExponential),
								ConsumerInitialOffset(sources.OffsetLatest),
							),
						),
						ConsumerSubscriber(NewSourceSinkReference()),
						ConsumerReply(ConsumerNoReply()),
					)),
					ConsumerGroupReplicas(2),
					ConsumerGroupReplicasStatus(1),
					ConsumerGroupReady,
				),
			},
			WithReactors: []clientgotesting.ReactionFunc{
				ReactorKEDAEnabled(),
			},
			Key: testKey,
			OtherTestData: map[string]interface{}{
				enableKEDA: true,
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSource(
						WithSourceConsumers(3),
						StatusSourceConsumerGroup(),
						StatusSourceSinkResolved(""),
						StatusSourceConsumerGroupReplicas(1),
						StatusSourceSelector(),
						StatusSourceOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
		},
		{
			Name: "Reconciled normal - with unordered consumer verticle",
			Objects: []runtime.Object{
				NewSource(WithOrdering(sources.Unordered)),
			},
			Key: testKey,
			WantCreates: []runtime.Object{
				NewConsumerGroup(
					WithConsumerGroupFinalizer(),
					WithConsumerGroupName(SourceUUID),
					WithConsumerGroupNamespace(SourceNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(NewSource())),
					WithConsumerGroupMetaLabels(OwnerAsSourceLabel),
					WithConsumerGroupLabels(ConsumerSourceLabel),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(SourceTopics[0], SourceTopics[1]),
						ConsumerConfigs(
							ConsumerGroupIdConfig(SourceConsumerGroup),
							ConsumerBootstrapServersConfig(SourceBootstrapServers),
						),
						ConsumerAuth(NewConsumerSpecAuth()),
						ConsumerDelivery(
							NewConsumerSpecDelivery(
								sources.Unordered,
								NewConsumerTimeout("PT600S"),
								NewConsumerRetry(10),
								NewConsumerBackoffDelay("PT0.3S"),
								NewConsumerBackoffPolicy(eventingduck.BackoffPolicyExponential),
								ConsumerInitialOffset(sources.OffsetLatest),
							),
						),
						ConsumerSubscriber(NewSourceSinkReference()),
						ConsumerReply(ConsumerNoReply()),
					)),
					ConsumerGroupReplicas(1),
				),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSource(
						WithOrdering(sources.Unordered),
						StatusSourceConsumerGroupUnknown(),
						StatusSourceSinkResolved(""),
						StatusSourceSelector(),
						StatusSourceOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
		},
		{
			Name: "Reconciled normal - with ordered consumer verticle",
			Objects: []runtime.Object{
				NewSource(WithOrdering(sources.Ordered)),
			},
			Key: testKey,
			WantCreates: []runtime.Object{
				NewConsumerGroup(
					WithConsumerGroupFinalizer(),
					WithConsumerGroupName(SourceUUID),
					WithConsumerGroupNamespace(SourceNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(NewSource())),
					WithConsumerGroupMetaLabels(OwnerAsSourceLabel),
					WithConsumerGroupLabels(ConsumerSourceLabel),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(SourceTopics[0], SourceTopics[1]),
						ConsumerConfigs(
							ConsumerGroupIdConfig(SourceConsumerGroup),
							ConsumerBootstrapServersConfig(SourceBootstrapServers),
						),
						ConsumerAuth(NewConsumerSpecAuth()),
						ConsumerDelivery(
							NewConsumerSpecDelivery(
								sources.Ordered,
								NewConsumerTimeout("PT600S"),
								NewConsumerRetry(10),
								NewConsumerBackoffDelay("PT0.3S"),
								NewConsumerBackoffPolicy(eventingduck.BackoffPolicyExponential),
								ConsumerInitialOffset(sources.OffsetLatest),
							),
						),
						ConsumerSubscriber(NewSourceSinkReference()),
						ConsumerReply(ConsumerNoReply()),
					)),
					ConsumerGroupReplicas(1),
				),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSource(
						WithOrdering(sources.Ordered),
						StatusSourceConsumerGroupUnknown(),
						StatusSourceSinkResolved(""),
						StatusSourceSelector(),
						StatusSourceOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled(),
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
		},
		{
			Name: "Reconciled normal - with OIDC enabled",
			Ctx: feature.ToContext(context.Background(), feature.Flags{
				feature.OIDCAuthentication: feature.Enabled,
			}),
			Objects: []runtime.Object{
				NewSource(),
			},
			Key: testKey,
			WantCreates: []runtime.Object{
				makeKafkaSourceOIDCServiceAccount(),
				NewConsumerGroup(
					WithConsumerGroupFinalizer(),
					WithConsumerGroupName(SourceUUID),
					WithConsumerGroupNamespace(SourceNamespace),
					WithConsumerGroupOwnerRef(kmeta.NewControllerRef(NewSource())),
					WithConsumerGroupMetaLabels(OwnerAsSourceLabel),
					WithConsumerGroupLabels(ConsumerSourceLabel),
					ConsumerGroupConsumerSpec(NewConsumerSpec(
						ConsumerTopics(SourceTopics[0], SourceTopics[1]),
						ConsumerConfigs(
							ConsumerGroupIdConfig(SourceConsumerGroup),
							ConsumerBootstrapServersConfig(SourceBootstrapServers),
						),
						ConsumerAuth(NewConsumerSpecAuth()),
						ConsumerDelivery(
							NewConsumerSpecDelivery(
								sources.Ordered,
								NewConsumerTimeout("PT600S"),
								NewConsumerRetry(10),
								NewConsumerBackoffDelay("PT0.3S"),
								NewConsumerBackoffPolicy(eventingduck.BackoffPolicyExponential),
								ConsumerInitialOffset(sources.OffsetLatest),
							),
						),
						ConsumerSubscriber(NewSourceSinkReference()),
						ConsumerReply(ConsumerNoReply()),
					)),
					ConsumerGroupReplicas(1),
				),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSource(
						StatusSourceConsumerGroupUnknown(),
						StatusSourceSinkResolved(""),
						StatusSourceSelector(),
						StatusSourceOIDCIdentityCreatedSucceeded(),
						StatusSourceOIDCIdentity(makeKafkaSourceOIDCServiceAccount().Name),
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
		},
	}

	table.Test(t, NewFactory(nil, func(ctx context.Context, listers *Listers, env *config.Env, row *TableRow) controller.Reconciler {
		store := configapis.NewStore(ctx)
		_, exampleConfig := cm.ConfigMapsFromTestFile(t, configapis.FlagsConfigName)
		store.OnConfigChanged(exampleConfig)

		if shouldEnableKeda, ok := row.OtherTestData[enableKEDA]; ok && shouldEnableKeda.(bool) == true {
			store.OnConfigChanged(&corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      configapis.FlagsConfigName,
					Namespace: SystemNamespace,
				},
				Data: map[string]string{
					"controller.autoscaler": "Enabled",
				},
			})
		}

		reconciler := &Reconciler{
			ConsumerGroupLister:  listers.GetConsumerGroupLister(),
			InternalsClient:      fakeconsumergroupinformer.Get(ctx),
			KedaClient:           kedaclient.Get(ctx),
			KafkaFeatureFlags:    configapis.DefaultFeaturesConfig(),
			ServiceAccountLister: listers.GetServiceAccountLister(),
			KubeClient:           fakekubeclient.Get(ctx),
		}

		reconciler.KafkaFeatureFlags = configapis.FromContext(store.ToContext(ctx))

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

func StatusSourceSelector() KRShapedOption {
	return func(obj duckv1.KRShaped) {
		ks := obj.(*sources.KafkaSource)
		ks.Status.Selector = "eventing.knative.dev/source=kafka-source-controller,eventing.knative.dev/sourceName=" + ks.Name
	}
}

func StatusSourceConsumerGroup() KRShapedOption {
	return func(obj duckv1.KRShaped) {
		ks := obj.(*sources.KafkaSource)
		ks.GetConditionSet().Manage(ks.GetStatus()).MarkTrue(KafkaConditionConsumerGroup)
	}
}

func StatusSourceConsumerGroupFailed(reason string, msg string) KRShapedOption {
	return func(obj duckv1.KRShaped) {
		ks := obj.(*sources.KafkaSource)
		ks.GetConditionSet().Manage(ks.GetStatus()).MarkFalse(KafkaConditionConsumerGroup, reason, msg)
	}
}

func StatusSourceConsumerGroupUnknown() KRShapedOption {
	return func(obj duckv1.KRShaped) {
		ks := obj.(*sources.KafkaSource)
		ks.GetConditionSet().Manage(ks.GetStatus()).MarkUnknown(KafkaConditionConsumerGroup, "failed to reconcile consumer group", "consumer group is not ready")
	}
}

func StatusSourceConsumerGroupReplicas(replicas int32) KRShapedOption {
	return func(obj duckv1.KRShaped) {
		ks := obj.(*sources.KafkaSource)
		ks.Status.Consumers = replicas
	}
}

func SourceNetSaslTls(withType bool) KRShapedOption {
	return func(obj duckv1.KRShaped) {
		ks := obj.(*sources.KafkaSource)
		ks.Spec.Net = bindings.KafkaNetSpec{
			SASL: bindings.KafkaSASLSpec{
				Enable: true,
				User: bindings.SecretValueFromSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: SecretName,
						},
						Key: "user",
					},
				},
				Password: bindings.SecretValueFromSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: SecretName,
						},
						Key: "password",
					},
				},
				// Type: bindings.SecretValueFromSource{
				// 	SecretKeyRef: &corev1.SecretKeySelector{
				// 		LocalObjectReference: corev1.LocalObjectReference{
				// 			Name: SecretName,
				// 		},
				// 		Key: "type",
				// 	},
				// },
			},
			TLS: bindings.KafkaTLSSpec{
				Enable: true,
			},
		}
		if withType {
			ks.Spec.Net.SASL.Type = bindings.SecretValueFromSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: SecretName,
					},
					Key: "type",
				},
			}
		}
	}
}

func patchFinalizers() clientgotesting.PatchActionImpl {
	action := clientgotesting.PatchActionImpl{}
	action.Name = SourceName
	action.Namespace = SourceNamespace
	patch := `{"metadata":{"finalizers":["` + finalizerName + `"],"resourceVersion":""}}`
	action.Patch = []byte(patch)
	return action
}

func makeKafkaSourceOIDCServiceAccount() *corev1.ServiceAccount {
	return auth.GetOIDCServiceAccountForResource(sources.SchemeGroupVersion.WithKind("KafkaSource"), metav1.ObjectMeta{
		Name:      SourceName,
		Namespace: SourceNamespace,
		UID:       SourceUUID,
	})
}
