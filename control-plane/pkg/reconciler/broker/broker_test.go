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
	"net/url"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"

	"k8s.io/utils/pointer"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	kafkatesting "knative.dev/eventing-kafka-broker/control-plane/pkg/kafka/testing"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/prober"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/prober/probertesting"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"

	"github.com/Shopify/sarama"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgotesting "k8s.io/client-go/testing"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1"
	"knative.dev/pkg/apis"
	kubeclient "knative.dev/pkg/client/injection/kube/client/fake"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	. "knative.dev/pkg/reconciler/testing"
	"knative.dev/pkg/resolver"
	"knative.dev/pkg/tracker"

	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	fakeeventingclient "knative.dev/eventing/pkg/client/injection/client/fake"
	brokerreconciler "knative.dev/eventing/pkg/client/injection/reconciler/eventing/v1/broker"
	reconcilertesting "knative.dev/eventing/pkg/reconciler/testing/v1"
	duckv1 "knative.dev/pkg/apis/duck/v1"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/receiver"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	. "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/broker"
	. "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/testing"
)

const (
	wantErrorOnCreateTopic = "wantErrorOnCreateTopic"
	wantErrorOnDeleteTopic = "wantErrorOnDeleteTopic"
	ExpectedTopicDetail    = "expectedTopicDetail"
	testProber             = "testProber"
	externalTopic          = "externalTopic"
)

const (
	finalizerName = "brokers.eventing.knative.dev"

	bootstrapServers = "kafka-1:9092,kafka-2:9093"
)

var (
	finalizerUpdatedEvent = Eventf(
		corev1.EventTypeNormal,
		"FinalizerUpdate",
		fmt.Sprintf(`Updated %q finalizers`, BrokerName),
	)

	createTopicError = fmt.Errorf("failed to create topic")
	deleteTopicError = fmt.Errorf("failed to delete topic")

	linear      = eventingduck.BackoffPolicyLinear
	exponential = eventingduck.BackoffPolicyExponential
)

var DefaultEnv = &config.Env{
	DataPlaneConfigMapNamespace: "knative-eventing",
	DataPlaneConfigMapName:      "kafka-broker-brokers-triggers",
	GeneralConfigMapName:        "kafka-broker-config",
	IngressName:                 "kafka-broker-ingress",
	SystemNamespace:             "knative-eventing",
	DataPlaneConfigFormat:       base.Json,
	DefaultBackoffDelayMs:       1000,
}

func TestBrokerReconciler(t *testing.T) {
	eventing.RegisterAlternateBrokerConditionSet(base.IngressConditionSet)

	t.Parallel()

	for _, f := range Formats {
		brokerReconciliation(t, f, *DefaultEnv)
	}
}

func brokerReconciliation(t *testing.T, format string, env config.Env) {

	testKey := fmt.Sprintf("%s/%s", BrokerNamespace, BrokerName)

	env.DataPlaneConfigFormat = format

	table := TableTest{
		{
			Name: "Reconciled normal - no DLS",
			Objects: []runtime.Object{
				NewBroker(),
				BrokerConfig(bootstrapServers, 20, 5),
				NewConfigMapWithBinaryData(&env, nil),
				NewService(),
				BrokerReceiverPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				BrokerDispatcherPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
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
				}),
				BrokerReceiverPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				BrokerDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
					"annotation_to_preserve":           "value_to_preserve",
				}),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewBroker(
						reconcilertesting.WithInitBrokerConditions,
						StatusBrokerConfigMapUpdatedReady(&env),
						StatusBrokerDataPlaneAvailable,
						StatusBrokerConfigParsed,
						StatusBrokerTopicReady,
						BrokerAddressable(&env),
						StatusBrokerProbeSucceeded,
						BrokerConfigMapAnnotations(),
					),
				},
			},
		}, {
			Name: "Reconciled normal - with external topic",
			Objects: []runtime.Object{
				NewBroker(
					WithExternalTopic(ExternalTopicName),
				),
				BrokerConfig(bootstrapServers, 20, 5),
				NewConfigMapWithBinaryData(&env, nil),
				NewService(),
				BrokerReceiverPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				BrokerDispatcherPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:              BrokerUUID,
							Topics:           []string{ExternalTopicName},
							Ingress:          &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							BootstrapServers: bootstrapServers,
							Reference:        BrokerReference(),
						},
					},
					Generation: 1,
				}),
				BrokerReceiverPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				BrokerDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
					"annotation_to_preserve":           "value_to_preserve",
				}),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewBroker(
						WithExternalTopic(ExternalTopicName),
						reconcilertesting.WithInitBrokerConditions,
						StatusBrokerConfigMapUpdatedReady(&env),
						StatusBrokerDataPlaneAvailable,
						StatusBrokerConfigParsed,
						StatusExternalBrokerTopicReady(ExternalTopicName),
						BrokerAddressable(&env),
						StatusBrokerProbeSucceeded,
						BrokerConfigMapAnnotations(),
					),
				},
			},

			OtherTestData: map[string]interface{}{
				externalTopic: ExternalTopicName,
			},
		},
		{
			Name: "external topic not present or invalid",
			Objects: []runtime.Object{
				NewBroker(
					WithExternalTopic("my-not-present-topic"),
				),
				BrokerConfig(bootstrapServers, 20, 5),
				BrokerReceiverPod(env.SystemNamespace, nil),
				BrokerDispatcherPod(env.SystemNamespace, nil),
			},
			Key:     testKey,
			WantErr: true,
			WantEvents: []string{
				finalizerUpdatedEvent,
				Eventf(
					corev1.EventTypeWarning,
					"InternalError",
					"topics %v not present or invalid: invalid topic %s",
					[]string{"my-not-present-topic"}, "my-not-present-topic",
				),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewBroker(
						WithExternalTopic("my-not-present-topic"),
						reconcilertesting.WithInitBrokerConditions,
						StatusBrokerDataPlaneAvailable,
						StatusBrokerConfigParsed,
						StatusExternalBrokerTopicNotPresentOrInvalid("my-not-present-topic"),
						BrokerConfigMapAnnotations(),
					),
				},
			},
			OtherTestData: map[string]interface{}{
				externalTopic: "my-not-present-topic",
			},
		},
		{
			Name: "Reconciled failed - probe " + prober.StatusNotReady.String(),
			Objects: []runtime.Object{
				NewBroker(),
				BrokerConfig(bootstrapServers, 20, 5),
				NewConfigMapWithBinaryData(&env, nil),
				NewService(),
				BrokerReceiverPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				BrokerDispatcherPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
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
				}),
				BrokerReceiverPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				BrokerDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
					"annotation_to_preserve":           "value_to_preserve",
				}),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewBroker(
						reconcilertesting.WithInitBrokerConditions,
						StatusBrokerConfigMapUpdatedReady(&env),
						StatusBrokerDataPlaneAvailable,
						StatusBrokerConfigParsed,
						StatusBrokerTopicReady,
						StatusBrokerProbeFailed(prober.StatusNotReady),
						BrokerConfigMapAnnotations(),
					),
				},
			},
			OtherTestData: map[string]interface{}{
				testProber: probertesting.MockProber(prober.StatusNotReady),
			},
		},
		{
			Name: "Reconciled failed - probe " + prober.StatusUnknown.String(),
			Objects: []runtime.Object{
				NewBroker(),
				BrokerConfig(bootstrapServers, 20, 5),
				NewConfigMapWithBinaryData(&env, nil),
				NewService(),
				BrokerReceiverPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				BrokerDispatcherPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
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
				}),
				BrokerReceiverPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				BrokerDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
					"annotation_to_preserve":           "value_to_preserve",
				}),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewBroker(
						reconcilertesting.WithInitBrokerConditions,
						StatusBrokerConfigMapUpdatedReady(&env),
						StatusBrokerDataPlaneAvailable,
						StatusBrokerConfigParsed,
						StatusBrokerTopicReady,
						BrokerConfigMapAnnotations(),
						StatusBrokerProbeFailed(prober.StatusUnknown),
					),
				},
			},
			OtherTestData: map[string]interface{}{
				testProber: probertesting.MockProber(prober.StatusUnknown),
			},
		},
		{
			Name: "Reconciled normal - with DLS",
			Objects: []runtime.Object{
				NewBroker(
					WithDelivery(),
				),
				BrokerConfig(bootstrapServers, 20, 5),
				NewConfigMapFromContract(&contract.Contract{
					Generation: 1,
				}, &env),
				NewService(),
				BrokerReceiverPod(env.SystemNamespace, map[string]string{base.VolumeGenerationAnnotationKey: "3"}),
				BrokerDispatcherPod(env.SystemNamespace, map[string]string{base.VolumeGenerationAnnotationKey: "1"}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:              BrokerUUID,
							Topics:           []string{BrokerTopic()},
							Ingress:          &contract.Ingress{ContentMode: contract.ContentMode_BINARY, Path: receiver.Path(BrokerNamespace, BrokerName)},
							BootstrapServers: bootstrapServers,
							EgressConfig:     &contract.EgressConfig{DeadLetter: ServiceURL},
							Reference:        BrokerReference(),
						},
					},
					Generation: 2,
				}),
				BrokerReceiverPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "2",
				}),
				BrokerDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "2",
				}),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewBroker(
						WithDelivery(),
						reconcilertesting.WithInitBrokerConditions,
						StatusBrokerConfigMapUpdatedReady(&env),
						StatusBrokerDataPlaneAvailable,
						StatusBrokerTopicReady,
						BrokerConfigMapAnnotations(),
						StatusBrokerConfigParsed,
						BrokerAddressable(&env),
						StatusBrokerProbeSucceeded,
						BrokerDLSResolved(ServiceURL),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - with DLS - no DLS ref namespace",
			Objects: []runtime.Object{
				NewBroker(
					WithDelivery(WithNoDeadLetterSinkNamespace),
				),
				BrokerConfig(bootstrapServers, 20, 5),
				NewConfigMapFromContract(&contract.Contract{
					Generation: 1,
				}, &env),
				NewService(WithServiceNamespace(BrokerNamespace)),
				BrokerReceiverPod(env.SystemNamespace, map[string]string{base.VolumeGenerationAnnotationKey: "3"}),
				BrokerDispatcherPod(env.SystemNamespace, map[string]string{base.VolumeGenerationAnnotationKey: "1"}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:              BrokerUUID,
							Topics:           []string{BrokerTopic()},
							Ingress:          &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							BootstrapServers: bootstrapServers,
							EgressConfig:     &contract.EgressConfig{DeadLetter: ServiceURLFrom(BrokerNamespace, ServiceName)},
							Reference:        BrokerReference(),
						},
					},
					Generation: 2,
				}),
				BrokerReceiverPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "2",
				}),
				BrokerDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "2",
				}),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewBroker(
						WithDelivery(WithNoDeadLetterSinkNamespace),
						reconcilertesting.WithInitBrokerConditions,
						StatusBrokerConfigMapUpdatedReady(&env),
						StatusBrokerDataPlaneAvailable,
						StatusBrokerTopicReady,
						BrokerConfigMapAnnotations(),
						StatusBrokerConfigParsed,
						BrokerAddressable(&env),
						StatusBrokerProbeSucceeded,
						BrokerDLSResolved(ServiceURLFrom(BrokerNamespace, ServiceName)),
					),
				},
			},
		},
		{
			Name: "Failed to create topic",
			Objects: []runtime.Object{
				NewBroker(),
				BrokerConfig(bootstrapServers, 20, 5),
				BrokerReceiverPod(env.SystemNamespace, nil),
				BrokerDispatcherPod(env.SystemNamespace, nil),
			},
			Key:     testKey,
			WantErr: true,
			WantEvents: []string{
				finalizerUpdatedEvent,
				Eventf(
					corev1.EventTypeWarning,
					"InternalError",
					"failed to create topic: %s: %v",
					BrokerTopic(), createTopicError,
				),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewBroker(
						reconcilertesting.WithInitBrokerConditions,
						StatusBrokerDataPlaneAvailable,
						StatusBrokerConfigParsed,
						StatusBrokerFailedToCreateTopic,
						BrokerConfigMapAnnotations(),
					),
				},
			},
			OtherTestData: map[string]interface{}{
				wantErrorOnCreateTopic: createTopicError,
			},
		},
		{
			Name: "Config map not found - create config map",
			Objects: []runtime.Object{
				NewBroker(
					WithDelivery(),
				),
				BrokerConfig(bootstrapServers, 20, 5),
				NewService(),
				BrokerReceiverPod(env.SystemNamespace, map[string]string{base.VolumeGenerationAnnotationKey: "2"}),
				BrokerDispatcherPod(env.SystemNamespace, map[string]string{base.VolumeGenerationAnnotationKey: "2"}),
				&corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: env.DataPlaneConfigMapNamespace,
						Name:      env.DataPlaneConfigMapName + "a", // Use a different name
					},
				},
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			SkipNamespaceValidation: true, // WantCreates compare the broker namespace with configmap namespace, so skip it
			WantCreates: []runtime.Object{
				NewConfigMapWithBinaryData(&env, nil),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:              BrokerUUID,
							Topics:           []string{BrokerTopic()},
							Ingress:          &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							BootstrapServers: bootstrapServers,
							EgressConfig:     &contract.EgressConfig{DeadLetter: ServiceURL},
							Reference:        BrokerReference(),
						},
					},
					Generation: 1,
				}),
				BrokerReceiverPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
				BrokerDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewBroker(
						WithDelivery(),
						reconcilertesting.WithInitBrokerConditions,
						StatusBrokerConfigParsed,
						StatusBrokerDataPlaneAvailable,
						StatusBrokerConfigMapUpdatedReady(&env),
						StatusBrokerTopicReady,
						BrokerConfigMapAnnotations(),
						BrokerAddressable(&env),
						StatusBrokerProbeSucceeded,
						BrokerDLSResolved(ServiceURL),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - config map not readable",
			Objects: []runtime.Object{
				NewBroker(),
				BrokerConfig(bootstrapServers, 20, 5),
				NewConfigMapWithBinaryData(&env, []byte(`{"hello": "world"}`)),
				NewService(),
				BrokerReceiverPod(env.SystemNamespace, nil),
				BrokerDispatcherPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "2",
				}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
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
				}),
				BrokerReceiverPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
				BrokerDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewBroker(
						reconcilertesting.WithInitBrokerConditions,
						StatusBrokerDataPlaneAvailable,
						StatusBrokerConfigMapUpdatedReady(&env),
						StatusBrokerConfigParsed,
						StatusBrokerTopicReady,
						BrokerConfigMapAnnotations(),
						BrokerAddressable(&env),
						StatusBrokerProbeSucceeded,
					),
				},
			},
		},
		{
			Name: "Reconciled normal - preserve config map previous state",
			Objects: []runtime.Object{
				NewBroker(),
				BrokerConfig(bootstrapServers, 20, 5),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:          "5384faa4-6bdf-428d-b6c2-d6f89ce1d44b",
							Topics:       []string{"my-existing-topic-a"},
							Ingress:      &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							EgressConfig: &contract.EgressConfig{DeadLetter: "http://www.my-sink.com"},
						},
						{
							Uid:          "5384faa4-6bdf-428d-b6c2-d6f89ce1d44a",
							Topics:       []string{"my-existing-topic-b"},
							EgressConfig: &contract.EgressConfig{DeadLetter: "http://www.my-sink.com"},
						},
					},
				}, &env),
				NewService(),
				BrokerReceiverPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "2",
				}),
				BrokerDispatcherPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "2",
				}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:          "5384faa4-6bdf-428d-b6c2-d6f89ce1d44b",
							Topics:       []string{"my-existing-topic-a"},
							Ingress:      &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							EgressConfig: &contract.EgressConfig{DeadLetter: "http://www.my-sink.com"},
						},
						{
							Uid:          "5384faa4-6bdf-428d-b6c2-d6f89ce1d44a",
							Topics:       []string{"my-existing-topic-b"},
							EgressConfig: &contract.EgressConfig{DeadLetter: "http://www.my-sink.com"},
						},
						{
							Uid:              BrokerUUID,
							Topics:           []string{BrokerTopic()},
							Ingress:          &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							BootstrapServers: bootstrapServers,
							Reference:        BrokerReference(),
						},
					},
					Generation: 1,
				}),
				BrokerReceiverPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
				BrokerDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewBroker(
						reconcilertesting.WithInitBrokerConditions,
						StatusBrokerDataPlaneAvailable,
						StatusBrokerConfigMapUpdatedReady(&env),
						StatusBrokerConfigParsed,
						StatusBrokerTopicReady,
						BrokerConfigMapAnnotations(),
						BrokerAddressable(&env),
						StatusBrokerProbeSucceeded,
					),
				},
			},
		},
		{
			Name: "Reconciled normal - update existing broker while preserving others",
			Objects: []runtime.Object{
				NewBroker(
					func(broker *eventing.Broker) {
						broker.Spec.Delivery = &eventingduck.DeliverySpec{
							DeadLetterSink: &duckv1.Destination{
								URI: &apis.URL{
									Scheme: "http",
									Host:   "www.my-sink.com",
									Path:   "/api",
								},
							},
						}
					},
				),
				BrokerConfig(bootstrapServers, 20, 5),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:          "5384faa4-6bdf-428d-b6c2-d6f89ce1d44b",
							Topics:       []string{"my-existing-topic-a"},
							EgressConfig: &contract.EgressConfig{DeadLetter: "http://www.my-sink.com"},
						},
						{
							Uid:          BrokerUUID,
							Topics:       []string{BrokerTopic()},
							EgressConfig: &contract.EgressConfig{DeadLetter: "http://www.my-sink.com"},
						},
					},
				}, &env),
				NewService(),
				BrokerReceiverPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "5",
				}),
				BrokerDispatcherPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "5",
				}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:          "5384faa4-6bdf-428d-b6c2-d6f89ce1d44b",
							Topics:       []string{"my-existing-topic-a"},
							EgressConfig: &contract.EgressConfig{DeadLetter: "http://www.my-sink.com"},
						},
						{
							Uid:              BrokerUUID,
							Topics:           []string{BrokerTopic()},
							Ingress:          &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							BootstrapServers: bootstrapServers,
							EgressConfig:     &contract.EgressConfig{DeadLetter: "http://www.my-sink.com/api"},
							Reference:        BrokerReference(),
						},
					},
					Generation: 1,
				}),
				BrokerReceiverPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
				BrokerDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewBroker(
						func(broker *eventing.Broker) {
							broker.Spec.Delivery = &eventingduck.DeliverySpec{
								DeadLetterSink: &duckv1.Destination{
									URI: func() *apis.URL {
										URL, _ := url.Parse("http://www.my-sink.com/api")
										return (*apis.URL)(URL)
									}(),
								},
							}
						},
						reconcilertesting.WithInitBrokerConditions,
						StatusBrokerDataPlaneAvailable,
						StatusBrokerConfigMapUpdatedReady(&env),
						StatusBrokerConfigParsed,
						StatusBrokerTopicReady,
						BrokerConfigMapAnnotations(),
						BrokerAddressable(&env),
						StatusBrokerProbeSucceeded,
						BrokerDLSResolved("http://www.my-sink.com/api"),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - remove existing broker DLS while preserving others",
			Objects: []runtime.Object{
				NewBroker(
					func(broker *eventing.Broker) {
						broker.Spec.Delivery = &eventingduck.DeliverySpec{}
					},
				),
				BrokerConfig(bootstrapServers, 20, 5),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:          "5384faa4-6bdf-428d-b6c2-d6f89ce1d44b",
							Topics:       []string{"my-existing-topic-a"},
							Ingress:      &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							EgressConfig: &contract.EgressConfig{DeadLetter: "http://www.my-sink.com"},
						},
						{
							Uid:          BrokerUUID,
							Topics:       []string{BrokerTopic()},
							Ingress:      &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							EgressConfig: &contract.EgressConfig{DeadLetter: "http://www.my-sink.com"},
						},
					},
				}, &env),
				NewService(),
				BrokerReceiverPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "5",
				}),
				BrokerDispatcherPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "5",
				}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:          "5384faa4-6bdf-428d-b6c2-d6f89ce1d44b",
							Topics:       []string{"my-existing-topic-a"},
							Ingress:      &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							EgressConfig: &contract.EgressConfig{DeadLetter: "http://www.my-sink.com"},
						},
						{
							Uid:              BrokerUUID,
							Topics:           []string{BrokerTopic()},
							Ingress:          &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							BootstrapServers: bootstrapServers,
							Reference:        BrokerReference(),
						},
					},
					Generation: 1,
				}),
				BrokerReceiverPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
				BrokerDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewBroker(
						func(broker *eventing.Broker) {
							broker.Spec.Delivery = &eventingduck.DeliverySpec{}
						},
						reconcilertesting.WithInitBrokerConditions,
						StatusBrokerDataPlaneAvailable,
						StatusBrokerConfigMapUpdatedReady(&env),
						StatusBrokerConfigParsed,
						StatusBrokerTopicReady,
						BrokerConfigMapAnnotations(),
						BrokerAddressable(&env),
						StatusBrokerProbeSucceeded,
					),
				},
			},
		},
		{
			Name: "Reconciled normal - increment volume generation",
			Objects: []runtime.Object{
				NewBroker(),
				BrokerConfig(bootstrapServers, 20, 5),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:          "5384faa4-6bdf-428d-b6c2-d6f89ce1d44b",
							Topics:       []string{"my-existing-topic-a"},
							Ingress:      &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							EgressConfig: &contract.EgressConfig{DeadLetter: "http://www.my-sink.com"},
						},
						{
							Uid:     BrokerUUID,
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
						},
					},
					Generation: 1,
				}, &env),
				NewService(),
				BrokerReceiverPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "5",
				}),
				BrokerDispatcherPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "5",
				}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:          "5384faa4-6bdf-428d-b6c2-d6f89ce1d44b",
							Topics:       []string{"my-existing-topic-a"},
							Ingress:      &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							EgressConfig: &contract.EgressConfig{DeadLetter: "http://www.my-sink.com"},
						},
						{
							Uid:              BrokerUUID,
							Topics:           []string{BrokerTopic()},
							Ingress:          &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							BootstrapServers: bootstrapServers,
							Reference:        BrokerReference(),
						},
					},
					Generation: 2,
				}),
				BrokerReceiverPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "2",
				}),
				BrokerDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "2",
				}),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewBroker(
						reconcilertesting.WithInitBrokerConditions,
						StatusBrokerDataPlaneAvailable,
						StatusBrokerConfigMapUpdatedReady(&env),
						StatusBrokerConfigParsed,
						StatusBrokerTopicReady,
						BrokerConfigMapAnnotations(),
						BrokerAddressable(&env),
						StatusBrokerProbeSucceeded,
					),
				},
			},
		},
		{
			Name: "Failed to resolve DLS",
			Objects: []runtime.Object{
				NewBroker(
					func(broker *eventing.Broker) {
						broker.Spec.Delivery = &eventingduck.DeliverySpec{
							DeadLetterSink: &duckv1.Destination{},
						}
					},
				),
				BrokerConfig(bootstrapServers, 20, 5),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:          "5384faa4-6bdf-428d-b6c2-d6f89ce1d44b",
							Topics:       []string{"my-existing-topic-a"},
							EgressConfig: &contract.EgressConfig{DeadLetter: "http://www.my-sink.com"},
							Ingress:      &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
						},
						{
							Uid:              BrokerUUID,
							Topics:           []string{BrokerTopic()},
							Ingress:          &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							BootstrapServers: bootstrapServers,
							Reference:        BrokerReference(),
						},
					},
					Generation: 1,
				}, &env),
				BrokerReceiverPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "5",
				}),
				BrokerDispatcherPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "5",
				}),
			},
			Key:     testKey,
			WantErr: true,
			WantEvents: []string{
				finalizerUpdatedEvent,
				Eventf(
					corev1.EventTypeWarning,
					"InternalError",
					"failed to get contract configuration: failed to resolve Spec.Delivery.DeadLetterSink: %v",
					"destination missing Ref and URI, expected at least one",
				),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewBroker(
						func(broker *eventing.Broker) {
							broker.Spec.Delivery = &eventingduck.DeliverySpec{
								DeadLetterSink: &duckv1.Destination{},
							}
						},
						reconcilertesting.WithInitBrokerConditions,
						StatusBrokerDataPlaneAvailable,
						StatusBrokerConfigNotParsed("failed to resolve Spec.Delivery.DeadLetterSink: destination missing Ref and URI, expected at least one"),
						StatusBrokerTopicReady,
						BrokerConfigMapAnnotations(),
					),
				},
			},
		},
		{
			Name: "No bootstrap.servers provided",
			Objects: []runtime.Object{
				NewBroker(),
				BrokerConfig("", 20, 5),
				NewConfigMapFromContract(&contract.Contract{
					Resources:  []*contract.Resource{},
					Generation: 1,
				}, &env),
				BrokerReceiverPod(env.SystemNamespace, nil),
				BrokerDispatcherPod(env.SystemNamespace, nil),
			},
			Key:     testKey,
			WantErr: true,
			WantEvents: []string{
				finalizerUpdatedEvent,
				Eventf(
					corev1.EventTypeWarning,
					"InternalError",
					"failed to get contract configuration: unable to build topic config from configmap: error validating topic config from configmap invalid configuration - numPartitions: 20 - replicationFactor: 5 - bootstrapServers: [] - ConfigMap data: map[bootstrap.servers: default.topic.partitions:20 default.topic.replication.factor:5] - ConfigMap data: map[bootstrap.servers: default.topic.partitions:20 default.topic.replication.factor:5]",
				),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewBroker(
						reconcilertesting.WithInitBrokerConditions,
						StatusBrokerDataPlaneAvailable,
						StatusBrokerConfigNotParsed("unable to build topic config from configmap: error validating topic config from configmap invalid configuration - numPartitions: 20 - replicationFactor: 5 - bootstrapServers: [] - ConfigMap data: map[bootstrap.servers: default.topic.partitions:20 default.topic.replication.factor:5] - ConfigMap data: map[bootstrap.servers: default.topic.partitions:20 default.topic.replication.factor:5]"),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - with broker config",
			Objects: []runtime.Object{
				NewBroker(
					WithBrokerConfig(
						KReference(BrokerConfig(bootstrapServers, 20, 5)),
					),
				),
				BrokerConfig(bootstrapServers, 20, 5),
				NewConfigMapWithBinaryData(&env, nil),
				NewService(),
				BrokerReceiverPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				BrokerDispatcherPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "3",
					"annotation_to_preserve":           "value_to_preserve",
				}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
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
				}),
				BrokerReceiverPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				BrokerDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
					"annotation_to_preserve":           "value_to_preserve",
				}),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewBroker(
						WithBrokerConfig(
							KReference(BrokerConfig(bootstrapServers, 20, 5)),
						),
						reconcilertesting.WithInitBrokerConditions,
						StatusBrokerConfigMapUpdatedReady(&env),
						StatusBrokerDataPlaneAvailable,
						StatusBrokerConfigParsed,
						StatusBrokerTopicReady,
						BrokerConfigMapAnnotations(),
						BrokerAddressable(&env),
						StatusBrokerProbeSucceeded,
					),
				},
			},
			OtherTestData: map[string]interface{}{
				ExpectedTopicDetail: sarama.TopicDetail{
					NumPartitions:     20,
					ReplicationFactor: 5,
				},
			},
		},
		{
			Name: "Reconciled normal - with auth config",
			Objects: []runtime.Object{
				NewBroker(
					WithBrokerConfig(KReference(BrokerConfig(bootstrapServers, 20, 5,
						BrokerAuthConfig("secret-1"),
					))),
				),
				NewSSLSecret(ConfigMapNamespace, "secret-1"),
				BrokerConfig(bootstrapServers, 20, 5, BrokerAuthConfig("secret-1")),
				NewConfigMapWithBinaryData(&env, nil),
				NewService(),
				BrokerReceiverPod(env.SystemNamespace, map[string]string{
					"annotation_to_preserve": "value_to_preserve",
				}),
				BrokerDispatcherPod(env.SystemNamespace, map[string]string{
					"annotation_to_preserve": "value_to_preserve",
				}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				SecretFinalizerUpdate("secret-1", SecretFinalizerName),
				ConfigMapUpdate(&env, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:              BrokerUUID,
							Topics:           []string{BrokerTopic()},
							Ingress:          &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							BootstrapServers: bootstrapServers,
							Reference:        BrokerReference(),
							Auth: &contract.Resource_AuthSecret{
								AuthSecret: &contract.Reference{
									Uuid:      SecretUUID,
									Namespace: ConfigMapNamespace,
									Name:      "secret-1",
									Version:   SecretResourceVersion,
								},
							},
						},
					},
					Generation: 1,
				}),
				BrokerReceiverPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				BrokerDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
					"annotation_to_preserve":           "value_to_preserve",
				}),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewBroker(
						WithBrokerConfig(KReference(BrokerConfig(bootstrapServers, 20, 5,
							BrokerAuthConfig("secret-1"),
						))),
						reconcilertesting.WithInitBrokerConditions,
						StatusBrokerConfigMapUpdatedReady(&env),
						StatusBrokerDataPlaneAvailable,
						StatusBrokerConfigParsed,
						StatusBrokerTopicReady,
						BrokerConfigMapAnnotations(),
						BrokerConfigMapSecretAnnotation("secret-1"),
						BrokerAddressable(&env),
						StatusBrokerProbeSucceeded,
					),
				},
			},
			OtherTestData: map[string]interface{}{
				ExpectedTopicDetail: sarama.TopicDetail{
					NumPartitions:     20,
					ReplicationFactor: 5,
				},
			},
		},
		{
			Name: "Failed to parse broker config - not found",
			Objects: []runtime.Object{
				NewBroker(
					WithBrokerConfig(
						KReference(BrokerConfig(bootstrapServers, 20, 5)),
					),
				),
				BrokerReceiverPod(env.SystemNamespace, nil),
				BrokerDispatcherPod(env.SystemNamespace, nil),
			},
			Key:     testKey,
			WantErr: true,
			WantEvents: []string{
				finalizerUpdatedEvent,
				Eventf(
					corev1.EventTypeWarning,
					"InternalError",
					fmt.Sprintf(`failed to get contract configuration: failed to get configmap %s/%s: configmap %q not found`, ConfigMapNamespace, ConfigMapName, ConfigMapName),
				),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewBroker(
						WithBrokerConfig(
							KReference(BrokerConfig(bootstrapServers, 20, 5)),
						),
						reconcilertesting.WithInitBrokerConditions,
						StatusBrokerDataPlaneAvailable,
						StatusBrokerConfigNotParsed(fmt.Sprintf(`failed to get configmap %s/%s: configmap %q not found`, ConfigMapNamespace, ConfigMapName, ConfigMapName)),
					),
				},
			},
		},
		{
			Name: "Unsupported Kind as config",
			Objects: []runtime.Object{
				NewBroker(
					WithBrokerConfig(&duckv1.KReference{
						Kind:       "Pod",
						Namespace:  BrokerNamespace,
						Name:       BrokerName,
						APIVersion: "v1",
					}),
				),
				&corev1.Pod{
					TypeMeta: metav1.TypeMeta{
						Kind:       "Pod",
						APIVersion: "v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Namespace: BrokerNamespace,
						Name:      BrokerName,
					},
				},
				BrokerReceiverPod(env.SystemNamespace, nil),
				BrokerDispatcherPod(env.SystemNamespace, nil),
			},
			Key:     testKey,
			WantErr: true,
			WantEvents: []string{
				finalizerUpdatedEvent,
				Eventf(
					corev1.EventTypeWarning,
					"InternalError",
					"failed to get contract configuration: supported config Kind: ConfigMap - got Pod",
				),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewBroker(
						WithBrokerConfig(&duckv1.KReference{
							Kind:       "Pod",
							Namespace:  BrokerNamespace,
							Name:       BrokerName,
							APIVersion: "v1",
						}),
						reconcilertesting.WithInitBrokerConditions,
						StatusBrokerDataPlaneAvailable,
						StatusBrokerConfigNotParsed(`supported config Kind: ConfigMap - got Pod`),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - keep all existing triggers",
			Objects: []runtime.Object{
				NewBroker(
					WithDelivery(),
				),
				BrokerConfig(bootstrapServers, 20, 5),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:          BrokerUUID,
							Topics:       []string{BrokerTopic()},
							EgressConfig: &contract.EgressConfig{DeadLetter: ServiceURL},
							Egresses: []*contract.Egress{
								{
									Filter: &contract.Filter{Attributes: map[string]string{
										"source": "source1",
									}},
									Destination:   "http://example.com",
									ConsumerGroup: TriggerUUID,
								},
								{
									Filter: &contract.Filter{Attributes: map[string]string{
										"source": "source1",
									}},
									Destination:   "http://example.com",
									ConsumerGroup: TriggerUUID + "a",
								},
								{
									Filter: &contract.Filter{Attributes: map[string]string{
										"source": "source1",
									}},
									Destination:   "http://example.com",
									ConsumerGroup: TriggerUUID + "b",
								},
							},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
						},
					},
					Generation: 1,
				}, &env),
				NewService(),
				BrokerReceiverPod(env.SystemNamespace, nil),
				BrokerDispatcherPod(env.SystemNamespace, nil),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:    BrokerUUID,
							Topics: []string{BrokerTopic()},
							Egresses: []*contract.Egress{
								{
									Filter: &contract.Filter{Attributes: map[string]string{
										"source": "source1",
									}},
									Destination:   "http://example.com",
									ConsumerGroup: TriggerUUID,
								},
								{
									Filter: &contract.Filter{Attributes: map[string]string{
										"source": "source1",
									}},
									Destination:   "http://example.com",
									ConsumerGroup: TriggerUUID + "a",
								},
								{
									Filter: &contract.Filter{Attributes: map[string]string{
										"source": "source1",
									}},
									Destination:   "http://example.com",
									ConsumerGroup: TriggerUUID + "b",
								},
							},
							EgressConfig:     &contract.EgressConfig{DeadLetter: ServiceURL},
							BootstrapServers: bootstrapServers,
							Reference:        BrokerReference(),
							Ingress:          &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
						},
					},
					Generation: 2,
				}),
				BrokerReceiverPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "2",
				}),
				BrokerDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "2",
				}),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewBroker(
						WithDelivery(),
						reconcilertesting.WithInitBrokerConditions,
						StatusBrokerDataPlaneAvailable,
						StatusBrokerConfigMapUpdatedReady(&env),
						StatusBrokerConfigParsed,
						StatusBrokerTopicReady,
						BrokerConfigMapAnnotations(),
						BrokerAddressable(&env),
						StatusBrokerProbeSucceeded,
						BrokerDLSResolved(ServiceURL),
					),
				},
			},
		},
		{
			Name: "no data plane pods running",
			Objects: []runtime.Object{
				NewBroker(),
				BrokerConfig(bootstrapServers, 20, 5),
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
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewBroker(
						reconcilertesting.WithInitBrokerConditions,
						StatusBrokerDataPlaneNotAvailable,
					),
				},
			},
		},
		{
			Name: "Reconciled normal - with retry config - exponential",
			Objects: []runtime.Object{
				NewBroker(
					WithDelivery(),
					WithRetry(pointer.Int32Ptr(10), &exponential, pointer.StringPtr("PT2S")),
				),
				BrokerConfig(bootstrapServers, 20, 5),
				NewConfigMapFromContract(&contract.Contract{
					Generation: 1,
				}, &env),
				NewService(),
				BrokerReceiverPod(env.SystemNamespace, map[string]string{base.VolumeGenerationAnnotationKey: "1"}),
				BrokerDispatcherPod(env.SystemNamespace, map[string]string{base.VolumeGenerationAnnotationKey: "1"}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:              BrokerUUID,
							Topics:           []string{BrokerTopic()},
							Ingress:          &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							BootstrapServers: bootstrapServers,
							Reference:        BrokerReference(),
							EgressConfig: &contract.EgressConfig{
								DeadLetter:    ServiceURL,
								Retry:         10,
								BackoffPolicy: contract.BackoffPolicy_Exponential,
								BackoffDelay:  2000,
							},
						},
					},
					Generation: 2,
				}),
				BrokerReceiverPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "2",
				}),
				BrokerDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "2",
				}),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewBroker(
						WithDelivery(),
						WithRetry(pointer.Int32Ptr(10), &exponential, pointer.StringPtr("PT2S")),
						reconcilertesting.WithInitBrokerConditions,
						StatusBrokerConfigMapUpdatedReady(&env),
						StatusBrokerDataPlaneAvailable,
						StatusBrokerTopicReady,
						BrokerConfigMapAnnotations(),
						StatusBrokerConfigParsed,
						BrokerAddressable(&env),
						StatusBrokerProbeSucceeded,
						BrokerDLSResolved(ServiceURL),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - with retry config - linear",
			Objects: []runtime.Object{
				NewBroker(
					WithDelivery(),
					WithRetry(pointer.Int32Ptr(10), &linear, pointer.StringPtr("PT2S")),
				),
				BrokerConfig(bootstrapServers, 20, 5),
				NewConfigMapFromContract(&contract.Contract{
					Generation: 1,
				}, &env),
				NewService(),
				BrokerReceiverPod(env.SystemNamespace, map[string]string{base.VolumeGenerationAnnotationKey: "1"}),
				BrokerDispatcherPod(env.SystemNamespace, map[string]string{base.VolumeGenerationAnnotationKey: "0"}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:              BrokerUUID,
							Topics:           []string{BrokerTopic()},
							Ingress:          &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							BootstrapServers: bootstrapServers,
							Reference:        BrokerReference(),
							EgressConfig: &contract.EgressConfig{
								DeadLetter:    ServiceURL,
								Retry:         10,
								BackoffPolicy: contract.BackoffPolicy_Linear,
								BackoffDelay:  2000,
							},
						},
					},
					Generation: 2,
				}),
				BrokerReceiverPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "2",
				}),
				BrokerDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "2",
				}),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewBroker(
						WithDelivery(),
						WithRetry(pointer.Int32Ptr(10), &linear, pointer.StringPtr("PT2S")),
						reconcilertesting.WithInitBrokerConditions,
						StatusBrokerConfigMapUpdatedReady(&env),
						StatusBrokerDataPlaneAvailable,
						StatusBrokerTopicReady,
						BrokerConfigMapAnnotations(),
						StatusBrokerConfigParsed,
						BrokerAddressable(&env),
						StatusBrokerProbeSucceeded,
						BrokerDLSResolved(ServiceURL),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - with no retry num",
			Objects: []runtime.Object{
				NewBroker(
					WithDelivery(),
					WithRetry(nil, &linear, pointer.StringPtr("PT2S")),
				),
				BrokerConfig(bootstrapServers, 20, 5),
				NewConfigMapFromContract(&contract.Contract{
					Generation: 1,
				}, &env),
				NewService(),
				BrokerReceiverPod(env.SystemNamespace, map[string]string{base.VolumeGenerationAnnotationKey: "1"}),
				BrokerDispatcherPod(env.SystemNamespace, map[string]string{base.VolumeGenerationAnnotationKey: "1"}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:              BrokerUUID,
							Topics:           []string{BrokerTopic()},
							Ingress:          &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							BootstrapServers: bootstrapServers,
							Reference:        BrokerReference(),
							EgressConfig: &contract.EgressConfig{
								DeadLetter: ServiceURL,
							},
						},
					},
					Generation: 2,
				}),
				BrokerReceiverPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "2",
				}),
				BrokerDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "2",
				}),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewBroker(
						WithDelivery(),
						WithRetry(nil, &linear, pointer.StringPtr("PT2S")),
						reconcilertesting.WithInitBrokerConditions,
						StatusBrokerConfigMapUpdatedReady(&env),
						StatusBrokerDataPlaneAvailable,
						StatusBrokerTopicReady,
						BrokerConfigMapAnnotations(),
						StatusBrokerConfigParsed,
						BrokerAddressable(&env),
						StatusBrokerProbeSucceeded,
						BrokerDLSResolved(ServiceURL),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - with retry config - no retry delay - use default delay",
			Objects: []runtime.Object{
				NewBroker(
					WithDelivery(),
					WithRetry(pointer.Int32Ptr(10), &linear, nil),
				),
				BrokerConfig(bootstrapServers, 20, 5),
				NewConfigMapFromContract(&contract.Contract{
					Generation: 1,
				}, &env),
				NewService(),
				BrokerReceiverPod(env.SystemNamespace, map[string]string{base.VolumeGenerationAnnotationKey: "1"}),
				BrokerDispatcherPod(env.SystemNamespace, map[string]string{base.VolumeGenerationAnnotationKey: "1"}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:              BrokerUUID,
							Topics:           []string{BrokerTopic()},
							Ingress:          &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							BootstrapServers: bootstrapServers,
							Reference:        BrokerReference(),
							EgressConfig: &contract.EgressConfig{
								DeadLetter:    ServiceURL,
								Retry:         10,
								BackoffPolicy: contract.BackoffPolicy_Linear,
								BackoffDelay:  env.DefaultBackoffDelayMs,
							},
						},
					},
					Generation: 2,
				}),
				BrokerReceiverPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "2",
				}),
				BrokerDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "2",
				}),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewBroker(
						WithDelivery(),
						WithRetry(pointer.Int32Ptr(10), &linear, nil),
						reconcilertesting.WithInitBrokerConditions,
						StatusBrokerConfigMapUpdatedReady(&env),
						StatusBrokerDataPlaneAvailable,
						StatusBrokerTopicReady,
						BrokerConfigMapAnnotations(),
						StatusBrokerConfigParsed,
						BrokerAddressable(&env),
						StatusBrokerProbeSucceeded,
						BrokerDLSResolved(ServiceURL),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - unchanged",
			Objects: []runtime.Object{
				NewBroker(
					WithDelivery(),
					BrokerConfigMapAnnotations(),
				),
				BrokerConfig(bootstrapServers, 20, 5),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:              BrokerUUID,
							Topics:           []string{BrokerTopic()},
							Ingress:          &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							BootstrapServers: bootstrapServers,
							Reference:        BrokerReference(),
							EgressConfig:     &contract.EgressConfig{DeadLetter: ServiceURL},
						},
					},
					Generation: 1,
				}, &env),
				NewService(),
				BrokerReceiverPod(env.SystemNamespace, map[string]string{base.VolumeGenerationAnnotationKey: "1"}),
				BrokerDispatcherPod(env.SystemNamespace, map[string]string{base.VolumeGenerationAnnotationKey: "1"}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewBroker(
						WithDelivery(),
						reconcilertesting.WithInitBrokerConditions,
						StatusBrokerConfigMapUpdatedReady(&env),
						StatusBrokerDataPlaneAvailable,
						StatusBrokerTopicReady,
						StatusBrokerConfigParsed,
						BrokerAddressable(&env),
						StatusBrokerProbeSucceeded,
						BrokerDLSResolved(ServiceURL),
						BrokerConfigMapAnnotations(),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - unchanged contract - changed data plane pods annotation",
			Objects: []runtime.Object{
				NewBroker(
					WithDelivery(),
				),
				BrokerConfig(bootstrapServers, 20, 5),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:              BrokerUUID,
							Topics:           []string{BrokerTopic()},
							Ingress:          &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							BootstrapServers: bootstrapServers,
							Reference:        BrokerReference(),
							EgressConfig:     &contract.EgressConfig{DeadLetter: ServiceURL},
						},
					},
					Generation: 1,
				}, &env),
				NewService(),
				BrokerReceiverPod(env.SystemNamespace, map[string]string{base.VolumeGenerationAnnotationKey: "0"}),
				BrokerDispatcherPod(env.SystemNamespace, map[string]string{base.VolumeGenerationAnnotationKey: "0"}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				BrokerReceiverPodUpdate(env.SystemNamespace, map[string]string{base.VolumeGenerationAnnotationKey: "1"}),
				BrokerDispatcherPodUpdate(env.SystemNamespace, map[string]string{base.VolumeGenerationAnnotationKey: "1"}),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewBroker(
						WithDelivery(),
						reconcilertesting.WithInitBrokerConditions,
						StatusBrokerConfigMapUpdatedReady(&env),
						StatusBrokerDataPlaneAvailable,
						StatusBrokerTopicReady,
						StatusBrokerConfigParsed,
						BrokerAddressable(&env),
						StatusBrokerProbeSucceeded,
						BrokerDLSResolved(ServiceURL),
						BrokerConfigMapAnnotations(),
					),
				},
			},
		},
	}

	for i := range table {
		table[i].Name = table[i].Name + " - " + format
	}

	useTable(t, table, &env)
}

func SecretFinalizerUpdate(secretName, finalizerName string) clientgotesting.UpdateActionImpl {
	return clientgotesting.NewUpdateAction(
		schema.GroupVersionResource{
			Group:    "*",
			Version:  "v1",
			Resource: "Secret",
		},
		ConfigMapNamespace,
		BrokerSecretWithFinalizer(ConfigMapNamespace, secretName, finalizerName),
	)
}

func SecretFinalizerUpdateRemove(secretName string) clientgotesting.UpdateActionImpl {
	return clientgotesting.NewUpdateAction(
		schema.GroupVersionResource{
			Group:    "*",
			Version:  "v1",
			Resource: "Secret",
		},
		ConfigMapNamespace,
		NewSSLSecret(ConfigMapNamespace, secretName),
	)
}

func TestBrokerFinalizer(t *testing.T) {
	t.Parallel()

	for _, f := range Formats {
		brokerFinalization(t, f, *DefaultEnv)
	}
}

func brokerFinalization(t *testing.T, format string, env config.Env) {

	testKey := fmt.Sprintf("%s/%s", BrokerNamespace, BrokerName)

	env.DataPlaneConfigFormat = format

	table := TableTest{
		{
			Name: "Reconciled normal - no DLS",
			Objects: []runtime.Object{
				NewDeletedBroker(),
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
				}, &env),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Resources:  []*contract.Resource{},
					Generation: 2,
				}),
			},
			OtherTestData: map[string]interface{}{
				testProber: probertesting.MockProber(prober.StatusNotReady),
			},
		},
		{
			Name: "Reconciled normal - no ConfigMap, rebuild from annotations",
			Objects: []runtime.Object{
				NewDeletedBroker(
					BrokerConfigMapAnnotations(),
				),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     BrokerUUID,
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
						},
					},
					Generation: 1,
				}, &env),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Resources:  []*contract.Resource{},
					Generation: 2,
				}),
			},
			OtherTestData: map[string]interface{}{
				testProber: probertesting.MockProber(prober.StatusNotReady),
			},
		},
		{
			Name: "Reconciled failed - probe not ready",
			Objects: []runtime.Object{
				NewDeletedBroker(),
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
				}, &env),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Resources:  []*contract.Resource{},
					Generation: 2,
				}),
			},
			WantErr: true,
			OtherTestData: map[string]interface{}{
				testProber: probertesting.MockProber(prober.StatusReady),
			},
		},
		{
			Name: "Reconciled normal - with DLS",
			Objects: []runtime.Object{
				NewDeletedBroker(
					WithDelivery(),
				),
				BrokerConfig(bootstrapServers, 20, 5),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:          BrokerUUID,
							Topics:       []string{BrokerTopic()},
							Ingress:      &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
							EgressConfig: &contract.EgressConfig{DeadLetter: ServiceURL},
						},
					},
					Generation: 1,
				}, &env),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Generation: 2,
				}),
			},
			OtherTestData: map[string]interface{}{
				testProber: probertesting.MockProber(prober.StatusNotReady),
			},
		},
		{
			Name: "Reconciled normal - with auth config",
			Objects: []runtime.Object{
				NewDeletedBroker(
					WithExternalTopic(ExternalTopicName),
					WithBrokerConfig(KReference(BrokerConfig(bootstrapServers, 20, 5,
						BrokerAuthConfig("secret-1"),
					))),
					BrokerConfigMapSecretAnnotation("secret-1"),
				),
				BrokerSecretWithFinalizer(ConfigMapNamespace, "secret-1", SecretFinalizerName),
				BrokerConfig(bootstrapServers, 20, 5, BrokerAuthConfig("secret-1")),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     BrokerUUID,
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{Path: receiver.Path(BrokerNamespace, BrokerName)},
						},
					},
					Generation: 1,
				}, &env),
				NewService(),
				BrokerReceiverPod(env.SystemNamespace, map[string]string{
					"annotation_to_preserve": "value_to_preserve",
				}),
				BrokerDispatcherPod(env.SystemNamespace, map[string]string{
					"annotation_to_preserve": "value_to_preserve",
				}),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Resources:  []*contract.Resource{},
					Generation: 2,
				}),
				BrokerReceiverPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "2",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				BrokerDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "2",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				SecretFinalizerUpdateRemove("secret-1"),
			},
			OtherTestData: map[string]interface{}{
				testProber: probertesting.MockProber(prober.StatusNotReady),
			},
		},

		{
			Name: "Failed to delete topic",
			Objects: []runtime.Object{
				NewDeletedBroker(),
				BrokerConfig(bootstrapServers, 20, 5),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:          BrokerUUID,
							Topics:       []string{BrokerTopic()},
							EgressConfig: &contract.EgressConfig{DeadLetter: ServiceURL},
						},
					},
					Generation: 1,
				}, &env),
			},
			Key:     testKey,
			WantErr: true,
			WantEvents: []string{
				Eventf(
					corev1.EventTypeWarning,
					"InternalError",
					"failed to delete topic %s: %v",
					BrokerTopic(), deleteTopicError,
				),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Generation: 2,
				}),
			},
			OtherTestData: map[string]interface{}{
				wantErrorOnDeleteTopic: deleteTopicError,
				testProber:             probertesting.MockProber(prober.StatusNotReady),
			},
		},
		{
			Name: "Config map not found - create config map",
			Objects: []runtime.Object{
				NewDeletedBroker(
					WithDelivery(),
				),
				BrokerConfig(bootstrapServers, 20, 5),
				NewService(),
			},
			Key:         testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{},
			WantCreates: []runtime.Object{
				NewConfigMapWithBinaryData(&env, nil),
			},
			SkipNamespaceValidation: true, // WantCreates compare the broker namespace with configmap namespace, so skip it
			OtherTestData: map[string]interface{}{
				testProber: probertesting.MockProber(prober.StatusNotReady),
			},
		},
		{
			Name: "Reconciled normal - preserve config map previous state",
			Objects: []runtime.Object{
				NewDeletedBroker(),
				BrokerConfig(bootstrapServers, 20, 5),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:          "5384faa4-6bdf-428d-b6c2-d6f89ce1d44b",
							Topics:       []string{"my-existing-topic-a"},
							EgressConfig: &contract.EgressConfig{DeadLetter: "http://www.my-sink.com"},
						},
						{
							Uid:          BrokerUUID,
							Topics:       []string{"my-existing-topic-b"},
							EgressConfig: &contract.EgressConfig{DeadLetter: "http://www.my-sink.com"},
						},
					},
					Generation: 5,
				}, &env),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:          "5384faa4-6bdf-428d-b6c2-d6f89ce1d44b",
							Topics:       []string{"my-existing-topic-a"},
							EgressConfig: &contract.EgressConfig{DeadLetter: "http://www.my-sink.com"},
						},
					},
					Generation: 6,
				}),
			},
			OtherTestData: map[string]interface{}{
				testProber: probertesting.MockProber(prober.StatusNotReady),
			},
		},
		{
			Name: "Reconciled normal - topic doesn't exist",
			Objects: []runtime.Object{
				NewDeletedBroker(),
				BrokerConfig(bootstrapServers, 20, 5),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:          "5384faa4-6bdf-428d-b6c2-d6f89ce1d44b",
							Topics:       []string{"my-existing-topic-a"},
							EgressConfig: &contract.EgressConfig{DeadLetter: "http://www.my-sink.com"},
						},
						{
							Uid:          BrokerUUID,
							Topics:       []string{"my-existing-topic-b"},
							EgressConfig: &contract.EgressConfig{DeadLetter: "http://www.my-sink.com"},
						},
					},
					Generation: 5,
				}, &env),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:          "5384faa4-6bdf-428d-b6c2-d6f89ce1d44b",
							Topics:       []string{"my-existing-topic-a"},
							EgressConfig: &contract.EgressConfig{DeadLetter: "http://www.my-sink.com"},
						},
					},
					Generation: 6,
				}),
			},
			OtherTestData: map[string]interface{}{
				wantErrorOnDeleteTopic: sarama.ErrUnknownTopicOrPartition,
				testProber:             probertesting.MockProber(prober.StatusNotReady),
			},
		},
		{
			Name: "Reconciled normal - no broker found in config map",
			Objects: []runtime.Object{
				NewDeletedBroker(),
				BrokerConfig(bootstrapServers, 20, 5),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:          "5384faa4-6bdf-428d-b6c2-d6f89ce1d44b",
							Topics:       []string{"my-existing-topic-a"},
							EgressConfig: &contract.EgressConfig{DeadLetter: "http://www.my-sink.com"},
						},
					},
					Generation: 5,
				}, &env),
			},
			Key:         testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{},
			OtherTestData: map[string]interface{}{
				testProber: probertesting.MockProber(prober.StatusNotReady),
			},
		},
	}

	for i := range table {
		table[i].Name = table[i].Name + " - " + format
	}

	useTable(t, table, &env)
}

func useTable(t *testing.T, table TableTest, env *config.Env) {

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

		reconciler := &Reconciler{
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
			Env:    env,
			Prober: proberMock,
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
			kafka.BrokerClass,
		)

		reconciler.Resolver = resolver.NewURIResolverFromTracker(ctx, tracker.New(func(name types.NamespacedName) {}, 0))

		return r
	}))
}

func patchFinalizers() clientgotesting.PatchActionImpl {
	action := clientgotesting.PatchActionImpl{}
	action.Name = BrokerName
	action.Namespace = BrokerNamespace
	patch := `{"metadata":{"finalizers":["` + finalizerName + `"],"resourceVersion":""}}`
	action.Patch = []byte(patch)
	return action
}
