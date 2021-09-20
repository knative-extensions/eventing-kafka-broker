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
	"sync"
	"testing"
	"time"

	"k8s.io/utils/pointer"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
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
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka"
	kafkatesting "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka/testing"
	. "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/testing"
)

const (
	wantErrorOnCreateTopic = "wantErrorOnCreateTopic"
	wantErrorOnDeleteTopic = "wantErrorOnDeleteTopic"
	ExpectedTopicDetail    = "expectedTopicDetail"
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

func TestBrokerReconciler(t *testing.T) {
	eventing.RegisterAlternateBrokerConditionSet(base.ConditionSet)

	t.Parallel()

	for _, f := range Formats {
		brokerReconciliation(t, f, *DefaultConfigs)
	}
}

func brokerReconciliation(t *testing.T, format string, configs Configs) {

	testKey := fmt.Sprintf("%s/%s", BrokerNamespace, BrokerName)

	configs.DataPlaneConfigFormat = format

	table := TableTest{
		{
			Name: "Reconciled normal - no DLS",
			Objects: []runtime.Object{
				NewBroker(),
				NewConfigMap(&configs, nil),
				NewService(),
				BrokerReceiverPod(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				BrokerDispatcherPod(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:              BrokerUUID,
							Topics:           []string{BrokerTopic()},
							Ingress:          &contract.Ingress{IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
							BootstrapServers: bootstrapServers,
						},
					},
					Generation: 1,
				}),
				BrokerReceiverPodUpdate(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				BrokerDispatcherPodUpdate(configs.SystemNamespace, map[string]string{
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
						BrokerConfigMapUpdatedReady(&configs),
						BrokerDataPlaneAvailable,
						BrokerConfigParsed,
						BrokerTopicReady,
						BrokerAddressable(&configs),
					),
				},
			},
			OtherTestData: map[string]interface{}{
				BootstrapServersConfigMapKey: bootstrapServers,
			},
		},
		{
			Name: "Reconciled normal - with DLS",
			Objects: []runtime.Object{
				NewBroker(
					WithDelivery(),
				),
				NewConfigMapFromContract(&contract.Contract{
					Generation: 1,
				}, &configs),
				NewService(),
				BrokerReceiverPod(configs.SystemNamespace, map[string]string{base.VolumeGenerationAnnotationKey: "3"}),
				BrokerDispatcherPod(configs.SystemNamespace, map[string]string{base.VolumeGenerationAnnotationKey: "1"}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:              BrokerUUID,
							Topics:           []string{BrokerTopic()},
							Ingress:          &contract.Ingress{ContentMode: contract.ContentMode_BINARY, IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
							BootstrapServers: bootstrapServers,
							EgressConfig:     &contract.EgressConfig{DeadLetter: ServiceURL},
						},
					},
					Generation: 2,
				}),
				BrokerReceiverPodUpdate(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "2",
				}),
				BrokerDispatcherPodUpdate(configs.SystemNamespace, map[string]string{
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
						BrokerConfigMapUpdatedReady(&configs),
						BrokerDataPlaneAvailable,
						BrokerTopicReady,
						BrokerConfigParsed,
						BrokerAddressable(&configs),
					),
				},
			},
			OtherTestData: map[string]interface{}{
				BootstrapServersConfigMapKey: bootstrapServers,
			},
		},
		{
			Name: "Reconciled normal - with DLS - no DLS ref namespace",
			Objects: []runtime.Object{
				NewBroker(
					WithDelivery(WithNoDeadLetterSinkNamespace),
				),
				NewConfigMapFromContract(&contract.Contract{
					Generation: 1,
				}, &configs),
				NewService(WithServiceNamespace(BrokerNamespace)),
				BrokerReceiverPod(configs.SystemNamespace, map[string]string{base.VolumeGenerationAnnotationKey: "3"}),
				BrokerDispatcherPod(configs.SystemNamespace, map[string]string{base.VolumeGenerationAnnotationKey: "1"}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:              BrokerUUID,
							Topics:           []string{BrokerTopic()},
							Ingress:          &contract.Ingress{ContentMode: contract.ContentMode_BINARY, IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
							BootstrapServers: bootstrapServers,
							EgressConfig:     &contract.EgressConfig{DeadLetter: ServiceURLFrom(BrokerNamespace, ServiceName)},
						},
					},
					Generation: 2,
				}),
				BrokerReceiverPodUpdate(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "2",
				}),
				BrokerDispatcherPodUpdate(configs.SystemNamespace, map[string]string{
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
						BrokerConfigMapUpdatedReady(&configs),
						BrokerDataPlaneAvailable,
						BrokerTopicReady,
						BrokerConfigParsed,
						BrokerAddressable(&configs),
					),
				},
			},
			OtherTestData: map[string]interface{}{
				BootstrapServersConfigMapKey: bootstrapServers,
			},
		},
		{
			Name: "Failed to create topic",
			Objects: []runtime.Object{
				NewBroker(),
				BrokerReceiverPod(configs.SystemNamespace, nil),
				BrokerDispatcherPod(configs.SystemNamespace, nil),
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
						BrokerDataPlaneAvailable,
						BrokerConfigParsed,
						BrokerFailedToCreateTopic,
					),
				},
			},
			OtherTestData: map[string]interface{}{
				wantErrorOnCreateTopic:       createTopicError,
				BootstrapServersConfigMapKey: bootstrapServers,
			},
		},
		{
			Name: "Config map not found - create config map",
			Objects: []runtime.Object{
				NewBroker(
					WithDelivery(),
				),
				NewService(),
				BrokerReceiverPod(configs.SystemNamespace, map[string]string{base.VolumeGenerationAnnotationKey: "2"}),
				BrokerDispatcherPod(configs.SystemNamespace, map[string]string{base.VolumeGenerationAnnotationKey: "2"}),
				&corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: configs.DataPlaneConfigMapNamespace,
						Name:      configs.DataPlaneConfigMapName + "a", // Use a different name
					},
				},
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			SkipNamespaceValidation: true, // WantCreates compare the broker namespace with configmap namespace, so skip it
			WantCreates: []runtime.Object{
				NewConfigMap(&configs, nil),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:              BrokerUUID,
							Topics:           []string{BrokerTopic()},
							Ingress:          &contract.Ingress{ContentMode: contract.ContentMode_BINARY, IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
							BootstrapServers: bootstrapServers,
							EgressConfig:     &contract.EgressConfig{DeadLetter: ServiceURL},
						},
					},
					Generation: 1,
				}),
				BrokerReceiverPodUpdate(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
				BrokerDispatcherPodUpdate(configs.SystemNamespace, map[string]string{
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
						BrokerConfigParsed,
						BrokerDataPlaneAvailable,
						BrokerConfigMapUpdatedReady(&configs),
						BrokerTopicReady,
						BrokerAddressable(&configs),
					),
				},
			},
			OtherTestData: map[string]interface{}{
				BootstrapServersConfigMapKey: bootstrapServers,
			},
		},
		{
			Name: "Reconciled normal - config map not readable",
			Objects: []runtime.Object{
				NewBroker(),
				NewConfigMap(&configs, []byte(`{"hello": "world"}`)),
				NewService(),
				BrokerReceiverPod(configs.SystemNamespace, nil),
				BrokerDispatcherPod(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "2",
				}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:              BrokerUUID,
							Topics:           []string{BrokerTopic()},
							Ingress:          &contract.Ingress{ContentMode: contract.ContentMode_BINARY, IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
							BootstrapServers: bootstrapServers,
						},
					},
					Generation: 1,
				}),
				BrokerReceiverPodUpdate(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
				BrokerDispatcherPodUpdate(configs.SystemNamespace, map[string]string{
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
						BrokerDataPlaneAvailable,
						BrokerConfigMapUpdatedReady(&configs),
						BrokerConfigParsed,
						BrokerTopicReady,
						BrokerAddressable(&configs),
					),
				},
			},
			OtherTestData: map[string]interface{}{
				BootstrapServersConfigMapKey: bootstrapServers,
			},
		},
		{
			Name: "Reconciled normal - preserve config map previous state",
			Objects: []runtime.Object{
				NewBroker(),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:          "5384faa4-6bdf-428d-b6c2-d6f89ce1d44b",
							Topics:       []string{"my-existing-topic-a"},
							Ingress:      &contract.Ingress{IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
							EgressConfig: &contract.EgressConfig{DeadLetter: "http://www.my-sink.com"},
						},
						{
							Uid:          "5384faa4-6bdf-428d-b6c2-d6f89ce1d44a",
							Topics:       []string{"my-existing-topic-b"},
							EgressConfig: &contract.EgressConfig{DeadLetter: "http://www.my-sink.com"},
						},
					},
				}, &configs),
				NewService(),
				BrokerReceiverPod(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "2",
				}),
				BrokerDispatcherPod(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "2",
				}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:          "5384faa4-6bdf-428d-b6c2-d6f89ce1d44b",
							Topics:       []string{"my-existing-topic-a"},
							Ingress:      &contract.Ingress{IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
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
							Ingress:          &contract.Ingress{ContentMode: contract.ContentMode_BINARY, IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
							BootstrapServers: bootstrapServers,
						},
					},
					Generation: 1,
				}),
				BrokerReceiverPodUpdate(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
				BrokerDispatcherPodUpdate(configs.SystemNamespace, map[string]string{
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
						BrokerDataPlaneAvailable,
						BrokerConfigMapUpdatedReady(&configs),
						BrokerConfigParsed,
						BrokerTopicReady,
						BrokerAddressable(&configs),
					),
				},
			},
			OtherTestData: map[string]interface{}{
				BootstrapServersConfigMapKey: bootstrapServers,
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
				}, &configs),
				NewService(),
				BrokerReceiverPod(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "5",
				}),
				BrokerDispatcherPod(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "5",
				}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:          "5384faa4-6bdf-428d-b6c2-d6f89ce1d44b",
							Topics:       []string{"my-existing-topic-a"},
							EgressConfig: &contract.EgressConfig{DeadLetter: "http://www.my-sink.com"},
						},
						{
							Uid:              BrokerUUID,
							Topics:           []string{BrokerTopic()},
							Ingress:          &contract.Ingress{ContentMode: contract.ContentMode_BINARY, IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
							BootstrapServers: bootstrapServers,
							EgressConfig:     &contract.EgressConfig{DeadLetter: "http://www.my-sink.com/api"},
						},
					},
					Generation: 1,
				}),
				BrokerReceiverPodUpdate(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
				BrokerDispatcherPodUpdate(configs.SystemNamespace, map[string]string{
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
						BrokerDataPlaneAvailable,
						BrokerConfigMapUpdatedReady(&configs),
						BrokerConfigParsed,
						BrokerTopicReady,
						BrokerAddressable(&configs),
					),
				},
			},
			OtherTestData: map[string]interface{}{
				BootstrapServersConfigMapKey: bootstrapServers,
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
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:          "5384faa4-6bdf-428d-b6c2-d6f89ce1d44b",
							Topics:       []string{"my-existing-topic-a"},
							Ingress:      &contract.Ingress{IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
							EgressConfig: &contract.EgressConfig{DeadLetter: "http://www.my-sink.com"},
						},
						{
							Uid:          BrokerUUID,
							Topics:       []string{BrokerTopic()},
							Ingress:      &contract.Ingress{IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
							EgressConfig: &contract.EgressConfig{DeadLetter: "http://www.my-sink.com"},
						},
					},
				}, &configs),
				NewService(),
				BrokerReceiverPod(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "5",
				}),
				BrokerDispatcherPod(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "5",
				}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:          "5384faa4-6bdf-428d-b6c2-d6f89ce1d44b",
							Topics:       []string{"my-existing-topic-a"},
							Ingress:      &contract.Ingress{IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
							EgressConfig: &contract.EgressConfig{DeadLetter: "http://www.my-sink.com"},
						},
						{
							Uid:              BrokerUUID,
							Topics:           []string{BrokerTopic()},
							Ingress:          &contract.Ingress{ContentMode: contract.ContentMode_BINARY, IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
							BootstrapServers: bootstrapServers,
						},
					},
					Generation: 1,
				}),
				BrokerReceiverPodUpdate(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
				BrokerDispatcherPodUpdate(configs.SystemNamespace, map[string]string{
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
						BrokerDataPlaneAvailable,
						BrokerConfigMapUpdatedReady(&configs),
						BrokerConfigParsed,
						BrokerTopicReady,
						BrokerAddressable(&configs),
					),
				},
			},
			OtherTestData: map[string]interface{}{
				BootstrapServersConfigMapKey: bootstrapServers,
			},
		},
		{
			Name: "Reconciled normal - increment volume generation",
			Objects: []runtime.Object{
				NewBroker(),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:          "5384faa4-6bdf-428d-b6c2-d6f89ce1d44b",
							Topics:       []string{"my-existing-topic-a"},
							Ingress:      &contract.Ingress{IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
							EgressConfig: &contract.EgressConfig{DeadLetter: "http://www.my-sink.com"},
						},
						{
							Uid:     BrokerUUID,
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
						},
					},
					Generation: 1,
				}, &configs),
				NewService(),
				BrokerReceiverPod(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "5",
				}),
				BrokerDispatcherPod(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "5",
				}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:          "5384faa4-6bdf-428d-b6c2-d6f89ce1d44b",
							Topics:       []string{"my-existing-topic-a"},
							Ingress:      &contract.Ingress{IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
							EgressConfig: &contract.EgressConfig{DeadLetter: "http://www.my-sink.com"},
						},
						{
							Uid:              BrokerUUID,
							Topics:           []string{BrokerTopic()},
							Ingress:          &contract.Ingress{ContentMode: contract.ContentMode_BINARY, IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
							BootstrapServers: bootstrapServers,
						},
					},
					Generation: 2,
				}),
				BrokerReceiverPodUpdate(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "2",
				}),
				BrokerDispatcherPodUpdate(configs.SystemNamespace, map[string]string{
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
						BrokerDataPlaneAvailable,
						BrokerConfigMapUpdatedReady(&configs),
						BrokerConfigParsed,
						BrokerTopicReady,
						BrokerAddressable(&configs),
					),
				},
			},
			OtherTestData: map[string]interface{}{
				BootstrapServersConfigMapKey: bootstrapServers,
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
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:          "5384faa4-6bdf-428d-b6c2-d6f89ce1d44b",
							Topics:       []string{"my-existing-topic-a"},
							EgressConfig: &contract.EgressConfig{DeadLetter: "http://www.my-sink.com"},
							Ingress:      &contract.Ingress{IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
						},
						{
							Uid:              BrokerUUID,
							Topics:           []string{BrokerTopic()},
							Ingress:          &contract.Ingress{ContentMode: contract.ContentMode_BINARY, IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
							BootstrapServers: bootstrapServers,
						},
					},
					Generation: 1,
				}, &configs),
				BrokerReceiverPod(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "5",
				}),
				BrokerDispatcherPod(configs.SystemNamespace, map[string]string{
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
						BrokerDataPlaneAvailable,
						BrokerConfigParsed,
						BrokerTopicReady,
					),
				},
			},
			OtherTestData: map[string]interface{}{
				BootstrapServersConfigMapKey: bootstrapServers,
			},
		},
		{
			Name: "No bootstrap.servers provided",
			Objects: []runtime.Object{
				NewBroker(),
				NewConfigMapFromContract(&contract.Contract{
					Resources:  []*contract.Resource{},
					Generation: 1,
				}, &configs),
				BrokerReceiverPod(configs.SystemNamespace, nil),
				BrokerDispatcherPod(configs.SystemNamespace, nil),
			},
			Key:     testKey,
			WantErr: true,
			WantEvents: []string{
				finalizerUpdatedEvent,
				Eventf(
					corev1.EventTypeWarning,
					"InternalError",
					"failed to get contract configuration: no bootstrap.servers provided",
				),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewBroker(
						reconcilertesting.WithInitBrokerConditions,
						BrokerDataPlaneAvailable,
						BrokerConfigNotParsed("no bootstrap.servers provided"),
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
				NewConfigMap(&configs, nil),
				NewService(),
				BrokerReceiverPod(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				BrokerDispatcherPod(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "3",
					"annotation_to_preserve":           "value_to_preserve",
				}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:              BrokerUUID,
							Topics:           []string{BrokerTopic()},
							Ingress:          &contract.Ingress{ContentMode: contract.ContentMode_BINARY, IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
							BootstrapServers: bootstrapServers,
						},
					},
					Generation: 1,
				}),
				BrokerReceiverPodUpdate(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				BrokerDispatcherPodUpdate(configs.SystemNamespace, map[string]string{
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
						BrokerConfigMapUpdatedReady(&configs),
						BrokerDataPlaneAvailable,
						BrokerConfigParsed,
						BrokerTopicReady,
						BrokerAddressable(&configs),
					),
				},
			},
			OtherTestData: map[string]interface{}{
				BootstrapServersConfigMapKey: bootstrapServers,
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
				NewConfigMap(&configs, nil),
				NewService(),
				BrokerReceiverPod(configs.SystemNamespace, map[string]string{
					"annotation_to_preserve": "value_to_preserve",
				}),
				BrokerDispatcherPod(configs.SystemNamespace, map[string]string{
					"annotation_to_preserve": "value_to_preserve",
				}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:              BrokerUUID,
							Topics:           []string{BrokerTopic()},
							Ingress:          &contract.Ingress{ContentMode: contract.ContentMode_BINARY, IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
							BootstrapServers: bootstrapServers,
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
				BrokerReceiverPodUpdate(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				BrokerDispatcherPodUpdate(configs.SystemNamespace, map[string]string{
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
						BrokerConfigMapUpdatedReady(&configs),
						BrokerDataPlaneAvailable,
						BrokerConfigParsed,
						BrokerTopicReady,
						BrokerAddressable(&configs),
					),
				},
			},
			OtherTestData: map[string]interface{}{
				BootstrapServersConfigMapKey: bootstrapServers,
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
				BrokerReceiverPod(configs.SystemNamespace, nil),
				BrokerDispatcherPod(configs.SystemNamespace, nil),
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
						BrokerDataPlaneAvailable,
						BrokerConfigNotParsed(fmt.Sprintf(`failed to get configmap %s/%s: configmap %q not found`, ConfigMapNamespace, ConfigMapName, ConfigMapName)),
					),
				},
			},
			OtherTestData: map[string]interface{}{
				BootstrapServersConfigMapKey: bootstrapServers,
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
				BrokerReceiverPod(configs.SystemNamespace, nil),
				BrokerDispatcherPod(configs.SystemNamespace, nil),
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
						BrokerDataPlaneAvailable,
						BrokerConfigNotParsed(`supported config Kind: ConfigMap - got Pod`),
					),
				},
			},
			OtherTestData: map[string]interface{}{
				BootstrapServersConfigMapKey: bootstrapServers,
			},
		},
		{
			Name: "Reconciled normal - keep all existing triggers",
			Objects: []runtime.Object{
				NewBroker(
					WithDelivery(),
				),
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
							Ingress: &contract.Ingress{IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
						},
					},
					Generation: 1,
				}, &configs),
				NewService(),
				BrokerReceiverPod(configs.SystemNamespace, nil),
				BrokerDispatcherPod(configs.SystemNamespace, nil),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &contract.Contract{
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
							Ingress:          &contract.Ingress{ContentMode: contract.ContentMode_BINARY, IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
						},
					},
					Generation: 2,
				}),
				BrokerReceiverPodUpdate(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "2",
				}),
				BrokerDispatcherPodUpdate(configs.SystemNamespace, map[string]string{
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
						BrokerDataPlaneAvailable,
						BrokerConfigMapUpdatedReady(&configs),
						BrokerConfigParsed,
						BrokerTopicReady,
						BrokerAddressable(&configs),
					),
				},
			},
			OtherTestData: map[string]interface{}{
				BootstrapServersConfigMapKey: bootstrapServers,
			},
		},
		{
			Name: "no data plane pods running",
			Objects: []runtime.Object{
				NewBroker(),
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
						BrokerDataPlaneNotAvailable,
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
				NewConfigMapFromContract(&contract.Contract{
					Generation: 1,
				}, &configs),
				NewService(),
				BrokerReceiverPod(configs.SystemNamespace, map[string]string{base.VolumeGenerationAnnotationKey: "1"}),
				BrokerDispatcherPod(configs.SystemNamespace, map[string]string{base.VolumeGenerationAnnotationKey: "1"}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:              BrokerUUID,
							Topics:           []string{BrokerTopic()},
							Ingress:          &contract.Ingress{ContentMode: contract.ContentMode_BINARY, IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
							BootstrapServers: bootstrapServers,
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
				BrokerReceiverPodUpdate(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "2",
				}),
				BrokerDispatcherPodUpdate(configs.SystemNamespace, map[string]string{
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
						BrokerConfigMapUpdatedReady(&configs),
						BrokerDataPlaneAvailable,
						BrokerTopicReady,
						BrokerConfigParsed,
						BrokerAddressable(&configs),
					),
				},
			},
			OtherTestData: map[string]interface{}{
				BootstrapServersConfigMapKey: bootstrapServers,
			},
		},
		{
			Name: "Reconciled normal - with retry config - linear",
			Objects: []runtime.Object{
				NewBroker(
					WithDelivery(),
					WithRetry(pointer.Int32Ptr(10), &linear, pointer.StringPtr("PT2S")),
				),
				NewConfigMapFromContract(&contract.Contract{
					Generation: 1,
				}, &configs),
				NewService(),
				BrokerReceiverPod(configs.SystemNamespace, map[string]string{base.VolumeGenerationAnnotationKey: "1"}),
				BrokerDispatcherPod(configs.SystemNamespace, map[string]string{base.VolumeGenerationAnnotationKey: "0"}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:              BrokerUUID,
							Topics:           []string{BrokerTopic()},
							Ingress:          &contract.Ingress{ContentMode: contract.ContentMode_BINARY, IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
							BootstrapServers: bootstrapServers,
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
				BrokerReceiverPodUpdate(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "2",
				}),
				BrokerDispatcherPodUpdate(configs.SystemNamespace, map[string]string{
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
						BrokerConfigMapUpdatedReady(&configs),
						BrokerDataPlaneAvailable,
						BrokerTopicReady,
						BrokerConfigParsed,
						BrokerAddressable(&configs),
					),
				},
			},
			OtherTestData: map[string]interface{}{
				BootstrapServersConfigMapKey: bootstrapServers,
			},
		},
		{
			Name: "Reconciled normal - with no retry num",
			Objects: []runtime.Object{
				NewBroker(
					WithDelivery(),
					WithRetry(nil, &linear, pointer.StringPtr("PT2S")),
				),
				NewConfigMapFromContract(&contract.Contract{
					Generation: 1,
				}, &configs),
				NewService(),
				BrokerReceiverPod(configs.SystemNamespace, map[string]string{base.VolumeGenerationAnnotationKey: "1"}),
				BrokerDispatcherPod(configs.SystemNamespace, map[string]string{base.VolumeGenerationAnnotationKey: "1"}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:              BrokerUUID,
							Topics:           []string{BrokerTopic()},
							Ingress:          &contract.Ingress{ContentMode: contract.ContentMode_BINARY, IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
							BootstrapServers: bootstrapServers,
							EgressConfig: &contract.EgressConfig{
								DeadLetter: ServiceURL,
							},
						},
					},
					Generation: 2,
				}),
				BrokerReceiverPodUpdate(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "2",
				}),
				BrokerDispatcherPodUpdate(configs.SystemNamespace, map[string]string{
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
						BrokerConfigMapUpdatedReady(&configs),
						BrokerDataPlaneAvailable,
						BrokerTopicReady,
						BrokerConfigParsed,
						BrokerAddressable(&configs),
					),
				},
			},
			OtherTestData: map[string]interface{}{
				BootstrapServersConfigMapKey: bootstrapServers,
			},
		},
		{
			Name: "Reconciled normal - with retry config - no retry delay - use default delay",
			Objects: []runtime.Object{
				NewBroker(
					WithDelivery(),
					WithRetry(pointer.Int32Ptr(10), &linear, nil),
				),
				NewConfigMapFromContract(&contract.Contract{
					Generation: 1,
				}, &configs),
				NewService(),
				BrokerReceiverPod(configs.SystemNamespace, map[string]string{base.VolumeGenerationAnnotationKey: "1"}),
				BrokerDispatcherPod(configs.SystemNamespace, map[string]string{base.VolumeGenerationAnnotationKey: "1"}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:              BrokerUUID,
							Topics:           []string{BrokerTopic()},
							Ingress:          &contract.Ingress{ContentMode: contract.ContentMode_BINARY, IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
							BootstrapServers: bootstrapServers,
							EgressConfig: &contract.EgressConfig{
								DeadLetter:    ServiceURL,
								Retry:         10,
								BackoffPolicy: contract.BackoffPolicy_Linear,
								BackoffDelay:  configs.DefaultBackoffDelayMs,
							},
						},
					},
					Generation: 2,
				}),
				BrokerReceiverPodUpdate(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "2",
				}),
				BrokerDispatcherPodUpdate(configs.SystemNamespace, map[string]string{
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
						BrokerConfigMapUpdatedReady(&configs),
						BrokerDataPlaneAvailable,
						BrokerTopicReady,
						BrokerConfigParsed,
						BrokerAddressable(&configs),
					),
				},
			},
			OtherTestData: map[string]interface{}{
				BootstrapServersConfigMapKey: bootstrapServers,
			},
		},
		{
			Name: "Reconciled normal - unchanged",
			Objects: []runtime.Object{
				NewBroker(
					WithDelivery(),
				),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:              BrokerUUID,
							Topics:           []string{BrokerTopic()},
							Ingress:          &contract.Ingress{ContentMode: contract.ContentMode_BINARY, IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
							BootstrapServers: bootstrapServers,
							EgressConfig:     &contract.EgressConfig{DeadLetter: ServiceURL},
						},
					},
					Generation: 1,
				}, &configs),
				NewService(),
				BrokerReceiverPod(configs.SystemNamespace, map[string]string{base.VolumeGenerationAnnotationKey: "1"}),
				BrokerDispatcherPod(configs.SystemNamespace, map[string]string{base.VolumeGenerationAnnotationKey: "1"}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewBroker(
						WithDelivery(),
						reconcilertesting.WithInitBrokerConditions,
						BrokerConfigMapUpdatedReady(&configs),
						BrokerDataPlaneAvailable,
						BrokerTopicReady,
						BrokerConfigParsed,
						BrokerAddressable(&configs),
					),
				},
			},
			OtherTestData: map[string]interface{}{
				BootstrapServersConfigMapKey: bootstrapServers,
			},
		},
		{
			Name: "Reconciled normal - unchanged contract - changed data plane pods annotation",
			Objects: []runtime.Object{
				NewBroker(
					WithDelivery(),
				),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:              BrokerUUID,
							Topics:           []string{BrokerTopic()},
							Ingress:          &contract.Ingress{ContentMode: contract.ContentMode_BINARY, IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
							BootstrapServers: bootstrapServers,
							EgressConfig:     &contract.EgressConfig{DeadLetter: ServiceURL},
						},
					},
					Generation: 1,
				}, &configs),
				NewService(),
				BrokerReceiverPod(configs.SystemNamespace, map[string]string{base.VolumeGenerationAnnotationKey: "0"}),
				BrokerDispatcherPod(configs.SystemNamespace, map[string]string{base.VolumeGenerationAnnotationKey: "0"}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				BrokerReceiverPodUpdate(configs.SystemNamespace, map[string]string{base.VolumeGenerationAnnotationKey: "1"}),
				BrokerDispatcherPodUpdate(configs.SystemNamespace, map[string]string{base.VolumeGenerationAnnotationKey: "1"}),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewBroker(
						WithDelivery(),
						reconcilertesting.WithInitBrokerConditions,
						BrokerConfigMapUpdatedReady(&configs),
						BrokerDataPlaneAvailable,
						BrokerTopicReady,
						BrokerConfigParsed,
						BrokerAddressable(&configs),
					),
				},
			},
			OtherTestData: map[string]interface{}{
				BootstrapServersConfigMapKey: bootstrapServers,
			},
		},
	}

	for i := range table {
		table[i].Name = table[i].Name + " - " + format
	}

	useTable(t, table, &configs)
}

func TestBrokerFinalizer(t *testing.T) {
	t.Parallel()

	for _, f := range Formats {
		brokerFinalization(t, f, *DefaultConfigs)
	}
}

func brokerFinalization(t *testing.T, format string, configs Configs) {

	testKey := fmt.Sprintf("%s/%s", BrokerNamespace, BrokerName)

	configs.DataPlaneConfigFormat = format

	table := TableTest{
		{
			Name: "Reconciled normal - no DLS",
			Objects: []runtime.Object{
				NewDeletedBroker(),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     BrokerUUID,
							Topics:  []string{BrokerTopic()},
							Ingress: &contract.Ingress{IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
						},
					},
					Generation: 1,
				}, &configs),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &contract.Contract{
					Resources:  []*contract.Resource{},
					Generation: 2,
				}),
			},
			OtherTestData: map[string]interface{}{
				BootstrapServersConfigMapKey: bootstrapServers,
			},
		},
		{
			Name: "Reconciled normal - with DLS",
			Objects: []runtime.Object{
				NewDeletedBroker(
					WithDelivery(),
				),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:          BrokerUUID,
							Topics:       []string{BrokerTopic()},
							Ingress:      &contract.Ingress{IngressType: &contract.Ingress_Path{Path: receiver.Path(BrokerNamespace, BrokerName)}},
							EgressConfig: &contract.EgressConfig{DeadLetter: ServiceURL},
						},
					},
					Generation: 1,
				}, &configs),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &contract.Contract{
					Generation: 2,
				}),
			},
			OtherTestData: map[string]interface{}{
				BootstrapServersConfigMapKey: bootstrapServers,
			},
		},
		{
			Name: "Failed to delete topic",
			Objects: []runtime.Object{
				NewDeletedBroker(),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:          BrokerUUID,
							Topics:       []string{BrokerTopic()},
							EgressConfig: &contract.EgressConfig{DeadLetter: ServiceURL},
						},
					},
					Generation: 1,
				}, &configs),
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
				ConfigMapUpdate(&configs, &contract.Contract{
					Generation: 2,
				}),
			},
			OtherTestData: map[string]interface{}{
				wantErrorOnDeleteTopic:       deleteTopicError,
				BootstrapServersConfigMapKey: bootstrapServers,
			},
		},
		{
			Name: "Config map not found - create config map",
			Objects: []runtime.Object{
				NewDeletedBroker(
					WithDelivery(),
				),
				NewService(),
			},
			Key: testKey,
			WantCreates: []runtime.Object{
				NewConfigMap(&configs, nil),
			},
			SkipNamespaceValidation: true, // WantCreates compare the broker namespace with configmap namespace, so skip it
			OtherTestData: map[string]interface{}{
				BootstrapServersConfigMapKey: bootstrapServers,
			},
		},
		{
			Name: "Reconciled normal - preserve config map previous state",
			Objects: []runtime.Object{
				NewDeletedBroker(),
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
				}, &configs),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &contract.Contract{
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
				BootstrapServersConfigMapKey: bootstrapServers,
			},
		},
		{
			Name: "Reconciled normal - topic doesn't exist",
			Objects: []runtime.Object{
				NewDeletedBroker(),
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
				}, &configs),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &contract.Contract{
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
				wantErrorOnDeleteTopic:       sarama.ErrUnknownTopicOrPartition,
				BootstrapServersConfigMapKey: bootstrapServers,
			},
		},
		{
			Name: "Reconciled normal - no broker found in config map",
			Objects: []runtime.Object{
				NewDeletedBroker(),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:          "5384faa4-6bdf-428d-b6c2-d6f89ce1d44b",
							Topics:       []string{"my-existing-topic-a"},
							EgressConfig: &contract.EgressConfig{DeadLetter: "http://www.my-sink.com"},
						},
					},
					Generation: 5,
				}, &configs),
			},
			Key: testKey,
			OtherTestData: map[string]interface{}{
				BootstrapServersConfigMapKey: bootstrapServers,
			},
		},
	}

	for i := range table {
		table[i].Name = table[i].Name + " - " + format
	}

	useTable(t, table, &configs)
}

func useTable(t *testing.T, table TableTest, configs *Configs) {

	testCtx, cancel := context.WithCancel(context.Background())

	table.Test(t, NewFactory(configs, func(ctx context.Context, listers *Listers, configs *Configs, row *TableRow) controller.Reconciler {

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

		bootstrapServers := ""
		if bs, ok := row.OtherTestData[BootstrapServersConfigMapKey]; ok {
			bootstrapServers = bs.(string)
		}

		expectedTopicDetail := defaultTopicDetail
		if td, ok := row.OtherTestData[ExpectedTopicDetail]; ok {
			expectedTopicDetail = td.(sarama.TopicDetail)
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
				DispatcherLabel:             base.BrokerDispatcherLabel,
				ReceiverLabel:               base.BrokerReceiverLabel,
			},
			KafkaDefaultTopicDetails:     defaultTopicDetail,
			KafkaDefaultTopicDetailsLock: sync.RWMutex{},
			ConfigMapLister:              listers.GetConfigMapLister(),
			ClusterAdmin: func(addrs []string, config *sarama.Config) (sarama.ClusterAdmin, error) {
				return &kafkatesting.MockKafkaClusterAdmin{
					ExpectedTopicName:   fmt.Sprintf("%s%s-%s", TopicPrefix, BrokerNamespace, BrokerName),
					ExpectedTopicDetail: expectedTopicDetail,
					ErrorOnCreateTopic:  onCreateTopicError,
					ErrorOnDeleteTopic:  onDeleteTopicError,
					T:                   t,
				}, nil
			},
			Configs: configs,
		}
		reconciler.SetBootstrapServers(bootstrapServers)

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

		// periodically update default topic details to simulate concurrency.
		go func() {

			ticker := time.NewTicker(10 * time.Millisecond)

			for {
				select {
				case <-testCtx.Done():
					return
				case <-ticker.C:
					reconciler.SetDefaultTopicDetails(defaultTopicDetail)
				}
			}
		}()

		return r
	}))

	cancel()
}

func TestConfigMapUpdate(t *testing.T) {

	cm := corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cmname",
			Namespace: "cmnamespace",
		},
		Data: map[string]string{
			DefaultTopicNumPartitionConfigMapKey:      "42",
			DefaultTopicReplicationFactorConfigMapKey: "3",
			BootstrapServersConfigMapKey:              "server1,server2",
		},
	}

	reconciler := Reconciler{}

	ctx, _ := SetupFakeContext(t)

	reconciler.ConfigMapUpdated(ctx)(&cm)

	assert.Equal(t, reconciler.KafkaDefaultTopicDetails, sarama.TopicDetail{
		NumPartitions:     42,
		ReplicationFactor: 3,
	})
}

func patchFinalizers() clientgotesting.PatchActionImpl {
	action := clientgotesting.PatchActionImpl{}
	action.Name = BrokerName
	action.Namespace = BrokerNamespace
	patch := `{"metadata":{"finalizers":["` + finalizerName + `"],"resourceVersion":""}}`
	action.Patch = []byte(patch)
	return action
}
