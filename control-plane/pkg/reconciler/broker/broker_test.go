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
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgotesting "k8s.io/client-go/testing"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1beta1"
	"knative.dev/pkg/apis"
	kubeclient "knative.dev/pkg/client/injection/kube/client/fake"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	. "knative.dev/pkg/reconciler/testing"
	"knative.dev/pkg/resolver"

	eventingduck "knative.dev/eventing/pkg/apis/duck/v1beta1"
	fakeeventingclient "knative.dev/eventing/pkg/client/injection/client/fake"
	brokerreconciler "knative.dev/eventing/pkg/client/injection/reconciler/eventing/v1beta1/broker"
	reconcilertesting "knative.dev/eventing/pkg/reconciler/testing/v1beta1"
	duckv1 "knative.dev/pkg/apis/duck/v1"

	coreconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/core/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	. "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/broker"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka"
	. "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/testing"
)

const (
	brokerNamespace = "test-namespace"
	brokerName      = "test-broker"

	serviceNamespace = "test-service-namespace"
	serviceName      = "test-service"

	brokerUUID = "e7185016-5d98-4b54-84e8-3b1cd4acc6b4"

	wantErrorOnCreateTopic = "wantErrorOnCreateTopic"
	wantErrorOnDeleteTopic = "wantErrorOnDeleteTopic"
)

var (
	finalizerUpdatedEvent = Eventf(
		corev1.EventTypeNormal,
		"FinalizerUpdate",
		fmt.Sprintf(`Updated %q finalizers`, brokerName),
	)

	createTopicError = fmt.Errorf("failed to create topic")
	deleteTopicError = fmt.Errorf("failed to delete topic")

	formats = []string{base.Protobuf, base.Json}
)

func TestBrokeReconciler(t *testing.T) {
	t.Parallel()

	for _, f := range formats {
		brokerReconciliation(t, f, *DefaultConfigs)
	}
}

func brokerReconciliation(t *testing.T, format string, configs Configs) {

	testKey := fmt.Sprintf("%s/%s", brokerNamespace, brokerName)

	configs.DataPlaneConfigFormat = format

	// TODO add WantStatusUpdates assertions after https://github.com/knative/eventing/issues/3094

	table := TableTest{
		{
			Name: "Reconciled normal - no DLS",
			Objects: []runtime.Object{
				NewBroker(),
				NewConfigMap(&configs, nil),
				NewService(),
				NewReceiverPod(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				NewDispatcherPod(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "2",
					"annotation_to_preserve":           "value_to_preserve",
				}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
				Eventf(
					corev1.EventTypeNormal,
					Reconciled,
					fmt.Sprintf(`%s reconciled: "%s/%s"`, Broker, brokerNamespace, brokerName),
				),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &coreconfig.Brokers{
					Broker: []*coreconfig.Broker{
						{
							Id:        brokerUUID,
							Topic:     topic(),
							Namespace: brokerNamespace,
							Name:      brokerName,
						},
					},
					VolumeGeneration: 1,
				}),
				ReceiverPodUpdate(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				DispatcherPodUpdate(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
					"annotation_to_preserve":           "value_to_preserve",
				}),
			},
		},
		{
			Name: "Reconciled normal - with DLS",
			Objects: []runtime.Object{
				NewBroker(
					WithDelivery(),
				),
				NewConfigMapFromBrokers(&coreconfig.Brokers{
					VolumeGeneration: 1,
				}, &configs),
				NewService(),
				NewReceiverPod(configs.SystemNamespace, map[string]string{base.VolumeGenerationAnnotationKey: "2"}),
				NewDispatcherPod(configs.SystemNamespace, map[string]string{base.VolumeGenerationAnnotationKey: "2"}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
				Eventf(
					corev1.EventTypeNormal,
					Reconciled,
					fmt.Sprintf(`%s reconciled: "%s/%s"`, Broker, brokerNamespace, brokerName),
				),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &coreconfig.Brokers{
					Broker: []*coreconfig.Broker{
						{
							Id:             brokerUUID,
							Topic:          topic(),
							DeadLetterSink: "http://test-service.test-service-namespace.svc.cluster.local/",
						},
					},
					VolumeGeneration: 2,
				}),
				ReceiverPodUpdate(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "2",
				}),
				DispatcherPodUpdate(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "2",
				}),
			},
		},
		{
			Name: "Failed to create topic",
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
					"failed to create topic: %s: %v",
					topic(), createTopicError,
				),
			},
			OtherTestData: map[string]interface{}{
				wantErrorOnCreateTopic: true,
			},
		},
		{
			Name: "Failed to get config map",
			Objects: []runtime.Object{
				NewBroker(),
				&corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: configs.DataPlaneConfigMapNamespace,
						Name:      configs.DataPlaneConfigMapName + "a",
					},
				},
			},
			Key:     testKey,
			WantErr: true,
			WantEvents: []string{
				finalizerUpdatedEvent,
				Eventf(
					corev1.EventTypeWarning,
					"InternalError",
					"failed to get brokers and triggers config map %s: %v",
					configs.DataPlaneConfigMapAsString(), `configmaps "knative-eventing" not found`,
				),
			},
		},
		{
			Name: "Reconciled normal - config map not readable",
			Objects: []runtime.Object{
				NewBroker(),
				NewConfigMap(&configs, []byte(`{"hello": "world"}`)),
				NewService(),
				NewReceiverPod(configs.SystemNamespace, nil),
				NewDispatcherPod(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "2",
				}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
				Eventf(
					corev1.EventTypeNormal,
					Reconciled,
					fmt.Sprintf(`%s reconciled: "%s/%s"`, Broker, brokerNamespace, brokerName),
				),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &coreconfig.Brokers{
					Broker: []*coreconfig.Broker{
						{
							Id:        brokerUUID,
							Topic:     topic(),
							Namespace: brokerNamespace,
							Name:      brokerName,
						},
					},
					VolumeGeneration: 1,
				}),
				ReceiverPodUpdate(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
				DispatcherPodUpdate(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
		},
		{
			Name: "Reconciled normal - preserve config map previous state",
			Objects: []runtime.Object{
				NewBroker(),
				NewConfigMapFromBrokers(&coreconfig.Brokers{
					Broker: []*coreconfig.Broker{
						{
							Id:             "5384faa4-6bdf-428d-b6c2-d6f89ce1d44b",
							Topic:          "my-existing-topic-a",
							DeadLetterSink: "http://www.my-sink.com",
							Namespace:      brokerNamespace,
							Name:           brokerName,
						},
						{
							Id:             "5384faa4-6bdf-428d-b6c2-d6f89ce1d44a",
							Topic:          "my-existing-topic-b",
							DeadLetterSink: "http://www.my-sink.com",
						},
					},
				}, &configs),
				NewService(),
				NewReceiverPod(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "2",
				}),
				NewDispatcherPod(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "2",
				}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
				Eventf(
					corev1.EventTypeNormal,
					Reconciled,
					fmt.Sprintf(`%s reconciled: "%s/%s"`, Broker, brokerNamespace, brokerName),
				),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &coreconfig.Brokers{
					Broker: []*coreconfig.Broker{
						{
							Id:             "5384faa4-6bdf-428d-b6c2-d6f89ce1d44b",
							Topic:          "my-existing-topic-a",
							DeadLetterSink: "http://www.my-sink.com",
							Namespace:      brokerNamespace,
							Name:           brokerName,
						},
						{
							Id:             "5384faa4-6bdf-428d-b6c2-d6f89ce1d44a",
							Topic:          "my-existing-topic-b",
							DeadLetterSink: "http://www.my-sink.com",
						},
						{
							Id:        brokerUUID,
							Topic:     topic(),
							Namespace: brokerNamespace,
							Name:      brokerName,
						},
					},
					VolumeGeneration: 1,
				}),
				ReceiverPodUpdate(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
				DispatcherPodUpdate(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
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
				NewConfigMapFromBrokers(&coreconfig.Brokers{
					Broker: []*coreconfig.Broker{
						{
							Id:             "5384faa4-6bdf-428d-b6c2-d6f89ce1d44b",
							Topic:          "my-existing-topic-a",
							DeadLetterSink: "http://www.my-sink.com",
						},
						{
							Id:             brokerUUID,
							Topic:          topic(),
							DeadLetterSink: "http://www.my-sink.com",
						},
					},
				}, &configs),
				NewService(),
				NewReceiverPod(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "5",
				}),
				NewDispatcherPod(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "5",
				}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
				Eventf(
					corev1.EventTypeNormal,
					Reconciled,
					fmt.Sprintf(`%s reconciled: "%s/%s"`, Broker, brokerNamespace, brokerName),
				),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &coreconfig.Brokers{
					Broker: []*coreconfig.Broker{
						{
							Id:             "5384faa4-6bdf-428d-b6c2-d6f89ce1d44b",
							Topic:          "my-existing-topic-a",
							DeadLetterSink: "http://www.my-sink.com",
						},
						{
							Id:             brokerUUID,
							Topic:          topic(),
							DeadLetterSink: "http://www.my-sink.com/api",
							Namespace:      brokerNamespace,
							Name:           brokerName,
						},
					},
					VolumeGeneration: 1,
				}),
				ReceiverPodUpdate(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
				DispatcherPodUpdate(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
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
				NewConfigMapFromBrokers(&coreconfig.Brokers{
					Broker: []*coreconfig.Broker{
						{
							Id:             "5384faa4-6bdf-428d-b6c2-d6f89ce1d44b",
							Topic:          "my-existing-topic-a",
							DeadLetterSink: "http://www.my-sink.com",
							Namespace:      brokerNamespace,
							Name:           brokerName,
						},
						{
							Id:             brokerUUID,
							Topic:          topic(),
							DeadLetterSink: "http://www.my-sink.com",
							Namespace:      brokerNamespace,
							Name:           brokerName,
						},
					},
				}, &configs),
				NewService(),
				NewReceiverPod(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "5",
				}),
				NewDispatcherPod(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "5",
				}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
				Eventf(
					corev1.EventTypeNormal,
					Reconciled,
					fmt.Sprintf(`%s reconciled: "%s/%s"`, Broker, brokerNamespace, brokerName),
				),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &coreconfig.Brokers{
					Broker: []*coreconfig.Broker{
						{
							Id:             "5384faa4-6bdf-428d-b6c2-d6f89ce1d44b",
							Topic:          "my-existing-topic-a",
							DeadLetterSink: "http://www.my-sink.com",
							Namespace:      brokerNamespace,
							Name:           brokerName,
						},
						{
							Id:        brokerUUID,
							Topic:     topic(),
							Namespace: brokerNamespace,
							Name:      brokerName,
						},
					},
					VolumeGeneration: 1,
				}),
				ReceiverPodUpdate(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
				DispatcherPodUpdate(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
		},
		{
			Name: "Reconciled normal - increment volume generation",
			Objects: []runtime.Object{
				NewBroker(),
				NewConfigMapFromBrokers(&coreconfig.Brokers{
					Broker: []*coreconfig.Broker{
						{
							Id:             "5384faa4-6bdf-428d-b6c2-d6f89ce1d44b",
							Topic:          "my-existing-topic-a",
							DeadLetterSink: "http://www.my-sink.com",
							Namespace:      brokerNamespace,
							Name:           brokerName,
						},
						{
							Id:        brokerUUID,
							Topic:     topic(),
							Namespace: brokerNamespace,
							Name:      brokerName,
						},
					},
					VolumeGeneration: 1,
				}, &configs),
				NewService(),
				NewReceiverPod(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "5",
				}),
				NewDispatcherPod(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "5",
				}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
				Eventf(
					corev1.EventTypeNormal,
					Reconciled,
					fmt.Sprintf(`%s reconciled: "%s/%s"`, Broker, brokerNamespace, brokerName),
				),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &coreconfig.Brokers{
					Broker: []*coreconfig.Broker{
						{
							Id:             "5384faa4-6bdf-428d-b6c2-d6f89ce1d44b",
							Topic:          "my-existing-topic-a",
							DeadLetterSink: "http://www.my-sink.com",
							Namespace:      brokerNamespace,
							Name:           brokerName,
						},
						{
							Id:        brokerUUID,
							Topic:     topic(),
							Namespace: brokerNamespace,
							Name:      brokerName,
						},
					},
					VolumeGeneration: 2,
				}),
				ReceiverPodUpdate(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "2",
				}),
				DispatcherPodUpdate(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "2",
				}),
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

	for _, f := range formats {
		brokerFinalization(t, f, *DefaultConfigs)
	}
}

func brokerFinalization(t *testing.T, format string, configs Configs) {

	testKey := fmt.Sprintf("%s/%s", brokerNamespace, brokerName)

	configs.DataPlaneConfigFormat = format

	table := TableTest{
		{
			Name: "Reconciled normal - no DLS",
			Objects: []runtime.Object{
				NewDeletedBroker(),
				NewConfigMapFromBrokers(&coreconfig.Brokers{
					Broker: []*coreconfig.Broker{
						{
							Id:        brokerUUID,
							Topic:     topic(),
							Namespace: brokerNamespace,
							Name:      brokerName,
						},
					},
					VolumeGeneration: 1,
				}, &configs),
			},
			Key: testKey,
			WantEvents: []string{
				Eventf(
					corev1.EventTypeNormal,
					Reconciled,
					fmt.Sprintf(`%s reconciled: "%s/%s"`, Broker, brokerNamespace, brokerName),
				),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &coreconfig.Brokers{
					Broker:           []*coreconfig.Broker{},
					VolumeGeneration: 1,
				}),
			},
		},
		{
			Name: "Reconciled normal - with DLS",
			Objects: []runtime.Object{
				NewDeletedBroker(
					WithDelivery(),
				),
				NewConfigMapFromBrokers(&coreconfig.Brokers{
					Broker: []*coreconfig.Broker{
						{
							Id:             brokerUUID,
							Topic:          topic(),
							DeadLetterSink: "http://test-service.test-service-namespace.svc.cluster.local/",
							Namespace:      brokerNamespace,
							Name:           brokerName,
						},
					},
					VolumeGeneration: 1,
				}, &configs),
			},
			Key: testKey,
			WantEvents: []string{
				Eventf(
					corev1.EventTypeNormal,
					Reconciled,
					fmt.Sprintf(`%s reconciled: "%s/%s"`, Broker, brokerNamespace, brokerName),
				),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &coreconfig.Brokers{
					VolumeGeneration: 1,
				}),
			},
		},
		{
			Name: "Failed to delete topic",
			Objects: []runtime.Object{
				NewDeletedBroker(),
				NewConfigMapFromBrokers(&coreconfig.Brokers{
					Broker: []*coreconfig.Broker{
						{
							Id:             brokerUUID,
							Topic:          topic(),
							DeadLetterSink: "http://test-service.test-service-namespace.svc.cluster.local/",
						},
					},
					VolumeGeneration: 1,
				}, &configs),
			},
			Key:     testKey,
			WantErr: true,
			WantEvents: []string{
				Eventf(
					corev1.EventTypeWarning,
					"InternalError",
					"failed to delete topic %s: %v",
					topic(), deleteTopicError,
				),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &coreconfig.Brokers{
					VolumeGeneration: 1,
				}),
			},
			OtherTestData: map[string]interface{}{
				wantErrorOnDeleteTopic: true,
			},
		},
		{
			Name: "Failed to get config map",
			Objects: []runtime.Object{
				NewDeletedBroker(),
				&corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: configs.DataPlaneConfigMapNamespace,
						Name:      configs.DataPlaneConfigMapName + "a",
					},
				},
			},
			Key:     testKey,
			WantErr: true,
			WantEvents: []string{
				Eventf(
					corev1.EventTypeWarning,
					"InternalError",
					"failed to get brokers and triggers config map %s: %v",
					configs.DataPlaneConfigMapAsString(), `configmaps "knative-eventing" not found`,
				),
			},
		},
		{
			Name: "Config map not readable",
			Objects: []runtime.Object{
				NewDeletedBroker(),
				NewConfigMap(&configs, []byte(`{"hello"-- "world"}`)),
			},
			Key:     testKey,
			WantErr: true,
			WantEvents: []string{
				Eventf(
					corev1.EventTypeWarning,
					"InternalError",
					"failed to get brokers and triggers: failed to unmarshal brokers and triggers: %v",
					getUnmarshallableError(format),
				),
			},
		},
		{
			Name: "Reconciled normal - preserve config map previous state",
			Objects: []runtime.Object{
				NewDeletedBroker(),
				NewConfigMapFromBrokers(&coreconfig.Brokers{
					Broker: []*coreconfig.Broker{
						{
							Id:             "5384faa4-6bdf-428d-b6c2-d6f89ce1d44b",
							Topic:          "my-existing-topic-a",
							DeadLetterSink: "http://www.my-sink.com",
						},
						{
							Id:             brokerUUID,
							Topic:          "my-existing-topic-b",
							DeadLetterSink: "http://www.my-sink.com",
						},
					},
					VolumeGeneration: 5,
				}, &configs),
			},
			Key: testKey,
			WantEvents: []string{
				Eventf(
					corev1.EventTypeNormal,
					Reconciled,
					fmt.Sprintf(`%s reconciled: "%s/%s"`, Broker, brokerNamespace, brokerName),
				),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &coreconfig.Brokers{
					Broker: []*coreconfig.Broker{
						{
							Id:             "5384faa4-6bdf-428d-b6c2-d6f89ce1d44b",
							Topic:          "my-existing-topic-a",
							DeadLetterSink: "http://www.my-sink.com",
						},
					},
					VolumeGeneration: 5,
				}),
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
		if want, ok := row.OtherTestData[wantErrorOnCreateTopic]; ok && want.(bool) {
			onCreateTopicError = createTopicError
		}

		var onDeleteTopicError error
		if want, ok := row.OtherTestData[wantErrorOnDeleteTopic]; ok && want.(bool) {
			onDeleteTopicError = deleteTopicError
		}

		clusterAdmin := &MockKafkaClusterAdmin{
			ExpectedTopicName:   fmt.Sprintf("%s%s-%s", TopicPrefix, brokerNamespace, brokerName),
			ExpectedTopicDetail: defaultTopicDetail,
			ErrorOnCreateTopic:  onCreateTopicError,
			ErrorOnDeleteTopic:  onDeleteTopicError,
			T:                   t,
		}

		reconciler := &Reconciler{
			Reconciler: &base.Reconciler{
				KubeClient:                  kubeclient.Get(ctx),
				PodLister:                   listers.GetPodLister(),
				DataPlaneConfigMapNamespace: configs.DataPlaneConfigMapNamespace,
				DataPlaneConfigMapName:      configs.DataPlaneConfigMapName,
				DataPlaneConfigFormat:       configs.DataPlaneConfigFormat,
				SystemNamespace:             configs.SystemNamespace,
			},
			KafkaClusterAdmin:            clusterAdmin,
			KafkaDefaultTopicDetails:     defaultTopicDetail,
			KafkaDefaultTopicDetailsLock: sync.RWMutex{},
			Configs:                      configs,
			Recorder:                     controller.GetEventRecorder(ctx),
		}

		r := brokerreconciler.NewReconciler(
			ctx,
			logging.FromContext(ctx),
			fakeeventingclient.Get(ctx),
			listers.GetBrokerLister(),
			controller.GetEventRecorder(ctx),
			reconciler,
			kafka.BrokerClass,
		)

		reconciler.Resolver = resolver.NewURIResolver(ctx, func(name types.NamespacedName) {})

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

	NewClusterAdmin = func(addrs []string, conf *sarama.Config) (sarama.ClusterAdmin, error) {
		return MockKafkaClusterAdmin{}, nil
	}

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
	assert.NotNil(t, reconciler.KafkaClusterAdmin)
}

func topic() string {
	return fmt.Sprintf("%s%s-%s", TopicPrefix, brokerNamespace, brokerName)
}

func NewDispatcherPod(namespace string, annotations map[string]string) runtime.Object {
	return &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        "kafka-broker-dispatcher",
			Namespace:   namespace,
			Annotations: annotations,
			Labels: map[string]string{
				"app": base.DispatcherLabel,
			},
		},
	}
}

func NewReceiverPod(namespace string, annotations map[string]string) runtime.Object {
	return &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        "kafka-broker-receiver",
			Namespace:   namespace,
			Annotations: annotations,
			Labels: map[string]string{
				"app": base.ReceiverLabel,
			},
		},
	}
}

func DispatcherPodUpdate(namespace string, annotations map[string]string) clientgotesting.UpdateActionImpl {
	return clientgotesting.NewUpdateAction(
		schema.GroupVersionResource{
			Group:    "*",
			Version:  "v1",
			Resource: "Pod",
		},
		namespace,
		NewDispatcherPod(namespace, annotations),
	)
}

func ReceiverPodUpdate(namespace string, annotations map[string]string) clientgotesting.UpdateActionImpl {
	return clientgotesting.NewUpdateAction(
		schema.GroupVersionResource{
			Group:    "*",
			Version:  "v1",
			Resource: "Pod",
		},
		namespace,
		NewReceiverPod(namespace, annotations),
	)
}

func NewService() *corev1.Service {
	return reconcilertesting.NewService(
		serviceName,
		serviceNamespace,
		func(service *corev1.Service) {
			service.APIVersion = "v1"
			service.Kind = "Service"
		},
	)
}

func NewConfigMap(configs *Configs, data []byte) runtime.Object {
	return reconcilertesting.NewConfigMap(
		configs.DataPlaneConfigMapName,
		configs.DataPlaneConfigMapNamespace,
		func(configMap *corev1.ConfigMap) {
			if configMap.BinaryData == nil {
				configMap.BinaryData = make(map[string][]byte, 1)
			}

			configMap.BinaryData[base.ConfigMapDataKey] = data
		},
	)
}

func NewConfigMapFromBrokers(brokers *coreconfig.Brokers, configs *Configs) runtime.Object {
	var data []byte
	var err error
	if configs.DataPlaneConfigFormat == base.Protobuf {
		data, err = proto.Marshal(brokers)
	} else {
		data, err = json.Marshal(brokers)
	}
	if err != nil {
		panic(err)
	}

	return NewConfigMap(configs, data)
}

func ConfigMapUpdate(configs *Configs, brokers *coreconfig.Brokers) clientgotesting.UpdateActionImpl {
	return clientgotesting.NewUpdateAction(
		schema.GroupVersionResource{
			Group:    "*",
			Version:  "v1",
			Resource: "ConfigMap",
		},
		configs.DataPlaneConfigMapNamespace,
		NewConfigMapFromBrokers(brokers, configs),
	)
}

// NewBroker creates a new Broker with broker class equals to kafka.BrokerClass
func NewBroker(options ...reconcilertesting.BrokerOption) runtime.Object {
	return reconcilertesting.NewBroker(
		brokerName,
		brokerNamespace,
		append(
			options,
			reconcilertesting.WithBrokerClass(kafka.BrokerClass),
			func(broker *eventing.Broker) {
				broker.UID = brokerUUID
			},
		)...,
	)
}

func NewDeletedBroker(options ...reconcilertesting.BrokerOption) runtime.Object {
	return NewBroker(
		append(
			options,
			func(broker *eventing.Broker) {
				broker.DeletionTimestamp = &metav1.Time{Time: time.Now()}
			},
		)...,
	)
}

func WithDelivery() func(*eventing.Broker) {
	service := NewService()

	return func(broker *eventing.Broker) {
		broker.Spec.Delivery = &eventingduck.DeliverySpec{
			DeadLetterSink: &duckv1.Destination{
				Ref: &duckv1.KReference{
					Kind:       service.Kind,
					Namespace:  service.Namespace,
					Name:       service.Name,
					APIVersion: service.APIVersion,
				},
			},
		}
	}
}

func getUnmarshallableError(format string) interface{} {
	if format == base.Protobuf {
		return "unexpected EOF"
	}
	return "invalid character '-' after object key"
}
