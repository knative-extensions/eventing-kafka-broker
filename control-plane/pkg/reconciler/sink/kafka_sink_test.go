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

package sink

import (
	"context"
	"fmt"
	"testing"

	"github.com/Shopify/sarama"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgotesting "k8s.io/client-go/testing"
	kubeclient "knative.dev/pkg/client/injection/kube/client/fake"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"

	. "knative.dev/pkg/reconciler/testing"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/apis/eventing"
	v1alpha1 "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/eventing/v1alpha1"
	fakeeventingkafkaclient "knative.dev/eventing-kafka-broker/control-plane/pkg/client/injection/client/fake"
	sinkreconciler "knative.dev/eventing-kafka-broker/control-plane/pkg/client/injection/reconciler/eventing/v1alpha1/kafkasink"
	coreconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/core/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/receiver"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/broker"
	kafkatesting "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka/testing"
	. "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/testing"
)

const (
	finalizerName = "kafkasinks." + eventing.GroupName

	bootstrapServers = "kafka-1:9092,kafka-2:9093"

	wantTopicName          = "wantTopicName"
	wantErrorOnCreateTopic = "wantErrorOnCreateTopic"
	wantErrorOnDeleteTopic = "wantErrorOnDeleteTopic"
	ExpectedTopicDetail    = "expectedTopicDetail"

	TopicPrefix = "knative-sink-"
)

var (
	finalizerUpdatedEvent = Eventf(
		corev1.EventTypeNormal,
		"FinalizerUpdate",
		fmt.Sprintf(`Updated %q finalizers`, SinkName),
	)

	bootstrapServersArr = []string{"kafka-1:9092", "kafka-2:9093"}

	createTopicError = fmt.Errorf("failed to create topic")
	deleteTopicError = fmt.Errorf("failed to delete topic")
)

func TestSinkReconciler(t *testing.T) {

	t.Parallel()

	for _, f := range Formats {
		sinkReconciliation(t, f, *DefaultConfigs)
	}
}

func sinkReconciliation(t *testing.T, format string, configs broker.Configs) {

	testKey := fmt.Sprintf("%s/%s", SinkNamespace, SinkName)

	configs.DataPlaneConfigFormat = format

	table := TableTest{
		{
			Name: "Reconciled normal",
			Objects: []runtime.Object{
				NewSink(),
				NewConfigMap(&configs, nil),
				SinkReceiverPod(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
					"annotation_to_preserve":           "value_to_preserve",
				}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &coreconfig.Brokers{
					Brokers: []*coreconfig.Broker{
						{
							Id:               SinkUUID,
							Topic:            SinkTopic(),
							Path:             receiver.Path(SinkNamespace, SinkName),
							BootstrapServers: bootstrapServers,
							ContentMode:      coreconfig.ContentMode_STRUCTURED,
						},
					},
					VolumeGeneration: 1,
				}),
				SinkReceiverPodUpdate(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
					"annotation_to_preserve":           "value_to_preserve",
				}),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSink(
						InitSinkConditions,
						BootstrapServers(bootstrapServersArr),
						SinkConfigMapUpdatedReady(&configs.Env),
						SinkTopicReady,
						SinkAddressable(&configs.Env),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - set topic and bootstrap servers",
			Objects: []runtime.Object{
				NewSink(
					func(sink *v1alpha1.KafkaSink) {
						sink.Spec.Topic = "my-topic-1"
						sink.Spec.BootstrapServers = []string{"kafka-broker:10000"}
					},
				),
				NewConfigMap(&configs, nil),
				SinkReceiverPod(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
					"annotation_to_preserve":           "value_to_preserve",
				}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &coreconfig.Brokers{
					Brokers: []*coreconfig.Broker{
						{
							Id:               SinkUUID,
							Topic:            "my-topic-1",
							Path:             receiver.Path(SinkNamespace, SinkName),
							BootstrapServers: "kafka-broker:10000",
							ContentMode:      coreconfig.ContentMode_STRUCTURED,
						},
					},
					VolumeGeneration: 1,
				}),
				SinkReceiverPodUpdate(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
					"annotation_to_preserve":           "value_to_preserve",
				}),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSink(
						func(sink *v1alpha1.KafkaSink) {
							sink.Spec.Topic = "my-topic-1"
							sink.Spec.BootstrapServers = []string{"kafka-broker:10000"}
						},
						InitSinkConditions,
						BootstrapServers([]string{"kafka-broker:10000"}),
						SinkConfigMapUpdatedReady(&configs.Env),
						SinkTopicReadyWithName("my-topic-1"),
						SinkAddressable(&configs.Env),
					),
				},
			},
			OtherTestData: map[string]interface{}{
				wantTopicName: "my-topic-1",
			},
		},
		{
			Name: "Failed to create topic",
			Objects: []runtime.Object{
				NewSink(
					BootstrapServers(bootstrapServersArr),
				),
			},
			Key:     testKey,
			WantErr: true,
			WantEvents: []string{
				finalizerUpdatedEvent,
				Eventf(
					corev1.EventTypeWarning,
					"InternalError",
					"failed to create topic: %s: %v",
					SinkTopic(), createTopicError,
				),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSink(
						InitSinkConditions,
						BootstrapServers(bootstrapServersArr),
						SinkFailedToCreateTopic,
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
				NewSink(
					BootstrapServers(bootstrapServersArr),
				),
				NewService(),
				SinkReceiverPod(configs.SystemNamespace, map[string]string{base.VolumeGenerationAnnotationKey: "2"}),
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
				ConfigMapUpdate(&configs, &coreconfig.Brokers{
					Brokers: []*coreconfig.Broker{
						{
							Id:               SinkUUID,
							Topic:            SinkTopic(),
							Path:             receiver.Path(SinkNamespace, SinkName),
							BootstrapServers: bootstrapServers,
							ContentMode:      coreconfig.ContentMode_STRUCTURED,
						},
					},
					VolumeGeneration: 1,
				}),
				SinkReceiverPodUpdate(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSink(
						InitSinkConditions,
						BootstrapServers(bootstrapServersArr),
						SinkConfigMapUpdatedReady(&configs.Env),
						SinkTopicReady,
						SinkAddressable(&configs.Env),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - config map not readable",
			Objects: []runtime.Object{
				NewSink(
					BootstrapServers(bootstrapServersArr),
				),
				NewConfigMap(&configs, []byte(`{"hello": "world"}`)),
				SinkReceiverPod(configs.SystemNamespace, nil),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &coreconfig.Brokers{
					Brokers: []*coreconfig.Broker{
						{
							Id:               SinkUUID,
							Topic:            SinkTopic(),
							Path:             receiver.Path(SinkNamespace, SinkName),
							BootstrapServers: bootstrapServers,
							ContentMode:      coreconfig.ContentMode_STRUCTURED,
						},
					},
					VolumeGeneration: 1,
				}),
				SinkReceiverPodUpdate(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSink(
						InitSinkConditions,
						BootstrapServers(bootstrapServersArr),
						SinkConfigMapUpdatedReady(&configs.Env),
						SinkTopicReady,
						SinkAddressable(&configs.Env),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - preserve config map previous state",
			Objects: []runtime.Object{
				NewSink(
					BootstrapServers(bootstrapServersArr),
				),
				NewConfigMapFromBrokers(&coreconfig.Brokers{
					Brokers: []*coreconfig.Broker{
						{
							Id:    "5384faa4-6bdf-428d-b6c2-d6f89ce1d44b",
							Topic: "my-existing-topic-a",
							Path:  receiver.Path(SinkNamespace, SinkName),
						},
						{
							Id:    "5384faa4-6bdf-428d-b6c2-d6f89ce1d44a",
							Topic: "my-existing-topic-b",
						},
					},
				}, &configs),
				SinkReceiverPod(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "2",
				}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &coreconfig.Brokers{
					Brokers: []*coreconfig.Broker{
						{
							Id:    "5384faa4-6bdf-428d-b6c2-d6f89ce1d44b",
							Topic: "my-existing-topic-a",
							Path:  receiver.Path(SinkNamespace, SinkName),
						},
						{
							Id:    "5384faa4-6bdf-428d-b6c2-d6f89ce1d44a",
							Topic: "my-existing-topic-b",
						},
						{
							Id:               SinkUUID,
							Topic:            SinkTopic(),
							Path:             receiver.Path(SinkNamespace, SinkName),
							BootstrapServers: bootstrapServers,
							ContentMode:      coreconfig.ContentMode_STRUCTURED,
						},
					},
					VolumeGeneration: 1,
				}),
				SinkReceiverPodUpdate(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSink(
						InitSinkConditions,
						BootstrapServers(bootstrapServersArr),
						SinkConfigMapUpdatedReady(&configs.Env),
						SinkTopicReady,
						SinkAddressable(&configs.Env),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - update existing broker while preserving others",
			Objects: []runtime.Object{
				NewSink(
					BootstrapServers(bootstrapServersArr),
				),
				NewConfigMapFromBrokers(&coreconfig.Brokers{
					Brokers: []*coreconfig.Broker{
						{
							Id:             "5384faa4-6bdf-428d-b6c2-d6f89ce1d44b",
							Topic:          "my-existing-topic-a",
							DeadLetterSink: "http://www.my-sink.com",
						},
						{
							Id:               SinkUUID,
							Topic:            SinkTopic(),
							BootstrapServers: bootstrapServers,
							ContentMode:      coreconfig.ContentMode_STRUCTURED,
						},
					},
				}, &configs),
				SinkReceiverPod(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "5",
				}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &coreconfig.Brokers{
					Brokers: []*coreconfig.Broker{
						{
							Id:             "5384faa4-6bdf-428d-b6c2-d6f89ce1d44b",
							Topic:          "my-existing-topic-a",
							DeadLetterSink: "http://www.my-sink.com",
						},
						{
							Id:               SinkUUID,
							Topic:            SinkTopic(),
							Path:             receiver.Path(SinkNamespace, SinkName),
							BootstrapServers: bootstrapServers,
							ContentMode:      coreconfig.ContentMode_STRUCTURED,
						},
					},
					VolumeGeneration: 1,
				}),
				SinkReceiverPodUpdate(configs.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSink(
						InitSinkConditions,
						BootstrapServers(bootstrapServersArr),
						SinkConfigMapUpdatedReady(&configs.Env),
						SinkTopicReady,
						SinkAddressable(&configs.Env),
					),
				},
			},
		},
	}

	for i := range table {
		table[i].Name = table[i].Name + " - " + format
	}

	useTable(t, table, &configs)
}

func useTable(t *testing.T, table TableTest, configs *broker.Configs) {

	table.Test(t, NewFactory(configs, func(ctx context.Context, listers *Listers, configs *broker.Configs, row *TableRow) controller.Reconciler {

		expectedTopicName := fmt.Sprintf("%s%s-%s", TopicPrefix, SinkNamespace, SinkName)
		if want, ok := row.OtherTestData[wantTopicName]; ok {
			expectedTopicName = want.(string)
		}

		var onCreateTopicError error
		if want, ok := row.OtherTestData[wantErrorOnCreateTopic]; ok {
			onCreateTopicError = want.(error)
		}

		var onDeleteTopicError error
		if want, ok := row.OtherTestData[wantErrorOnDeleteTopic]; ok {
			onDeleteTopicError = want.(error)
		}

		defaultTopicDetail := sarama.TopicDetail{
			NumPartitions:     SinkNumPartitions,
			ReplicationFactor: SinkReplicationFactor,
		}

		expectedTopicDetail := defaultTopicDetail
		if td, ok := row.OtherTestData[ExpectedTopicDetail]; ok {
			expectedTopicDetail = td.(sarama.TopicDetail)
		}

		reconciler := &Reconciler{
			Reconciler: &base.Reconciler{
				KubeClient:                  kubeclient.Get(ctx),
				PodLister:                   listers.GetPodLister(),
				DataPlaneConfigMapNamespace: configs.DataPlaneConfigMapNamespace,
				DataPlaneConfigMapName:      configs.DataPlaneConfigMapName,
				DataPlaneConfigFormat:       configs.DataPlaneConfigFormat,
				SystemNamespace:             configs.SystemNamespace,
				ReceiverLabel:               base.SinkReceiverLabel,
			},
			ConfigMapLister: listers.GetConfigMapLister(),
			ClusterAdmin: func(addrs []string, config *sarama.Config) (sarama.ClusterAdmin, error) {
				return &kafkatesting.MockKafkaClusterAdmin{
					ExpectedTopicName:   expectedTopicName,
					ExpectedTopicDetail: expectedTopicDetail,
					ErrorOnCreateTopic:  onCreateTopicError,
					ErrorOnDeleteTopic:  onDeleteTopicError,
					T:                   t,
				}, nil
			},
			Configs: &configs.Env,
		}

		return sinkreconciler.NewReconciler(
			ctx,
			logging.FromContext(ctx),
			fakeeventingkafkaclient.Get(ctx),
			listers.GetKafkaSinkLister(),
			controller.GetEventRecorder(ctx),
			reconciler,
		)
	}))
}

func patchFinalizers() clientgotesting.PatchActionImpl {
	action := clientgotesting.PatchActionImpl{}
	action.Name = SinkName
	action.Namespace = SinkNamespace
	patch := `{"metadata":{"finalizers":["` + finalizerName + `"],"resourceVersion":""}}`
	action.Patch = []byte(patch)
	return action
}
