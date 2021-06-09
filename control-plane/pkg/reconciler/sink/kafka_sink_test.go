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

package sink_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

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
	"knative.dev/eventing-kafka-broker/control-plane/pkg/apis/eventing/v1alpha1"
	fakeeventingkafkaclient "knative.dev/eventing-kafka-broker/control-plane/pkg/client/injection/client/fake"
	sinkreconciler "knative.dev/eventing-kafka-broker/control-plane/pkg/client/injection/reconciler/eventing/v1alpha1/kafkasink"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/receiver"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/broker"
	kafkatesting "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka/testing"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/sink"
	. "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/testing"
	testinghttp "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/testing/http"
)

const (
	finalizerName = "kafkasinks." + eventing.GroupName

	bootstrapServers = "kafka-1:9092,kafka-2:9093"

	wantTopicName                  = "wantTopicName"
	wantErrorOnCreateTopic         = "wantErrorOnCreateTopic"
	wantErrorOnDeleteTopic         = "wantErrorOnDeleteTopic"
	ExpectedTopicDetail            = "expectedTopicDetail"
	ExpectedTopicsOnDescribeTopics = "expectedTopicsOnDescribeTopics"
	ExpectedTopicIsPresent         = "expectedTopicIsPresent"
	ExpectedErrorOnDescribeTopics  = "expectedErrorOnDescribeTopics"
	wantErrorOnProbeReceivers      = "wantErrorOnProbeReceivers"

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

	v1alpha1.RegisterConditionSet(base.ConditionSet)

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
				NewSink(
					SinkControllerOwnsTopic,
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
				ConfigMapUpdate(&configs, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:              SinkUUID,
							Topics:           []string{SinkTopic()},
							Ingress:          &contract.Ingress{ContentMode: contract.ContentMode_STRUCTURED, IngressType: &contract.Ingress_Path{Path: receiver.Path(SinkNamespace, SinkName)}},
							BootstrapServers: bootstrapServers,
						},
					},
					Generation: 1,
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
						SinkControllerOwnsTopic,
						InitSinkConditions,
						SinkDataPlaneAvailable,
						SinkConfigParsed,
						SinkProbeSucceeded,
						BootstrapServers(bootstrapServersArr),
						SinkConfigMapUpdatedReady(&configs.Env),
						SinkTopicReady,
						SinkTopicReadyWithOwner(SinkTopic(), sink.ControllerTopicOwner),
						SinkAddressable(&configs.Env),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - with auth config",
			Objects: []runtime.Object{
				NewSink(
					SinkControllerOwnsTopic,
					SinkAuthSecretRef("secret-1"),
				),
				NewSSLSecret(SinkNamespace, "secret-1"),
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
				ConfigMapUpdate(&configs, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:              SinkUUID,
							Topics:           []string{SinkTopic()},
							BootstrapServers: bootstrapServers,
							Ingress:          &contract.Ingress{ContentMode: contract.ContentMode_STRUCTURED, IngressType: &contract.Ingress_Path{Path: receiver.Path(SinkNamespace, SinkName)}},
							Auth: &contract.Resource_AuthSecret{
								AuthSecret: &contract.Reference{
									Uuid:      SecretUUID,
									Namespace: SinkNamespace,
									Name:      "secret-1",
									Version:   SecretResourceVersion,
								},
							},
						},
					},
					Generation: 1,
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
						SinkControllerOwnsTopic,
						SinkAuthSecretRef("secret-1"),
						InitSinkConditions,
						SinkDataPlaneAvailable,
						SinkConfigParsed,
						SinkProbeSucceeded,
						BootstrapServers(bootstrapServersArr),
						SinkConfigMapUpdatedReady(&configs.Env),
						SinkTopicReady,
						SinkTopicReadyWithOwner(SinkTopic(), sink.ControllerTopicOwner),
						SinkAddressable(&configs.Env),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - no topic owner",
			Objects: []runtime.Object{
				NewSink(
					SinkControllerDontOwnTopic,
					func(sink *v1alpha1.KafkaSink) {
						sink.Spec.ReplicationFactor = nil
						sink.Spec.NumPartitions = nil
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
				ConfigMapUpdate(&configs, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:              SinkUUID,
							Topics:           []string{SinkTopic()},
							BootstrapServers: bootstrapServers,
							Ingress: &contract.Ingress{
								ContentMode: contract.ContentMode_STRUCTURED,
								IngressType: &contract.Ingress_Path{
									Path: receiver.Path(SinkNamespace, SinkName),
								},
							},
						},
					},
					Generation: 1,
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
						SinkControllerDontOwnTopic,
						func(sink *v1alpha1.KafkaSink) {
							sink.Spec.ReplicationFactor = nil
							sink.Spec.NumPartitions = nil
						},
						InitSinkConditions,
						SinkDataPlaneAvailable,
						SinkConfigParsed,
						SinkProbeSucceeded,
						BootstrapServers(bootstrapServersArr),
						SinkConfigMapUpdatedReady(&configs.Env),
						SinkTopicReady,
						SinkTopicReadyWithOwner(SinkTopic(), sink.ExternalTopicOwner),
						SinkAddressable(&configs.Env),
					),
				},
			},
		},
		{
			Name: "No topic owner - topic present err",
			Objects: []runtime.Object{
				NewSink(
					SinkControllerDontOwnTopic,
					func(sink *v1alpha1.KafkaSink) {
						sink.Spec.ReplicationFactor = nil
						sink.Spec.NumPartitions = nil
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
				Eventf(
					corev1.EventTypeWarning,
					"InternalError",
					"topic is not present: "+SinkNotPresentErrFormat,
					SinkTopic(), io.EOF,
				),
			},
			WantErr: true,
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSink(
						SinkControllerDontOwnTopic,
						func(sink *v1alpha1.KafkaSink) {
							sink.Spec.ReplicationFactor = nil
							sink.Spec.NumPartitions = nil
						},
						InitSinkConditions,
						SinkDataPlaneAvailable,
						SinkTopicNotPresentErr(SinkTopic(), io.EOF),
					),
				},
			},
			OtherTestData: map[string]interface{}{
				ExpectedErrorOnDescribeTopics: io.EOF,
			},
		},
		{
			Name: "Reconciled normal - set topic and bootstrap servers",
			Objects: []runtime.Object{
				NewSink(
					SinkControllerOwnsTopic,
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
				ConfigMapUpdate(&configs, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:              SinkUUID,
							Topics:           []string{"my-topic-1"},
							Ingress:          &contract.Ingress{ContentMode: contract.ContentMode_STRUCTURED, IngressType: &contract.Ingress_Path{Path: receiver.Path(SinkNamespace, SinkName)}},
							BootstrapServers: "kafka-broker:10000",
						},
					},
					Generation: 1,
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
						SinkControllerOwnsTopic,
						func(sink *v1alpha1.KafkaSink) {
							sink.Spec.Topic = "my-topic-1"
							sink.Spec.BootstrapServers = []string{"kafka-broker:10000"}
						},
						InitSinkConditions,
						SinkDataPlaneAvailable,
						SinkConfigParsed,
						SinkProbeSucceeded,
						BootstrapServers([]string{"kafka-broker:10000"}),
						SinkTopicReadyWithOwner("my-topic-1", sink.ControllerTopicOwner),
						SinkConfigMapUpdatedReady(&configs.Env),
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
					SinkControllerOwnsTopic,
					BootstrapServers(bootstrapServersArr),
				),
				SinkReceiverPod(configs.SystemNamespace, nil),
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
						SinkControllerOwnsTopic,
						InitSinkConditions,
						SinkDataPlaneAvailable,
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
					SinkControllerOwnsTopic,
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
				ConfigMapUpdate(&configs, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:              SinkUUID,
							Topics:           []string{SinkTopic()},
							Ingress:          &contract.Ingress{ContentMode: contract.ContentMode_STRUCTURED, IngressType: &contract.Ingress_Path{Path: receiver.Path(SinkNamespace, SinkName)}},
							BootstrapServers: bootstrapServers,
						},
					},
					Generation: 1,
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
						SinkControllerOwnsTopic,
						InitSinkConditions,
						SinkDataPlaneAvailable,
						SinkConfigParsed,
						SinkProbeSucceeded,
						BootstrapServers(bootstrapServersArr),
						SinkConfigMapUpdatedReady(&configs.Env),
						SinkTopicReady,
						SinkTopicReadyWithOwner(SinkTopic(), sink.ControllerTopicOwner),
						SinkAddressable(&configs.Env),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - config map not readable",
			Objects: []runtime.Object{
				NewSink(
					SinkControllerOwnsTopic,
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
				ConfigMapUpdate(&configs, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:              SinkUUID,
							Topics:           []string{SinkTopic()},
							Ingress:          &contract.Ingress{ContentMode: contract.ContentMode_STRUCTURED, IngressType: &contract.Ingress_Path{Path: receiver.Path(SinkNamespace, SinkName)}},
							BootstrapServers: bootstrapServers,
						},
					},
					Generation: 1,
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
						SinkControllerOwnsTopic,
						InitSinkConditions,
						SinkDataPlaneAvailable,
						SinkConfigParsed,
						SinkProbeSucceeded,
						BootstrapServers(bootstrapServersArr),
						SinkConfigMapUpdatedReady(&configs.Env),
						SinkTopicReady,
						SinkTopicReadyWithOwner(SinkTopic(), sink.ControllerTopicOwner),
						SinkAddressable(&configs.Env),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - preserve config map previous state",
			Objects: []runtime.Object{
				NewSink(
					SinkControllerOwnsTopic,
					BootstrapServers(bootstrapServersArr),
				),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     "5384faa4-6bdf-428d-b6c2-d6f89ce1d44b",
							Topics:  []string{"my-existing-topic-a"},
							Ingress: &contract.Ingress{IngressType: &contract.Ingress_Path{Path: receiver.Path(SinkNamespace, SinkName)}},
						},
						{
							Uid:    "5384faa4-6bdf-428d-b6c2-d6f89ce1d44a",
							Topics: []string{"my-existing-topic-b"},
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
				ConfigMapUpdate(&configs, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     "5384faa4-6bdf-428d-b6c2-d6f89ce1d44b",
							Topics:  []string{"my-existing-topic-a"},
							Ingress: &contract.Ingress{IngressType: &contract.Ingress_Path{Path: receiver.Path(SinkNamespace, SinkName)}},
						},
						{
							Uid:    "5384faa4-6bdf-428d-b6c2-d6f89ce1d44a",
							Topics: []string{"my-existing-topic-b"},
						},
						{
							Uid:              SinkUUID,
							Topics:           []string{SinkTopic()},
							Ingress:          &contract.Ingress{ContentMode: contract.ContentMode_STRUCTURED, IngressType: &contract.Ingress_Path{Path: receiver.Path(SinkNamespace, SinkName)}},
							BootstrapServers: bootstrapServers,
						},
					},
					Generation: 1,
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
						SinkControllerOwnsTopic,
						InitSinkConditions,
						SinkDataPlaneAvailable,
						SinkConfigParsed,
						SinkProbeSucceeded,
						BootstrapServers(bootstrapServersArr),
						SinkConfigMapUpdatedReady(&configs.Env),
						SinkTopicReady,
						SinkTopicReadyWithOwner(SinkTopic(), sink.ControllerTopicOwner),
						SinkAddressable(&configs.Env),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - update existing broker while preserving others",
			Objects: []runtime.Object{
				NewSink(
					SinkControllerOwnsTopic,
					BootstrapServers(bootstrapServersArr),
				),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:          "5384faa4-6bdf-428d-b6c2-d6f89ce1d44b",
							Topics:       []string{"my-existing-topic-a"},
							EgressConfig: &contract.EgressConfig{DeadLetter: "http://www.my-sink.com"},
						},
						{
							Uid:              SinkUUID,
							Topics:           []string{SinkTopic()},
							BootstrapServers: bootstrapServers,
							Ingress:          &contract.Ingress{ContentMode: contract.ContentMode_STRUCTURED},
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
				ConfigMapUpdate(&configs, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:          "5384faa4-6bdf-428d-b6c2-d6f89ce1d44b",
							Topics:       []string{"my-existing-topic-a"},
							EgressConfig: &contract.EgressConfig{DeadLetter: "http://www.my-sink.com"},
						},
						{
							Uid:              SinkUUID,
							Topics:           []string{SinkTopic()},
							Ingress:          &contract.Ingress{ContentMode: contract.ContentMode_STRUCTURED, IngressType: &contract.Ingress_Path{Path: receiver.Path(SinkNamespace, SinkName)}},
							BootstrapServers: bootstrapServers,
						},
					},
					Generation: 1,
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
						SinkControllerOwnsTopic,
						InitSinkConditions,
						SinkDataPlaneAvailable,
						SinkConfigParsed,
						SinkProbeSucceeded,
						BootstrapServers(bootstrapServersArr),
						SinkConfigMapUpdatedReady(&configs.Env),
						SinkTopicReadyWithOwner(SinkTopic(), sink.ControllerTopicOwner),
						SinkAddressable(&configs.Env),
					),
				},
			},
		},
		{
			Name: "no data plane pods running",
			Objects: []runtime.Object{
				NewSink(),
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
					Object: NewSink(
						InitSinkConditions,
						SinkDataPlaneNotAvailable,
					),
				},
			},
		},
		{
			Name: "Reconciled normal - unchanged",
			Objects: []runtime.Object{
				NewSink(
					SinkControllerOwnsTopic,
					BootstrapServers(bootstrapServersArr),
				),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     "5384faa4-6bdf-428d-b6c2-d6f89ce1d44b",
							Topics:  []string{"my-existing-topic-a"},
							Ingress: &contract.Ingress{IngressType: &contract.Ingress_Path{Path: receiver.Path(SinkNamespace, SinkName)}},
						},
						{
							Uid:    "5384faa4-6bdf-428d-b6c2-d6f89ce1d44a",
							Topics: []string{"my-existing-topic-b"},
						},
						{
							Uid:              SinkUUID,
							Topics:           []string{SinkTopic()},
							Ingress:          &contract.Ingress{ContentMode: contract.ContentMode_STRUCTURED, IngressType: &contract.Ingress_Path{Path: receiver.Path(SinkNamespace, SinkName)}},
							BootstrapServers: bootstrapServers,
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
			WantUpdates: []clientgotesting.UpdateActionImpl{},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSink(
						SinkControllerOwnsTopic,
						InitSinkConditions,
						SinkDataPlaneAvailable,
						SinkConfigParsed,
						SinkProbeSucceeded,
						BootstrapServers(bootstrapServersArr),
						SinkConfigMapUpdatedReady(&configs.Env),
						SinkTopicReady,
						SinkTopicReadyWithOwner(SinkTopic(), sink.ControllerTopicOwner),
						SinkAddressable(&configs.Env),
					),
				},
			},
		},
		{
			Name: "Reconciled failed - probe receivers failed",
			Objects: []runtime.Object{
				NewSink(
					SinkControllerOwnsTopic,
					BootstrapServers(bootstrapServersArr),
				),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:     "5384faa4-6bdf-428d-b6c2-d6f89ce1d44b",
							Topics:  []string{"my-existing-topic-a"},
							Ingress: &contract.Ingress{IngressType: &contract.Ingress_Path{Path: receiver.Path(SinkNamespace, SinkName)}},
						},
						{
							Uid:    "5384faa4-6bdf-428d-b6c2-d6f89ce1d44a",
							Topics: []string{"my-existing-topic-b"},
						},
						{
							Uid:              SinkUUID,
							Topics:           []string{SinkTopic()},
							Ingress:          &contract.Ingress{ContentMode: contract.ContentMode_STRUCTURED, IngressType: &contract.Ingress_Path{Path: receiver.Path(SinkNamespace, SinkName)}},
							BootstrapServers: bootstrapServers,
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
			WantUpdates: []clientgotesting.UpdateActionImpl{},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSink(
						SinkControllerOwnsTopic,
						InitSinkConditions,
						SinkDataPlaneAvailable,
						SinkConfigParsed,
						SinkProbeFailed(http.StatusBadGateway),
						BootstrapServers(bootstrapServersArr),
						SinkConfigMapUpdatedReady(&configs.Env),
						SinkTopicReady,
						SinkTopicReadyWithOwner(SinkTopic(), sink.ControllerTopicOwner),
					),
				},
			},
			OtherTestData: map[string]interface{}{
				wantErrorOnProbeReceivers: http.StatusBadGateway,
			},
		},
	}

	for i := range table {
		table[i].Name = table[i].Name + " - " + format
	}

	useTable(t, table, &configs)
}

func TestSinkFinalizer(t *testing.T) {

	v1alpha1.RegisterConditionSet(base.ConditionSet)

	t.Parallel()

	for _, f := range Formats {
		sinkFinalization(t, f, *DefaultConfigs)
	}
}

func sinkFinalization(t *testing.T, format string, configs broker.Configs) {

	testKey := fmt.Sprintf("%s/%s", SinkNamespace, SinkName)

	configs.DataPlaneConfigFormat = format

	table := TableTest{
		{
			Name: "Reconciled normal - topic externally controlled",
			Objects: []runtime.Object{
				NewDeletedSink(
					SinkControllerDontOwnTopic,
				),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:    SinkUUID + "a",
							Topics: []string{"topic"},
							Ingress: &contract.Ingress{
								IngressType: &contract.Ingress_Path{

									Path: "path",
								},
								ContentMode: contract.ContentMode_STRUCTURED,
							},
							BootstrapServers: bootstrapServers,
						},
						{
							Uid:    SinkUUID,
							Topics: []string{"topic"},
							Ingress: &contract.Ingress{
								IngressType: &contract.Ingress_Path{
									Path: "path",
								},
								ContentMode: contract.ContentMode_STRUCTURED,
							},
							BootstrapServers: bootstrapServers,
						},
					},
					Generation: 1,
				}, &configs),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:    SinkUUID + "a",
							Topics: []string{"topic"},
							Ingress: &contract.Ingress{
								IngressType: &contract.Ingress_Path{
									Path: "path",
								},
								ContentMode: contract.ContentMode_STRUCTURED,
							},
							BootstrapServers: bootstrapServers,
						},
					},
					Generation: 2,
				}),
			},
		},
		{
			Name: "Reconciled normal - topic controlled by us",
			Objects: []runtime.Object{
				NewDeletedSink(
					SinkControllerOwnsTopic,
				),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:    SinkUUID + "a",
							Topics: []string{"topic"},
							Ingress: &contract.Ingress{
								IngressType: &contract.Ingress_Path{
									Path: "path",
								},
								ContentMode: contract.ContentMode_STRUCTURED,
							},
							BootstrapServers: bootstrapServers,
						},
						{
							Uid:    SinkUUID,
							Topics: []string{"topic-2"},
							Ingress: &contract.Ingress{
								IngressType: &contract.Ingress_Path{
									Path: "path",
								},
								ContentMode: contract.ContentMode_STRUCTURED,
							},
							BootstrapServers: bootstrapServers,
						},
					},
					Generation: 1,
				}, &configs),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:    SinkUUID + "a",
							Topics: []string{"topic"},
							Ingress: &contract.Ingress{
								IngressType: &contract.Ingress_Path{
									Path: "path",
								},
								ContentMode: contract.ContentMode_STRUCTURED,
							},
							BootstrapServers: bootstrapServers,
						},
					},
					Generation: 2,
				}),
			},
		},
		{
			Name: "Reconciled normal - topic controlled by us - error deleting topic",
			Objects: []runtime.Object{
				NewDeletedSink(
					SinkControllerOwnsTopic,
					func(sink *v1alpha1.KafkaSink) {
						sink.Spec.Topic = "topic-2"
					},
				),
				NewConfigMapFromContract(&contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:    SinkUUID + "a",
							Topics: []string{"topic"},
							Ingress: &contract.Ingress{
								IngressType: &contract.Ingress_Path{
									Path: "path",
								},
								ContentMode: contract.ContentMode_STRUCTURED,
							},
							BootstrapServers: bootstrapServers,
						},
						{
							Uid:    SinkUUID,
							Topics: []string{"topic-2"},
							Ingress: &contract.Ingress{
								IngressType: &contract.Ingress_Path{
									Path: "path",
								},
								ContentMode: contract.ContentMode_STRUCTURED,
							},
							BootstrapServers: bootstrapServers,
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
					"topic-2", deleteTopicError,
				),
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&configs, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:    SinkUUID + "a",
							Topics: []string{"topic"},
							Ingress: &contract.Ingress{
								IngressType: &contract.Ingress_Path{
									Path: "path",
								},
								ContentMode: contract.ContentMode_STRUCTURED,
							},
							BootstrapServers: bootstrapServers,
						},
					},
					Generation: 2,
				}),
			},
			OtherTestData: map[string]interface{}{
				wantErrorOnDeleteTopic: deleteTopicError,
				wantTopicName:          "topic-2",
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

		expectedTopicsOnDescribeTopics := []string{SinkTopic()}
		if et, ok := row.OtherTestData[ExpectedTopicsOnDescribeTopics]; ok {
			expectedTopicsOnDescribeTopics = et.([]string)
		}
		expectedTopicIsPresent := true
		if isPresent, ok := row.OtherTestData[ExpectedTopicIsPresent]; ok {
			expectedTopicIsPresent = isPresent.(bool)
		}

		var metadata []*sarama.TopicMetadata
		if expectedTopicIsPresent {
			for _, topic := range expectedTopicsOnDescribeTopics {
				metadata = append(metadata, &sarama.TopicMetadata{
					Name:       topic,
					IsInternal: false,
				})
			}
		}

		var errorOnDescribeTopics error
		if isPresentError, ok := row.OtherTestData[ExpectedErrorOnDescribeTopics]; ok {
			errorOnDescribeTopics = isPresentError.(error)
		}

		probeResponseStatusCode := http.StatusOK
		if s, ok := row.OtherTestData[wantErrorOnProbeReceivers]; ok {
			probeResponseStatusCode = s.(int)
		}

		reconciler := &sink.Reconciler{
			Reconciler: &base.Reconciler{
				KubeClient:                  kubeclient.Get(ctx),
				PodLister:                   listers.GetPodLister(),
				SecretLister:                listers.GetSecretLister(),
				DataPlaneConfigMapNamespace: configs.DataPlaneConfigMapNamespace,
				DataPlaneConfigMapName:      configs.DataPlaneConfigMapName,
				DataPlaneConfigFormat:       configs.DataPlaneConfigFormat,
				SystemNamespace:             configs.SystemNamespace,
				ReceiverLabel:               base.SinkReceiverLabel,
				RequestProbeDoer:            testinghttp.Do(probeResponseStatusCode),
			},
			ConfigMapLister: listers.GetConfigMapLister(),
			ClusterAdmin: func(addrs []string, config *sarama.Config) (sarama.ClusterAdmin, error) {
				return &kafkatesting.MockKafkaClusterAdmin{
					ExpectedTopicName:                      expectedTopicName,
					ExpectedTopicDetail:                    expectedTopicDetail,
					ErrorOnCreateTopic:                     onCreateTopicError,
					ErrorOnDeleteTopic:                     onDeleteTopicError,
					ExpectedTopics:                         expectedTopicsOnDescribeTopics,
					ExpectedTopicsMetadataOnDescribeTopics: metadata,
					ExpectedErrorOnDescribeTopics:          errorOnDescribeTopics,
					T:                                      t,
				}, nil
			},
			Configs: &configs.Env,
		}

		reconciler.EnqueueAfter = func(ks *v1alpha1.KafkaSink, duration time.Duration) {}

		reconciler.SecretTracker = &FakeTracker{}

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
