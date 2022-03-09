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
	"testing"

	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/network"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
	kafkatesting "knative.dev/eventing-kafka-broker/control-plane/pkg/kafka/testing"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/prober"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/prober/probertesting"

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
	"knative.dev/eventing-kafka-broker/control-plane/pkg/receiver"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/sink"
	reconciler "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/sink"
	. "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/testing"
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
	testProber                     = "testProber"

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

var DefaultEnv = &config.Env{
	DataPlaneConfigMapNamespace: "knative-eventing",
	DataPlaneConfigMapName:      "kafka-sink-sinks",
	GeneralConfigMapName:        "kafka-broker-config",
	IngressName:                 "kafka-sink-ingress",
	SystemNamespace:             "knative-eventing",
	DataPlaneConfigFormat:       base.Json,
}

func TestSinkReconciler(t *testing.T) {

	v1alpha1.RegisterConditionSet(base.IngressConditionSet)

	t.Parallel()

	for _, f := range Formats {
		sinkReconciliation(t, f, *DefaultEnv)
	}
}

func sinkReconciliation(t *testing.T, format string, env config.Env) {

	testKey := fmt.Sprintf("%s/%s", SinkNamespace, SinkName)

	env.DataPlaneConfigFormat = format

	table := TableTest{
		{
			Name: "Reconciled normal",
			Objects: []runtime.Object{
				NewSink(
					StatusControllerOwnsTopic(reconciler.ControllerTopicOwner),
				),
				NewConfigMapWithBinaryData(&env, nil),
				SinkReceiverPod(env.SystemNamespace, map[string]string{
					"annotation_to_preserve": "value_to_preserve",
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
							Uid:              SinkUUID,
							Topics:           []string{SinkTopic()},
							Ingress:          &contract.Ingress{ContentMode: contract.ContentMode_STRUCTURED, IngressType: &contract.Ingress_Path{Path: receiver.Path(SinkNamespace, SinkName)}},
							BootstrapServers: bootstrapServers,
							Reference:        SinkReference(),
						},
					},
					Generation: 1,
				}),
				SinkReceiverPodUpdate(env.SystemNamespace, map[string]string{
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
						StatusControllerOwnsTopic(reconciler.ControllerTopicOwner),
						InitSinkConditions,
						StatusDataPlaneAvailable,
						StatusConfigParsed,
						BootstrapServers(bootstrapServersArr),
						StatusConfigMapUpdatedReady(&env),
						StatusTopicReadyWithOwner(SinkTopic(), sink.ControllerTopicOwner),
						SinkAddressable(&env),
						StatusProbeSucceeded,
					),
				},
			},
		},
		{
			Name: "Reconciled normal - with auth config",
			Objects: []runtime.Object{
				NewSink(
					StatusControllerOwnsTopic(reconciler.ControllerTopicOwner),
					SinkAuthSecretRef("secret-1"),
				),
				NewSSLSecret(SinkNamespace, "secret-1"),
				NewConfigMapWithBinaryData(&env, nil),
				SinkReceiverPod(env.SystemNamespace, map[string]string{
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
							Uid:              SinkUUID,
							Topics:           []string{SinkTopic()},
							BootstrapServers: bootstrapServers,
							Reference:        SinkReference(),
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
				SinkReceiverPodUpdate(env.SystemNamespace, map[string]string{
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
						StatusControllerOwnsTopic(reconciler.ControllerTopicOwner),
						SinkAuthSecretRef("secret-1"),
						InitSinkConditions,
						StatusDataPlaneAvailable,
						StatusConfigParsed,
						BootstrapServers(bootstrapServersArr),
						StatusConfigMapUpdatedReady(&env),
						StatusTopicReadyWithOwner(SinkTopic(), sink.ControllerTopicOwner),
						SinkAddressable(&env),
						StatusProbeSucceeded,
					),
				},
			},
		},
		{
			Name: "Reconciled normal - no topic owner",
			Objects: []runtime.Object{
				NewSink(
					StatusControllerOwnsTopic(reconciler.ExternalTopicOwner),
					func(obj duckv1.KRShaped) {
						s := obj.(*v1alpha1.KafkaSink)
						s.Spec.ReplicationFactor = nil
						s.Spec.NumPartitions = nil
					},
				),
				NewConfigMapWithBinaryData(&env, nil),
				SinkReceiverPod(env.SystemNamespace, map[string]string{
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
							Uid:              SinkUUID,
							Topics:           []string{SinkTopic()},
							BootstrapServers: bootstrapServers,
							Reference:        SinkReference(),
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
				SinkReceiverPodUpdate(env.SystemNamespace, map[string]string{
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
						StatusControllerOwnsTopic(reconciler.ExternalTopicOwner),
						func(obj duckv1.KRShaped) {
							s := obj.(*v1alpha1.KafkaSink)
							s.Spec.ReplicationFactor = nil
							s.Spec.NumPartitions = nil
						},
						InitSinkConditions,
						StatusDataPlaneAvailable,
						StatusConfigParsed,
						BootstrapServers(bootstrapServersArr),
						StatusConfigMapUpdatedReady(&env),
						StatusTopicReadyWithOwner(SinkTopic(), sink.ExternalTopicOwner),
						SinkAddressable(&env),
						StatusProbeSucceeded,
					),
				},
			},
		},
		{
			Name: "No topic owner - topic present err",
			Objects: []runtime.Object{
				NewSink(
					StatusControllerOwnsTopic(reconciler.ExternalTopicOwner),
					func(obj duckv1.KRShaped) {
						s := obj.(*v1alpha1.KafkaSink)
						s.Spec.ReplicationFactor = nil
						s.Spec.NumPartitions = nil
					},
				),
				NewConfigMapWithBinaryData(&env, nil),
				SinkReceiverPod(env.SystemNamespace, map[string]string{
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
					"topics %v not present or invalid: "+SinkNotPresentErrFormat,
					[]string{SinkTopic()}, []string{SinkTopic()}, io.EOF,
				),
			},
			WantErr: true,
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSink(
						StatusControllerOwnsTopic(reconciler.ExternalTopicOwner),
						func(obj duckv1.KRShaped) {
							s := obj.(*v1alpha1.KafkaSink)
							s.Spec.ReplicationFactor = nil
							s.Spec.NumPartitions = nil
						},
						InitSinkConditions,
						StatusDataPlaneAvailable,
						StatusTopicNotPresentErr(SinkTopic(), io.EOF),
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
					StatusControllerOwnsTopic(reconciler.ControllerTopicOwner),
					func(obj duckv1.KRShaped) {
						s := obj.(*v1alpha1.KafkaSink)
						s.Spec.Topic = "my-topic-1"
						s.Spec.BootstrapServers = []string{"kafka-broker:10000"}
					},
				),
				NewConfigMapWithBinaryData(&env, nil),
				SinkReceiverPod(env.SystemNamespace, map[string]string{
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
							Uid:              SinkUUID,
							Topics:           []string{"my-topic-1"},
							Ingress:          &contract.Ingress{ContentMode: contract.ContentMode_STRUCTURED, IngressType: &contract.Ingress_Path{Path: receiver.Path(SinkNamespace, SinkName)}},
							BootstrapServers: "kafka-broker:10000",
							Reference:        SinkReference(),
						},
					},
					Generation: 1,
				}),
				SinkReceiverPodUpdate(env.SystemNamespace, map[string]string{
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
						StatusControllerOwnsTopic(reconciler.ControllerTopicOwner),
						func(obj duckv1.KRShaped) {
							s := obj.(*v1alpha1.KafkaSink)
							s.Spec.Topic = "my-topic-1"
							s.Spec.BootstrapServers = []string{"kafka-broker:10000"}
						},
						InitSinkConditions,
						StatusDataPlaneAvailable,
						StatusConfigParsed,
						BootstrapServers([]string{"kafka-broker:10000"}),
						StatusTopicReadyWithOwner("my-topic-1", sink.ControllerTopicOwner),
						StatusConfigMapUpdatedReady(&env),
						SinkAddressable(&env),
						StatusProbeSucceeded,
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
					StatusControllerOwnsTopic(reconciler.ControllerTopicOwner),
					BootstrapServers(bootstrapServersArr),
				),
				SinkReceiverPod(env.SystemNamespace, nil),
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
						StatusControllerOwnsTopic(reconciler.ControllerTopicOwner),
						InitSinkConditions,
						StatusDataPlaneAvailable,
						BootstrapServers(bootstrapServersArr),
						StatusFailedToCreateTopic(SinkTopic()),
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
					StatusControllerOwnsTopic(reconciler.ControllerTopicOwner),
					BootstrapServers(bootstrapServersArr),
				),
				NewService(),
				SinkReceiverPod(env.SystemNamespace, map[string]string{base.VolumeGenerationAnnotationKey: "2"}),
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
							Uid:              SinkUUID,
							Topics:           []string{SinkTopic()},
							Ingress:          &contract.Ingress{ContentMode: contract.ContentMode_STRUCTURED, IngressType: &contract.Ingress_Path{Path: receiver.Path(SinkNamespace, SinkName)}},
							BootstrapServers: bootstrapServers,
							Reference:        SinkReference(),
						},
					},
					Generation: 1,
				}),
				SinkReceiverPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSink(
						StatusControllerOwnsTopic(reconciler.ControllerTopicOwner),
						InitSinkConditions,
						StatusDataPlaneAvailable,
						StatusConfigParsed,
						BootstrapServers(bootstrapServersArr),
						StatusConfigMapUpdatedReady(&env),
						StatusTopicReadyWithOwner(SinkTopic(), sink.ControllerTopicOwner),
						SinkAddressable(&env),
						StatusProbeSucceeded,
					),
				},
			},
		},
		{
			Name: "Reconciled normal - config map not readable",
			Objects: []runtime.Object{
				NewSink(
					StatusControllerOwnsTopic(reconciler.ControllerTopicOwner),
					BootstrapServers(bootstrapServersArr),
				),
				NewConfigMapWithBinaryData(&env, []byte(`{"hello": "world"}`)),
				SinkReceiverPod(env.SystemNamespace, nil),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid:              SinkUUID,
							Topics:           []string{SinkTopic()},
							Ingress:          &contract.Ingress{ContentMode: contract.ContentMode_STRUCTURED, IngressType: &contract.Ingress_Path{Path: receiver.Path(SinkNamespace, SinkName)}},
							BootstrapServers: bootstrapServers,
							Reference:        SinkReference(),
						},
					},
					Generation: 1,
				}),
				SinkReceiverPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSink(
						StatusControllerOwnsTopic(reconciler.ControllerTopicOwner),
						InitSinkConditions,
						StatusDataPlaneAvailable,
						StatusConfigParsed,
						BootstrapServers(bootstrapServersArr),
						StatusConfigMapUpdatedReady(&env),
						StatusTopicReadyWithOwner(SinkTopic(), sink.ControllerTopicOwner),
						SinkAddressable(&env),
						StatusProbeSucceeded,
					),
				},
			},
		},
		{
			Name: "Reconciled normal - preserve config map previous state",
			Objects: []runtime.Object{
				NewSink(
					StatusControllerOwnsTopic(reconciler.ControllerTopicOwner),
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
				}, &env),
				SinkReceiverPod(env.SystemNamespace, map[string]string{
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
							Reference:        SinkReference(),
						},
					},
					Generation: 1,
				}),
				SinkReceiverPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSink(
						StatusControllerOwnsTopic(reconciler.ControllerTopicOwner),
						InitSinkConditions,
						StatusDataPlaneAvailable,
						StatusConfigParsed,
						BootstrapServers(bootstrapServersArr),
						StatusConfigMapUpdatedReady(&env),
						StatusTopicReadyWithOwner(SinkTopic(), sink.ControllerTopicOwner),
						SinkAddressable(&env),
						StatusProbeSucceeded,
					),
				},
			},
		},
		{
			Name: "Reconciled normal - update existing broker while preserving others",
			Objects: []runtime.Object{
				NewSink(
					StatusControllerOwnsTopic(reconciler.ControllerTopicOwner),
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
							Reference:        SinkReference(),
							Ingress:          &contract.Ingress{ContentMode: contract.ContentMode_STRUCTURED},
						},
					},
				}, &env),
				SinkReceiverPod(env.SystemNamespace, map[string]string{
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
							Uid:              SinkUUID,
							Topics:           []string{SinkTopic()},
							Ingress:          &contract.Ingress{ContentMode: contract.ContentMode_STRUCTURED, IngressType: &contract.Ingress_Path{Path: receiver.Path(SinkNamespace, SinkName)}},
							BootstrapServers: bootstrapServers,
							Reference:        SinkReference(),
						},
					},
					Generation: 1,
				}),
				SinkReceiverPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSink(
						StatusControllerOwnsTopic(reconciler.ControllerTopicOwner),
						InitSinkConditions,
						StatusDataPlaneAvailable,
						StatusConfigParsed,
						BootstrapServers(bootstrapServersArr),
						StatusConfigMapUpdatedReady(&env),
						StatusTopicReadyWithOwner(SinkTopic(), sink.ControllerTopicOwner),
						SinkAddressable(&env),
						StatusProbeSucceeded,
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
						StatusDataPlaneNotAvailable,
					),
				},
			},
		},
		{
			Name: "Reconciled normal - unchanged",
			Objects: []runtime.Object{
				NewSink(
					StatusControllerOwnsTopic(reconciler.ControllerTopicOwner),
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
							Reference:        SinkReference(),
						},
					},
					Generation: 1,
				}, &env),
				SinkReceiverPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
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
					Object: NewSink(
						StatusControllerOwnsTopic(reconciler.ControllerTopicOwner),
						InitSinkConditions,
						StatusDataPlaneAvailable,
						StatusConfigParsed,
						BootstrapServers(bootstrapServersArr),
						StatusConfigMapUpdatedReady(&env),
						StatusTopicReadyWithOwner(SinkTopic(), sink.ControllerTopicOwner),
						SinkAddressable(&env),
						StatusProbeSucceeded,
					),
				},
			},
		},
		{
			Name: "Reconciled normal - unchanged contract - changed receiver pod annotation",
			Objects: []runtime.Object{
				NewSink(
					StatusControllerOwnsTopic(reconciler.ControllerTopicOwner),
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
							Reference:        SinkReference(),
						},
					},
					Generation: 1,
				}, &env),
				SinkReceiverPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
				}),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			WantUpdates: []clientgotesting.UpdateActionImpl{
				SinkReceiverPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewSink(
						StatusControllerOwnsTopic(reconciler.ControllerTopicOwner),
						InitSinkConditions,
						StatusDataPlaneAvailable,
						StatusConfigParsed,
						BootstrapServers(bootstrapServersArr),
						StatusConfigMapUpdatedReady(&env),
						StatusTopicReadyWithOwner(SinkTopic(), sink.ControllerTopicOwner),
						SinkAddressable(&env),
						StatusProbeSucceeded,
					),
				},
			},
		},
		{
			Name: "Reconciled failed - probe " + prober.StatusNotReady.String(),
			Objects: []runtime.Object{
				NewSink(
					StatusControllerOwnsTopic(reconciler.ControllerTopicOwner),
				),
				NewConfigMapWithBinaryData(&env, nil),
				SinkReceiverPod(env.SystemNamespace, map[string]string{
					"annotation_to_preserve": "value_to_preserve",
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
							Uid:              SinkUUID,
							Topics:           []string{SinkTopic()},
							Ingress:          &contract.Ingress{ContentMode: contract.ContentMode_STRUCTURED, IngressType: &contract.Ingress_Path{Path: receiver.Path(SinkNamespace, SinkName)}},
							BootstrapServers: bootstrapServers,
							Reference:        SinkReference(),
						},
					},
					Generation: 1,
				}),
				SinkReceiverPodUpdate(env.SystemNamespace, map[string]string{
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
						StatusControllerOwnsTopic(reconciler.ControllerTopicOwner),
						InitSinkConditions,
						StatusDataPlaneAvailable,
						StatusConfigParsed,
						BootstrapServers(bootstrapServersArr),
						StatusConfigMapUpdatedReady(&env),
						StatusTopicReadyWithOwner(SinkTopic(), sink.ControllerTopicOwner),
						StatusProbeFailed(prober.StatusNotReady),
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
				NewSink(
					StatusControllerOwnsTopic(reconciler.ControllerTopicOwner),
				),
				NewConfigMapWithBinaryData(&env, nil),
				SinkReceiverPod(env.SystemNamespace, map[string]string{
					"annotation_to_preserve": "value_to_preserve",
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
							Uid:              SinkUUID,
							Topics:           []string{SinkTopic()},
							Ingress:          &contract.Ingress{ContentMode: contract.ContentMode_STRUCTURED, IngressType: &contract.Ingress_Path{Path: receiver.Path(SinkNamespace, SinkName)}},
							BootstrapServers: bootstrapServers,
							Reference:        SinkReference(),
						},
					},
					Generation: 1,
				}),
				SinkReceiverPodUpdate(env.SystemNamespace, map[string]string{
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
						StatusControllerOwnsTopic(reconciler.ControllerTopicOwner),
						InitSinkConditions,
						StatusDataPlaneAvailable,
						StatusConfigParsed,
						BootstrapServers(bootstrapServersArr),
						StatusConfigMapUpdatedReady(&env),
						StatusTopicReadyWithOwner(SinkTopic(), sink.ControllerTopicOwner),
						StatusProbeFailed(prober.StatusUnknown),
					),
				},
			},
			OtherTestData: map[string]interface{}{
				testProber: probertesting.MockProber(prober.StatusUnknown),
			},
		},
	}

	for i := range table {
		table[i].Name = table[i].Name + " - " + format
	}

	useTable(t, table, &env)
}

func TestSinkFinalizer(t *testing.T) {

	v1alpha1.RegisterConditionSet(base.IngressConditionSet)

	t.Parallel()

	for _, f := range Formats {
		sinkFinalization(t, f, *DefaultEnv)
	}
}

func sinkFinalization(t *testing.T, format string, env config.Env) {

	testKey := fmt.Sprintf("%s/%s", SinkNamespace, SinkName)

	env.DataPlaneConfigFormat = format

	table := TableTest{
		{
			Name: "Reconciled normal - topic externally controlled",
			Objects: []runtime.Object{
				NewDeletedSink(
					StatusControllerOwnsTopic(reconciler.ExternalTopicOwner),
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
				}, &env),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
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
				testProber: probertesting.MockProber(prober.StatusNotReady),
			},
		},
		{
			Name: "Reconciled normal - topic controlled by us",
			Objects: []runtime.Object{
				NewDeletedSink(
					StatusControllerOwnsTopic(reconciler.ControllerTopicOwner),
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
				}, &env),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
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
				testProber: probertesting.MockProber(prober.StatusNotReady),
			},
		},
		{
			Name: "Reconciled normal, probe not ready",
			Objects: []runtime.Object{
				NewDeletedSink(
					StatusControllerOwnsTopic(reconciler.ControllerTopicOwner),
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
				}, &env),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
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
				testProber: probertesting.MockProber(prober.StatusNotReady),
			},
		},
		{
			Name: "Reconciled failed, probe ready",
			Objects: []runtime.Object{
				NewDeletedSink(
					StatusControllerOwnsTopic(reconciler.ControllerTopicOwner),
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
				}, &env),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
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
			WantErr: true,
			OtherTestData: map[string]interface{}{
				testProber: probertesting.MockProber(prober.StatusReady),
			},
		},
		{
			Name: "Reconciled normal - topic controlled by us - error deleting topic",
			Objects: []runtime.Object{
				NewDeletedSink(
					StatusControllerOwnsTopic(reconciler.ControllerTopicOwner),
					func(obj duckv1.KRShaped) {
						s := obj.(*v1alpha1.KafkaSink)
						s.Spec.Topic = "topic-2"
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
				}, &env),
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
				ConfigMapUpdate(&env, &contract.Contract{
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
				testProber:             probertesting.MockProber(prober.StatusNotReady),
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
					Partitions: []*sarama.PartitionMetadata{{}},
				})
			}
		}

		var errorOnDescribeTopics error
		if isPresentError, ok := row.OtherTestData[ExpectedErrorOnDescribeTopics]; ok {
			errorOnDescribeTopics = isPresentError.(error)
		}

		proberMock := probertesting.MockProber(prober.StatusReady)
		if p, ok := row.OtherTestData[testProber]; ok {
			proberMock = p.(prober.Prober)
		}

		reconciler := &sink.Reconciler{
			Reconciler: &base.Reconciler{
				KubeClient:                  kubeclient.Get(ctx),
				PodLister:                   listers.GetPodLister(),
				SecretLister:                listers.GetSecretLister(),
				DataPlaneConfigMapNamespace: env.DataPlaneConfigMapNamespace,
				DataPlaneConfigMapName:      env.DataPlaneConfigMapName,
				DataPlaneConfigFormat:       env.DataPlaneConfigFormat,
				SystemNamespace:             env.SystemNamespace,
				ReceiverLabel:               base.SinkReceiverLabel,
			},
			ConfigMapLister: listers.GetConfigMapLister(),
			NewKafkaClusterAdminClient: func(_ []string, _ *sarama.Config) (sarama.ClusterAdmin, error) {
				return &kafkatesting.MockKafkaClusterAdmin{
					ExpectedTopicName:                      expectedTopicName,
					ExpectedTopicDetail:                    expectedTopicDetail,
					ErrorOnCreateTopic:                     onCreateTopicError,
					ErrorOnDeleteTopic:                     onDeleteTopicError,
					ExpectedTopics:                         expectedTopicsOnDescribeTopics,
					ExpectedErrorOnDescribeTopics:          errorOnDescribeTopics,
					ExpectedTopicsMetadataOnDescribeTopics: metadata,
					T:                                      t,
				}, nil
			},
			Env:         env,
			Prober:      proberMock,
			IngressHost: network.GetServiceHostname(env.IngressName, env.SystemNamespace),
		}

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
