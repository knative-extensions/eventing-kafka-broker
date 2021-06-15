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

package testing

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgotesting "k8s.io/client-go/testing"
	"k8s.io/utils/pointer"
	"knative.dev/pkg/apis"
	"knative.dev/pkg/network"

	eventing "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/eventing/v1alpha1"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka"
	sinkreconciler "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/sink"
)

const (
	SinkUUID      = "e7185016-5d98-4b54-84e8-3b1cd4acc6b5"
	SinkNamespace = "sink-namespace"
	SinkName      = "sink-name"

	SinkNumPartitions     = 10
	SinkReplicationFactor = 3

	SinkNotPresentErrFormat = "failed to describe topic %s: %v"

	topicPrefix = "knative-sink-"
)

var (
	bootstrapServers = []string{"kafka-1:9092", "kafka-2:9093"}
)

type SinkOption func(sink *eventing.KafkaSink)

func NewSink(options ...SinkOption) runtime.Object {
	sink := &eventing.KafkaSink{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: SinkNamespace,
			Name:      SinkName,
			UID:       SinkUUID,
		},
		Spec: eventing.KafkaSinkSpec{
			Topic:             SinkTopic(),
			NumPartitions:     pointer.Int32Ptr(SinkNumPartitions),
			ReplicationFactor: func(rf int16) *int16 { return &rf }(SinkReplicationFactor),
			BootstrapServers:  bootstrapServers,
		},
	}

	for _, opt := range options {
		opt(sink)
	}

	sink.SetDefaults(context.Background())

	return sink
}

func NewDeletedSink(options ...SinkOption) runtime.Object {
	return NewSink(
		append(
			options,
			func(sink *eventing.KafkaSink) {
				sink.DeletionTimestamp = &metav1.Time{Time: time.Now()}
			},
		)...,
	)
}

func SinkTopic() string {
	return kafka.Topic(topicPrefix, &metav1.ObjectMeta{
		Name:      SinkName,
		Namespace: SinkNamespace,
	})
}

func BootstrapServers(bootstrapServers []string) func(sink *eventing.KafkaSink) {
	return func(sink *eventing.KafkaSink) {
		sink.Spec.BootstrapServers = bootstrapServers
	}
}

func InitSinkConditions(sink *eventing.KafkaSink) {
	sink.Status.InitializeConditions()
}

func SinkConfigMapUpdatedReady(configs *config.Env) func(sink *eventing.KafkaSink) {
	return func(sink *eventing.KafkaSink) {
		sink.GetConditionSet().Manage(sink.GetStatus()).MarkTrueWithReason(
			base.ConditionConfigMapUpdated,
			fmt.Sprintf("Config map %s updated", configs.DataPlaneConfigMapAsString()),
			"",
		)
	}
}

func SinkTopicReadyWithName(topic string) func(sink *eventing.KafkaSink) {
	return func(sink *eventing.KafkaSink) {
		sink.GetConditionSet().Manage(sink.GetStatus()).MarkTrueWithReason(
			base.ConditionTopicReady,
			fmt.Sprintf("Topic %s created", topic),
			"",
		)
	}
}

func SinkTopicReadyWithOwner(topic, owner string) func(sink *eventing.KafkaSink) {
	return func(sink *eventing.KafkaSink) {
		sink.GetConditionSet().Manage(sink.GetStatus()).MarkTrueWithReason(
			base.ConditionTopicReady,
			fmt.Sprintf("Topic %s (owner %s)", topic, owner),
			"",
		)
	}
}
func SinkTopicReady(sink *eventing.KafkaSink) {
	SinkTopicReadyWithName(SinkTopic())(sink)
}

func SinkConfigParsed(sink *eventing.KafkaSink) {
	sink.GetConditionSet().Manage(sink.GetStatus()).MarkTrue(base.ConditionConfigParsed)
}

func SinkProbeSucceeded(sink *eventing.KafkaSink) {
	sink.GetConditionSet().Manage(sink.GetStatus()).MarkTrue(base.ConditionProbeSucceeded)
}

func SinkProbeFailed(statusCode int) func(ks *eventing.KafkaSink) {
	return func(ks *eventing.KafkaSink) {
		ks.GetConditionSet().Manage(ks.GetStatus()).MarkFalse(
			base.ConditionProbeSucceeded,
			base.ReasonProbeFailed,
			fmt.Sprintf("probe failed response status code %d from pod knative-eventing/kafka-broker-receiver with ip %s", statusCode, ReceiverIP),
		)
	}
}

func SinkTopicNotPresentErr(topic string, err error) func(sink *eventing.KafkaSink) {
	return func(sink *eventing.KafkaSink) {
		sink.GetConditionSet().Manage(sink.GetStatus()).MarkFalse(
			base.ConditionTopicReady,
			base.ReasonTopicNotPresent,
			fmt.Sprintf(SinkNotPresentErrFormat, topic, err),
		)
	}
}

func SinkDataPlaneAvailable(sink *eventing.KafkaSink) {
	sink.GetConditionSet().Manage(sink.GetStatus()).MarkTrue(base.ConditionDataPlaneAvailable)
}

func SinkDataPlaneNotAvailable(sink *eventing.KafkaSink) {
	sink.GetConditionSet().Manage(sink.GetStatus()).MarkFalse(
		base.ConditionDataPlaneAvailable,
		base.ReasonDataPlaneNotAvailable,
		base.MessageDataPlaneNotAvailable,
	)
}

func SinkControllerOwnsTopic(sink *eventing.KafkaSink) {
	allocateStatusAnnotations(sink)
	sink.GetStatus().Annotations[base.TopicOwnerAnnotation] = sinkreconciler.ControllerTopicOwner
}

func SinkControllerDontOwnTopic(sink *eventing.KafkaSink) {
	allocateStatusAnnotations(sink)
	sink.GetStatus().Annotations[base.TopicOwnerAnnotation] = sinkreconciler.ExternalTopicOwner
}

func SinkAuthSecretRef(name string) func(sink *eventing.KafkaSink) {
	return func(sink *eventing.KafkaSink) {
		sink.Spec.Auth = &eventing.Auth{
			Secret: &eventing.Secret{
				Ref: &eventing.SecretReference{
					Name: name,
				},
			},
		}
	}
}

func allocateStatusAnnotations(sink *eventing.KafkaSink) {
	if sink.GetStatus().Annotations == nil {
		sink.GetStatus().Annotations = make(map[string]string, 1)
	}
}

func SinkAddressable(configs *config.Env) func(sink *eventing.KafkaSink) {

	return func(sink *eventing.KafkaSink) {

		sink.Status.Address.URL = &apis.URL{
			Scheme: "http",
			Host:   network.GetServiceHostname(configs.IngressName, configs.SystemNamespace),
			Path:   fmt.Sprintf("/%s/%s", sink.Namespace, sink.Name),
		}

		sink.GetConditionSet().Manage(sink.GetStatus()).MarkTrue(base.ConditionAddressable)
	}
}

func SinkFailedToCreateTopic(sink *eventing.KafkaSink) {

	sink.GetConditionSet().Manage(sink.GetStatus()).MarkFalse(
		base.ConditionTopicReady,
		fmt.Sprintf("Failed to create topic: %s", SinkTopic()),
		"%v",
		fmt.Errorf("failed to create topic"),
	)

}

func SinkFailedToGetConfigMap(configs *config.Env) func(sink *eventing.KafkaSink) {

	return func(sink *eventing.KafkaSink) {

		sink.GetConditionSet().Manage(sink.GetStatus()).MarkFalse(
			base.ConditionConfigMapUpdated,
			fmt.Sprintf(
				"Failed to get ConfigMap: %s",
				configs.DataPlaneConfigMapAsString(),
			),
			`configmaps "knative-eventing" not found`,
		)
	}

}

func SinkReceiverPod(namespace string, annotations map[string]string) runtime.Object {
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
				"app": base.SinkReceiverLabel,
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: ReceiverIP,
		},
	}
}

func SinkReceiverPodUpdate(namespace string, annotations map[string]string) clientgotesting.UpdateActionImpl {
	return clientgotesting.NewUpdateAction(
		schema.GroupVersionResource{
			Group:    "*",
			Version:  "v1",
			Resource: "Pod",
		},
		namespace,
		SinkReceiverPod(namespace, annotations),
	)
}
