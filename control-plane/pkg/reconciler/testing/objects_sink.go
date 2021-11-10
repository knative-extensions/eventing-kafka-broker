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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgotesting "k8s.io/client-go/testing"
	"k8s.io/utils/pointer"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/network"

	eventing "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/eventing/v1alpha1"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
)

const (
	SinkUUID      = "e7185016-5d98-4b54-84e8-3b1cd4acc6b5"
	SinkNamespace = "sink-namespace"
	SinkName      = "sink-name"

	SinkNumPartitions     = 10
	SinkReplicationFactor = 3

	SinkNotPresentErrFormat = "failed to describe topics %v: %v"
)

var (
	bootstrapServers = []string{"kafka-1:9092", "kafka-2:9093"}
)

func NewSink(options ...KRShapedOption) runtime.Object {
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

func NewDeletedSink(options ...KRShapedOption) runtime.Object {
	return NewSink(
		append(
			options,
			WithDeletedTimeStamp,
		)...,
	)
}

func SinkTopic() string {
	return fmt.Sprintf("knative-sink-%s-%s", SinkNamespace, SinkName)
}

func BootstrapServers(bootstrapServers []string) func(obj duckv1.KRShaped) {
	return func(obj duckv1.KRShaped) {
		sink := obj.(*eventing.KafkaSink)
		sink.Spec.BootstrapServers = bootstrapServers
	}
}

func InitSinkConditions(obj duckv1.KRShaped) {
	sink := obj.(*eventing.KafkaSink)
	sink.Status.InitializeConditions()
}

func SinkAuthSecretRef(name string) func(obj duckv1.KRShaped) {
	return func(obj duckv1.KRShaped) {
		sink := obj.(*eventing.KafkaSink)
		sink.Spec.Auth = &eventing.Auth{
			Secret: &eventing.Secret{
				Ref: &eventing.SecretReference{
					Name: name,
				},
			},
		}
	}
}

func SinkAddressable(configs *config.Env) func(obj duckv1.KRShaped) {

	return func(obj duckv1.KRShaped) {
		sink := obj.(*eventing.KafkaSink)
		sink.Status.Address.URL = &apis.URL{
			Scheme: "http",
			Host:   network.GetServiceHostname(configs.IngressName, configs.SystemNamespace),
			Path:   fmt.Sprintf("/%s/%s", sink.Namespace, sink.Name),
		}

		sink.GetConditionSet().Manage(sink.GetStatus()).MarkTrue(base.ConditionAddressable)
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
