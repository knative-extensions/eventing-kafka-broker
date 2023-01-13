/*
Copyright 2019 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package resources

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	duckv1 "knative.dev/pkg/apis/duck/v1"

	kafkabindingv1beta1 "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/bindings/v1beta1"
	kafkasourcev1beta1 "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/sources/v1beta1"
)

type KafkaSourceV1Beta1Option func(source *kafkasourcev1beta1.KafkaSource)

func WithNameV1Beta1(name string) KafkaSourceV1Beta1Option {
	return func(source *kafkasourcev1beta1.KafkaSource) {
		source.Name = name
	}
}

func WithConsumerGroupV1Beta1(cg string) KafkaSourceV1Beta1Option {
	return func(source *kafkasourcev1beta1.KafkaSource) {
		source.Spec.ConsumerGroup = cg
	}
}

func WithExtensionsV1Beta1(extensions map[string]string) KafkaSourceV1Beta1Option {
	return func(source *kafkasourcev1beta1.KafkaSource) {
		source.Spec.CloudEventOverrides = &duckv1.CloudEventOverrides{
			Extensions: extensions,
		}
	}
}

func KafkaSourceV1Beta1(bootstrapServer string, topicName string, ref *corev1.ObjectReference, options ...KafkaSourceV1Beta1Option) *kafkasourcev1beta1.KafkaSource {
	source := &kafkasourcev1beta1.KafkaSource{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-kafka-source",
		},
		Spec: kafkasourcev1beta1.KafkaSourceSpec{
			KafkaAuthSpec: kafkabindingv1beta1.KafkaAuthSpec{
				BootstrapServers: []string{bootstrapServer},
			},
			Topics:        []string{topicName},
			ConsumerGroup: "test-consumer-group",
			SourceSpec: duckv1.SourceSpec{
				Sink: duckv1.Destination{
					Ref: &duckv1.KReference{
						APIVersion: ref.APIVersion,
						Kind:       ref.Kind,
						Name:       ref.Name,
						Namespace:  ref.Namespace,
					},
				},
			},
		},
	}

	for _, opt := range options {
		opt(source)
	}

	return source
}
