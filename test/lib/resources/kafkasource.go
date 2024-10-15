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

	bindings "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/bindings/v1"
	sources "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/sources/v1"
)

type KafkaSourceOption func(source *sources.KafkaSource)

func WithName(name string) KafkaSourceOption {
	return func(source *sources.KafkaSource) {
		source.Name = name
	}
}

func WithConsumerGroup(cg string) KafkaSourceOption {
	return func(source *sources.KafkaSource) {
		source.Spec.ConsumerGroup = cg
	}
}

func WithExtensions(extensions map[string]string) KafkaSourceOption {
	return func(source *sources.KafkaSource) {
		source.Spec.CloudEventOverrides = &duckv1.CloudEventOverrides{
			Extensions: extensions,
		}
	}
}

func KafkaSource(bootstrapServer string, topicName string, ordering sources.DeliveryOrdering, ref *corev1.ObjectReference, options ...KafkaSourceOption) *sources.KafkaSource {
	source := &sources.KafkaSource{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-kafka-source",
		},
		Spec: sources.KafkaSourceSpec{
			KafkaAuthSpec: bindings.KafkaAuthSpec{
				BootstrapServers: []string{bootstrapServer},
			},
			Topics:        []string{topicName},
			ConsumerGroup: "test-consumer-group",
			Ordering:      &ordering,
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
