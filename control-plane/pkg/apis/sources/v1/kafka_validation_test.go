/*
 * Copyright 2021 The Knative Authors
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

package v1

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	bindingsv1beta1 "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/bindings/v1beta1"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
)

func TestKafka_Validate(t *testing.T) {
	validOrdering := Ordered
	badOrdering := DeliveryOrdering("badOrder")
	badInitialOffset := Offset("badbOffset")

	tests := []struct {
		name string
		ks   *KafkaSource
		ctx  context.Context
		want *apis.FieldError
	}{
		{
			name: "no bootstrap servers",
			ks: &KafkaSource{
				Spec: KafkaSourceSpec{
					Topics:        []string{"test-topic"},
					KafkaAuthSpec: bindingsv1beta1.KafkaAuthSpec{},
					ConsumerGroup: "ks-group",
					SourceSpec: duckv1.SourceSpec{
						Sink: NewSourceSinkReference(),
					},
				},
			},
			ctx:  context.Background(),
			want: apis.ErrMissingField("spec.bootstrapServers"),
		},
		{
			name: "no topics",
			ks: &KafkaSource{
				Spec: KafkaSourceSpec{
					KafkaAuthSpec: bindingsv1beta1.KafkaAuthSpec{
						BootstrapServers: []string{"kafka:9092"},
					},
					ConsumerGroup: "ks-group",
					SourceSpec: duckv1.SourceSpec{
						Sink: NewSourceSinkReference(),
					},
				},
			},
			ctx:  context.Background(),
			want: apis.ErrMissingField("spec.topics"),
		},
		{
			name: "invalid ordering",
			ks: &KafkaSource{
				Spec: KafkaSourceSpec{
					Topics: []string{"test-topic"},
					KafkaAuthSpec: bindingsv1beta1.KafkaAuthSpec{
						BootstrapServers: []string{"kafka:9092"},
					},
					Ordering:      &badOrdering,
					ConsumerGroup: "ks-group",
					SourceSpec: duckv1.SourceSpec{
						Sink: NewSourceSinkReference(),
					},
				},
			},
			ctx:  context.Background(),
			want: apis.ErrInvalidValue(badOrdering, "spec.ordering"),
		},
		{
			name: "valid ordering",
			ks: &KafkaSource{
				Spec: KafkaSourceSpec{
					Topics: []string{"test-topic"},
					KafkaAuthSpec: bindingsv1beta1.KafkaAuthSpec{
						BootstrapServers: []string{"kafka:9092"},
					},
					Ordering:      &validOrdering,
					ConsumerGroup: "ks-group",
					SourceSpec: duckv1.SourceSpec{
						Sink: NewSourceSinkReference(),
					},
				},
			},
			ctx:  context.Background(),
			want: nil,
		},
		{
			name: "invalid initialOffset",
			ks: &KafkaSource{
				Spec: KafkaSourceSpec{
					Topics: []string{"test-topic"},
					KafkaAuthSpec: bindingsv1beta1.KafkaAuthSpec{
						BootstrapServers: []string{"kafka:9092"},
					},
					InitialOffset: badInitialOffset,
					ConsumerGroup: "ks-group",
					SourceSpec: duckv1.SourceSpec{
						Sink: NewSourceSinkReference(),
					},
				},
			},
			ctx:  context.Background(),
			want: apis.ErrInvalidValue(badInitialOffset, "spec.initialOffset"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.ks.SetDefaults(tt.ctx)
			if got := tt.ks.Validate(tt.ctx); got.Error() != tt.want.Error() {
				t.Errorf("Validate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func NewService(mutations ...func(*corev1.Service)) *corev1.Service {
	s := &corev1.Service{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Service",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-service",
			Namespace: "test-service-name",
		},
	}
	for _, mut := range mutations {
		mut(s)
	}
	return s
}

func NewSourceSinkReference() duckv1.Destination {
	s := NewService()
	return duckv1.Destination{
		Ref: &duckv1.KReference{
			Kind:       s.Kind,
			Namespace:  s.Namespace,
			Name:       s.Name,
			APIVersion: s.APIVersion,
		},
	}
}
