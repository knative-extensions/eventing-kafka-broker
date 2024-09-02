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

package v1alpha1

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"
	sources "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/sources/v1beta1"
)

func TestConsumerGroupSetDefaults(t *testing.T) {
	tests := []struct {
		name  string
		ctx   context.Context
		given *ConsumerGroup
		want  *ConsumerGroup
	}{
		{
			name: "default replicas",
			ctx:  context.Background(),
			given: &ConsumerGroup{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "ns",
					Name:      "name",
				},
				Spec: ConsumerGroupSpec{
					Template: ConsumerTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Namespace: "ns",
						},
					},
				},
			},
			want: &ConsumerGroup{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "ns",
					Name:      "name",
				},
				Spec: ConsumerGroupSpec{
					Template: ConsumerTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Namespace: "ns",
						},
						Spec: ConsumerSpec{
							Delivery: &DeliverySpec{
								InitialOffset: sources.OffsetLatest,
							},
						},
					},
					Replicas: pointer.Int32(1),
				},
			},
		},
		{
			name: "default selector",
			ctx:  context.Background(),
			given: &ConsumerGroup{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "ns",
					Name:      "name",
				},
				Spec: ConsumerGroupSpec{
					Template: ConsumerTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Namespace: "ns",
							Labels:    map[string]string{"app": "app"},
						},
					},
					Replicas: pointer.Int32(1),
				},
			},
			want: &ConsumerGroup{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "ns",
					Name:      "name",
				},
				Spec: ConsumerGroupSpec{
					Template: ConsumerTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Namespace: "ns",
							Labels:    map[string]string{"app": "app"},
						},
						Spec: ConsumerSpec{
							Delivery: &DeliverySpec{
								InitialOffset: sources.OffsetLatest,
							},
						},
					},
					Replicas: pointer.Int32(1),
					Selector: map[string]string{"app": "app"},
				},
			},
		},
		{
			name: "default namespace",
			ctx:  context.Background(),
			given: &ConsumerGroup{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "ns",
					Name:      "name",
				},
				Spec: ConsumerGroupSpec{
					Replicas: pointer.Int32(1),
				},
			},
			want: &ConsumerGroup{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "ns",
					Name:      "name",
				},
				Spec: ConsumerGroupSpec{
					Template: ConsumerTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Namespace: "ns",
						},
						Spec: ConsumerSpec{
							Delivery: &DeliverySpec{
								InitialOffset: sources.OffsetLatest,
							},
						},
					},
					Replicas: pointer.Int32(1),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.given.SetDefaults(tt.ctx)

			if diff := cmp.Diff(tt.want, tt.given); diff != "" {
				t.Error("(-want, +got)", diff)
			}
		})
	}
}
