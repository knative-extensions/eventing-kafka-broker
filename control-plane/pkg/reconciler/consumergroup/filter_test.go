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

package consumergroup

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kafkainternals "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing/v1alpha1"
)

func TestFilter(t *testing.T) {
	tests := []struct {
		name               string
		resource           interface{}
		userFacingResource string
		want               bool
	}{
		{
			name:               "unknown type",
			resource:           &kafkainternals.Consumer{},
			userFacingResource: "trigger",
			want:               false,
		},
		{
			name: "trigger pass",
			resource: &kafkainternals.ConsumerGroup{
				TypeMeta: metav1.TypeMeta{},
				ObjectMeta: metav1.ObjectMeta{
					OwnerReferences: []metav1.OwnerReference{
						{
							Kind: "trigger",
						},
					},
				},
			},
			userFacingResource: "trigger",
			want:               true,
		},
		{
			name: "no pass",
			resource: &kafkainternals.ConsumerGroup{
				TypeMeta: metav1.TypeMeta{},
				ObjectMeta: metav1.ObjectMeta{
					OwnerReferences: []metav1.OwnerReference{
						{
							Kind: "kafkasource",
						},
					},
				},
			},
			userFacingResource: "trigger",
			want:               false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Filter(tt.userFacingResource)(tt.resource); got != tt.want {
				t.Errorf("Filter() = %v, want %v", got, tt.want)
			}
		})
	}
}
