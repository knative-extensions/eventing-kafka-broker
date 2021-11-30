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

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestValidateBrokerFromUnstructured(t *testing.T) {
	tests := []struct {
		name         string
		ctx          context.Context
		unstructured *unstructured.Unstructured
		wantErr      bool
	}{
		{
			name: "no kafka broker",
			ctx:  context.Background(),
			unstructured: &unstructured.Unstructured{
				Object: map[string]interface{}{"spec": map[string]interface{}{}},
			},
			wantErr: false,
		},
		{
			name: "missing config",
			ctx:  context.Background(),
			unstructured: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"annotations": map[string]string{
							"eventing.knative.dev/broker.class": "Kafka",
						},
					},
					"spec": map[string]interface{}{},
				},
			},
			wantErr: true,
		},
		{
			name: "unknown kind",
			ctx:  context.Background(),
			unstructured: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"annotations": map[string]string{
							"eventing.knative.dev/broker.class": "Kafka",
						},
					},
					"spec": map[string]interface{}{
						"config": map[string]string{
							"apiVersion": "eventing.knative.dev/v1",
							"kind":       "Broker",
							"namespace":  "ns",
							"name":       "name",
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "ConfigMap in config",
			ctx:  context.Background(),
			unstructured: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"annotations": map[string]string{
							"eventing.knative.dev/broker.class": "Kafka",
						},
					},
					"spec": map[string]interface{}{
						"config": map[string]string{
							"apiVersion": "v1",
							"kind":       "ConfigMap",
							"namespace":  "ns",
							"name":       "name",
						},
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateBrokerFromUnstructured(tt.ctx, tt.unstructured); (err != nil) != tt.wantErr {
				t.Errorf("validateBrokerFromUnstructured() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
