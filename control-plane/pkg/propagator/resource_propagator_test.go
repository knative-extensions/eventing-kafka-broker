/*
 * Copyright 2022 The Knative Authors
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

package propagator

import (
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

var (
	serviceMonitorX = unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "monitoring.coreos.com/v1",
			"kind":       "ServiceMonitor",
			"spec": map[string]interface{}{
				"targetLabels": "x",
			},
		},
	}
	serviceMonitorY = unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "monitoring.coreos.com/v1",
			"kind":       "ServiceMonitor",
			"spec": map[string]interface{}{
				"targetLabels": "y",
			},
		},
	}

	resources = `- apiVersion: monitoring.coreos.com/v1
  kind: ServiceMonitor
  spec:
    targetLabels: x
- apiVersion: monitoring.coreos.com/v1
  kind: ServiceMonitor
  spec:
    targetLabels: "y"
`
)

func TestUnmarshal(t *testing.T) {

	tests := []struct {
		name    string
		cm      v1.ConfigMap
		want    Resources
		wantErr bool
	}{
		{
			name: "Unmarshal ServiceMonitors",
			cm: v1.ConfigMap{
				Data: map[string]string{
					"resources": resources,
				},
			},
			want:    Resources{Resources: []unstructured.Unstructured{serviceMonitorX, serviceMonitorY}},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Unmarshal(&tt.cm)
			if (err != nil) != tt.wantErr {
				t.Errorf("Unmarshal() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Unmarshal() got = %v, want %v", got, tt.want)
				t.Errorf("Diff (-want, +got)\n%s", cmp.Diff(tt.want, got))
			}
		})
	}
}

func TestMarshal(t *testing.T) {

	tests := []struct {
		name      string
		resources Resources
		want      string
		wantErr   bool
	}{
		{
			name:      "Marshal ServiceMonitors",
			resources: Resources{Resources: []unstructured.Unstructured{serviceMonitorX, serviceMonitorY}},
			want:      resources,
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Marshal(tt.resources)
			if (err != nil) != tt.wantErr {
				t.Errorf("Marshal() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Marshal() got = %v, want %v", got, tt.want)
				t.Errorf("Diff (-want, +got)\n%s", cmp.Diff(tt.want, got))
			}
		})
	}
}
