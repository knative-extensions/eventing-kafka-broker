package propagator

import (
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestPropagate(t *testing.T) {

	serviceMonitorX := unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "monitoring.coreos.com/v1",
			"kind":       "ServiceMonitor",
			"spec": map[string]interface{}{
				"targetLabels": "x",
			},
		},
	}
	serviceMonitorY := unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "monitoring.coreos.com/v1",
			"kind":       "ServiceMonitor",
			"spec": map[string]interface{}{
				"targetLabels": "y",
			},
		},
	}

	tests := []struct {
		name    string
		cm      v1.ConfigMap
		want    Resources
		wantErr bool
	}{
		{
			name: "Propagate ServiceMonitor",
			cm: v1.ConfigMap{
				Data: map[string]string{
					"resources": `
- apiVersion: monitoring.coreos.com/v1
  kind: ServiceMonitor
  spec:
    targetLabels: "x"
- apiVersion: monitoring.coreos.com/v1
  kind: ServiceMonitor
  spec:
    targetLabels: "y"
`,
				},
			},
			want:    Resources{Resources: []unstructured.Unstructured{serviceMonitorX, serviceMonitorY}},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Propagate(tt.cm)
			if (err != nil) != tt.wantErr {
				t.Errorf("Propagate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Propagate() got = %v, want %v", got, tt.want)
				t.Errorf("Diff (-want, +got)\n%s", cmp.Diff(tt.want, got))
			}
		})
	}
}
