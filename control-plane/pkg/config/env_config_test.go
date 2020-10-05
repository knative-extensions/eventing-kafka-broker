package config

import (
	"os"
	"reflect"
	"testing"
)

func TestGetEnvConfig(t *testing.T) {
	type args struct {
		prefix string
	}

	tests := []struct {
		name    string
		args    args
		setEnv  func()
		want    *Env
		wantErr bool
	}{
		{
			name: "broker prefix",
			args: args{
				prefix: "BROKER",
			},
			want: &Env{
				DataPlaneConfigMapNamespace: "knative-eventing",
				DataPlaneConfigMapName:      "kafka-brokers-triggers",
				GeneralConfigMapName:        "kafka-config",
				IngressName:                 "kafka-broker-ingress",
				SystemNamespace:             "knative-eventing",
				DataPlaneConfigFormat:       "json",
			},
			setEnv: func() {
				_ = os.Setenv("BROKER_DATA_PLANE_CONFIG_MAP_NAMESPACE", "knative-eventing")
				_ = os.Setenv("BROKER_DATA_PLANE_CONFIG_MAP_NAME", "kafka-brokers-triggers")
				_ = os.Setenv("BROKER_GENERAL_CONFIG_MAP_NAME", "kafka-config")
				_ = os.Setenv("BROKER_INGRESS_NAME", "kafka-broker-ingress")
				_ = os.Setenv("BROKER_SYSTEM_NAMESPACE", "knative-eventing")
				_ = os.Setenv("BROKER_DATA_PLANE_CONFIG_FORMAT", "json")
			},
			wantErr: false,
		},
		{
			name: "sink prefix",
			args: args{
				prefix: "SINK",
			},
			want: &Env{
				DataPlaneConfigMapNamespace: "knative-eventing",
				DataPlaneConfigMapName:      "kafka-sinks",
				GeneralConfigMapName:        "kafka-config",
				IngressName:                 "kafka-sink-ingress",
				SystemNamespace:             "knative-eventing",
				DataPlaneConfigFormat:       "json",
			},
			setEnv: func() {
				_ = os.Setenv("SINK_DATA_PLANE_CONFIG_MAP_NAMESPACE", "knative-eventing")
				_ = os.Setenv("SINK_DATA_PLANE_CONFIG_MAP_NAME", "kafka-sinks")
				_ = os.Setenv("SINK_GENERAL_CONFIG_MAP_NAME", "kafka-config")
				_ = os.Setenv("SINK_INGRESS_NAME", "kafka-sink-ingress")
				_ = os.Setenv("SINK_SYSTEM_NAMESPACE", "knative-eventing")
				_ = os.Setenv("SINK_DATA_PLANE_CONFIG_FORMAT", "json")
			},
			wantErr: false,
		},
		{
			name: "missing required variable - SINK_DATA_PLANE_CONFIG_MAP_NAMESPACE",
			args: args{
				prefix: "SINK",
			},
			setEnv: func() {
				_ = os.Setenv("SINK_DATA_PLANE_CONFIG_MAP_NAME", "kafka-sinks")
				_ = os.Setenv("SINK_GENERAL_CONFIG_MAP_NAME", "kafka-config")
				_ = os.Setenv("SINK_INGRESS_NAME", "kafka-sink-ingress")
				_ = os.Setenv("SINK_SYSTEM_NAMESPACE", "knative-eventing")
				_ = os.Setenv("SINK_DATA_PLANE_CONFIG_FORMAT", "json")
			},
			wantErr: true,
		},
		{
			name: "invalid backoff delay",
			args: args{
				prefix: "BROKER",
			},
			setEnv: func() {
				_ = os.Setenv("BROKER_DATA_PLANE_CONFIG_MAP_NAMESPACE", "knative-eventing")
				_ = os.Setenv("BROKER_DATA_PLANE_CONFIG_MAP_NAME", "kafka-brokers-triggers")
				_ = os.Setenv("BROKER_GENERAL_CONFIG_MAP_NAME", "kafka-config")
				_ = os.Setenv("BROKER_INGRESS_NAME", "kafka-broker-ingress")
				_ = os.Setenv("BROKER_SYSTEM_NAMESPACE", "knative-eventing")
				_ = os.Setenv("BROKER_DATA_PLANE_CONFIG_FORMAT", "json")
				_ = os.Setenv("BROKER_DEFAULT_BACKOFF_DELAY", "PTT")
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setEnv()
			defer os.Clearenv()

			got, err := GetEnvConfig(tt.args.prefix)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetEnvConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetEnvConfig() got = %v, want %v", got, tt.want)
			}
		})
	}
}
