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

package config

import (
	"io"
	"os"
	"reflect"
	"testing"
)

func TestGetEnvConfig(t *testing.T) {

	tests := []struct {
		name        string
		prefix      string
		validations []ValidationOption
		setEnv      func()
		want        *Env
		wantErr     bool
	}{
		{
			name:   "broker prefix",
			prefix: "BROKER",
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
			name:   "sink prefix",
			prefix: "SINK",
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
			name:   "missing required variable - SINK_DATA_PLANE_CONFIG_MAP_NAMESPACE",
			prefix: "SINK",
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
			name:   "invalid backoff delay 0",
			prefix: "BROKER",
			validations: []ValidationOption{
				func(env Env) error {
					if env.DefaultBackoffDelayMs == 0 {
						return io.EOF
					}
					return nil
				},
			},
			setEnv: func() {
				_ = os.Setenv("BROKER_DATA_PLANE_CONFIG_MAP_NAMESPACE", "knative-eventing")
				_ = os.Setenv("BROKER_DATA_PLANE_CONFIG_MAP_NAME", "kafka-brokers-triggers")
				_ = os.Setenv("BROKER_GENERAL_CONFIG_MAP_NAME", "kafka-config")
				_ = os.Setenv("BROKER_INGRESS_NAME", "kafka-broker-ingress")
				_ = os.Setenv("BROKER_SYSTEM_NAMESPACE", "knative-eventing")
				_ = os.Setenv("BROKER_DATA_PLANE_CONFIG_FORMAT", "json")
				_ = os.Setenv("BROKER_DEFAULT_BACKOFF_DELAY_MS", "0")
			},
			wantErr: true,
		},
		{
			name:   "invalid backoff delay unset",
			prefix: "BROKER",
			validations: []ValidationOption{
				func(env Env) error {
					if env.DefaultBackoffDelayMs == 0 {
						return io.EOF
					}
					return nil
				},
			},
			setEnv: func() {
				_ = os.Setenv("BROKER_DATA_PLANE_CONFIG_MAP_NAMESPACE", "knative-eventing")
				_ = os.Setenv("BROKER_DATA_PLANE_CONFIG_MAP_NAME", "kafka-brokers-triggers")
				_ = os.Setenv("BROKER_GENERAL_CONFIG_MAP_NAME", "kafka-config")
				_ = os.Setenv("BROKER_INGRESS_NAME", "kafka-broker-ingress")
				_ = os.Setenv("BROKER_SYSTEM_NAMESPACE", "knative-eventing")
				_ = os.Setenv("BROKER_DATA_PLANE_CONFIG_FORMAT", "json")
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setEnv()
			defer os.Clearenv()

			got, err := GetEnvConfig(tt.prefix, tt.validations...)
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
