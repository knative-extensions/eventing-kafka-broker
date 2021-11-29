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

package broker

import (
	"reflect"
	"testing"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka"
)

func TestTopicConfigFromConfigMap(t *testing.T) {
	tests := []struct {
		name    string
		data    map[string]string
		want    kafka.TopicConfig
		wantErr bool
	}{
		{
			name: "All valid",
			data: map[string]string{
				"default.topic.partitions":         "5",
				"default.topic.replication.factor": "8",
				"bootstrap.servers":                "server1:9092, server2:9092",
			},
			want: kafka.TopicConfig{
				TopicDetail: sarama.TopicDetail{
					NumPartitions:     5,
					ReplicationFactor: 8,
				},
				BootstrapServers: []string{"server1:9092", "server2:9092"},
			},
		},
		{
			name: "Missing keys 'default.topic.partitions' - not allowed",
			data: map[string]string{
				"default.topic.replication.factor": "8",
				"bootstrap.servers":                "server1:9092, server2:9092",
			},
			wantErr: true,
		},
		{
			name: "Missing keys 'default.topic.replication.factor' - not allowed",
			data: map[string]string{
				"default.topic.partitions": "5",
				"bootstrap.servers":        "server1:9092, server2:9092",
			},
			wantErr: true,
		},
		{
			name: "Missing keys 'bootstrap.servers' - not allowed",
			data: map[string]string{
				"default.topic.partitions":         "5",
				"default.topic.replication.factor": "8",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {

		logger := zap.NewNop()

		cm := &corev1.ConfigMap{
			Data: tt.data,
		}

		t.Run(tt.name, func(t *testing.T) {
			got, err := TopicConfigFromConfigMap(logger, cm)

			if (err != nil) != tt.wantErr {
				t.Errorf("TopicConfigFromConfigMap() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && !reflect.DeepEqual(*got, tt.want) {
				t.Errorf("TopicConfigFromConfigMap() got = %v, want %v", got, tt.want)
			}
		})
	}
}
