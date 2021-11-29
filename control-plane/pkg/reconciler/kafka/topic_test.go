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

package kafka

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1"

	kafkatesting "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka/testing"
)

func TestCreateTopic(t *testing.T) {
	type args struct {
		admin  sarama.ClusterAdmin
		logger *zap.Logger
		topic  string
		config *TopicConfig
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "Topic created no error",
			args: args{
				admin: &kafkatesting.MockKafkaClusterAdmin{
					ExpectedTopicName: "topic-name-1",
					ExpectedTopicDetail: sarama.TopicDetail{
						NumPartitions:     10,
						ReplicationFactor: 3,
					},
					T: t,
				},
				logger: zap.NewNop(),
				topic:  "topic-name-1",
				config: &TopicConfig{
					TopicDetail: sarama.TopicDetail{
						NumPartitions:     10,
						ReplicationFactor: 3,
					},
					BootstrapServers: []string{"server-1:9092", "server-2:8989"},
				},
			},
			want:    "topic-name-1",
			wantErr: false,
		},
		{
			name: "Topic already exists",
			args: args{
				admin: &kafkatesting.MockKafkaClusterAdmin{
					ExpectedTopicName: "topic-name-1",
					ExpectedTopicDetail: sarama.TopicDetail{
						NumPartitions:     10,
						ReplicationFactor: 3,
					},
					ErrorOnCreateTopic: &sarama.TopicError{
						Err: sarama.ErrTopicAlreadyExists,
					},
					T: t,
				},
				logger: zap.NewNop(),
				topic:  "topic-name-1",
				config: &TopicConfig{
					TopicDetail: sarama.TopicDetail{
						NumPartitions:     10,
						ReplicationFactor: 3,
					},
					BootstrapServers: []string{"server-1:9092", "server-2:8989"},
				},
			},
			want:    "topic-name-1",
			wantErr: false,
		},
		{
			name: "Create Topic error",
			args: args{
				admin: &kafkatesting.MockKafkaClusterAdmin{
					ExpectedTopicName: "topic-name-1",
					ExpectedTopicDetail: sarama.TopicDetail{
						NumPartitions:     10,
						ReplicationFactor: 3,
					},
					ErrorOnCreateTopic: &sarama.TopicError{
						Err: 2999,
					},
					T: t,
				},
				logger: zap.NewNop(),
				topic:  "topic-name-1",
				config: &TopicConfig{
					TopicDetail: sarama.TopicDetail{
						NumPartitions:     10,
						ReplicationFactor: 3,
					},
					BootstrapServers: []string{"server-1:9092", "server-2:8989"},
				},
			},
			want:    "topic-name-1",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CreateTopicIfDoesntExist(tt.args.admin, tt.args.logger, tt.args.topic, tt.args.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateTopicIfDoesntExist() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("CreateTopicIfDoesntExist() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTopic(t *testing.T) {
	type args struct {
		prefix string
		obj    *metav1.ObjectMeta
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "prefix-namespace-name",
			args: args{
				prefix: "pr-",
				obj: &metav1.ObjectMeta{
					Name:      "n",
					Namespace: "ns",
				},
			},
			want: "pr-ns-n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := BrokerTopic(tt.args.prefix, tt.args.obj); got != tt.want {
				t.Errorf("Topic() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTopicConfig_GetBootstrapServers(t *testing.T) {
	type fields struct {
		TopicDetail      sarama.TopicDetail
		BootstrapServers []string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "1 broker",
			fields: fields{
				BootstrapServers: []string{"broker1:9092"},
			},
			want: "broker1:9092",
		},
		{
			name: "2 brokers",
			fields: fields{
				BootstrapServers: []string{"broker1:9092", "broker2:9092"},
			},
			want: "broker1:9092,broker2:9092",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := TopicConfig{
				TopicDetail:      tt.fields.TopicDetail,
				BootstrapServers: tt.fields.BootstrapServers,
			}
			if got := c.GetBootstrapServers(); got != tt.want {
				t.Errorf("GetBootstrapServers() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDeleteTopic(t *testing.T) {
	type args struct {
		admin sarama.ClusterAdmin
		topic string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "no errors",
			args: args{
				admin: &kafkatesting.MockKafkaClusterAdmin{
					ExpectedTopicName: "topic-name",
					T:                 t,
				},
				topic: "topic-name",
			},
			want:    "topic-name",
			wantErr: false,
		},
		{
			name: "unknown error",
			args: args{
				admin: &kafkatesting.MockKafkaClusterAdmin{
					ExpectedTopicName:  "topic-name",
					ErrorOnDeleteTopic: fmt.Errorf("failed to delete topic topic-name"),
					T:                  t,
				},
				topic: "topic-name",
			},
			want:    "topic-name",
			wantErr: true,
		},
		{
			name: "unknown topic or partition error (external)",
			args: args{
				admin: &kafkatesting.MockKafkaClusterAdmin{
					ExpectedTopicName:  "topic-name",
					ErrorOnDeleteTopic: sarama.ErrUnknownTopicOrPartition,
					T:                  t,
				},
				topic: "topic-name",
			},
			want:    "topic-name",
			wantErr: false,
		},
		{
			name: "unknown topic or partition error (internal)",
			args: args{
				admin: &kafkatesting.MockKafkaClusterAdmin{
					ExpectedTopicName: "topic-name",
					ErrorOnDeleteTopic: &sarama.TopicError{
						Err: sarama.ErrUnknownTopicOrPartition,
					},
					T: t,
				},
				topic: "topic-name",
			},
			want:    "topic-name",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DeleteTopic(tt.args.admin, tt.args.topic)
			if (err != nil) != tt.wantErr {
				t.Errorf("DeleteTopic() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("DeleteTopic() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCreateTopicTopicAlreadyExists(t *testing.T) {

	b := &eventing.Broker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "bname",
			Namespace: "bnamespace",
		},
	}
	topic := BrokerTopic("", b)
	errMsg := "topic already exists"

	ca := &kafkatesting.MockKafkaClusterAdmin{
		ExpectedTopicName:   topic,
		ExpectedTopicDetail: sarama.TopicDetail{},
		ErrorOnCreateTopic: &sarama.TopicError{
			Err:    sarama.ErrTopicAlreadyExists,
			ErrMsg: &errMsg,
		},
		T: t,
	}

	topicRet, err := CreateTopicIfDoesntExist(ca, zap.NewNop(), topic, &TopicConfig{})

	assert.Equal(t, topicRet, topic, "expected topic %s go %s", topic, topicRet)
	assert.Nil(t, err, "expected nil error on topic already exists")
}

func TestNewClusterAdminClientFuncIsTopicPresent(t *testing.T) {
	tests := []struct {
		name         string
		clusterAdmin sarama.ClusterAdmin
		topics       []string
		want         bool
		wantErr      bool
	}{
		{
			name: "topic exists",
			clusterAdmin: &kafkatesting.MockKafkaClusterAdmin{
				ExpectedTopics: []string{"topic-name-1"},
				ExpectedTopicsMetadataOnDescribeTopics: []*sarama.TopicMetadata{
					{
						Name:       "topic-name-1",
						IsInternal: false,
						Partitions: []*sarama.PartitionMetadata{{}},
					},
				},
				T: t,
			},
			topics:  []string{"topic-name-1"},
			want:    true,
			wantErr: false,
		},
		{
			name: "topic exists - internal topic",
			clusterAdmin: &kafkatesting.MockKafkaClusterAdmin{
				ExpectedTopics: []string{"topic-name-1"},
				ExpectedTopicsMetadataOnDescribeTopics: []*sarama.TopicMetadata{
					{
						Name:       "topic-name-1",
						IsInternal: true,
					},
				},
				T: t,
			},
			topics:  []string{"topic-name-1"},
			want:    false,
			wantErr: true,
		},
		{
			name: "topic exists - no partitions",
			clusterAdmin: &kafkatesting.MockKafkaClusterAdmin{
				ExpectedTopics: []string{"topic-name-1"},
				ExpectedTopicsMetadataOnDescribeTopics: []*sarama.TopicMetadata{
					{
						Name:       "topic-name-1",
						IsInternal: false,
						Partitions: []*sarama.PartitionMetadata{},
					},
				},
				T: t,
			},
			topics:  []string{"topic-name-1"},
			want:    false,
			wantErr: true,
		},
		{
			name:    "no topics given",
			topics:  []string{},
			want:    false,
			wantErr: true,
		},
		{
			name: "DescribeTopics returns error",
			clusterAdmin: &kafkatesting.MockKafkaClusterAdmin{
				ExpectedTopics:                []string{"topic-name-1"},
				ExpectedErrorOnDescribeTopics: fmt.Errorf("error"),
				T:                             t,
			},
			topics:  []string{"topic-name-1"},
			want:    false,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := AreTopicsPresentAndValid(tt.clusterAdmin, tt.topics...)
			if (err != nil) != tt.wantErr {
				t.Errorf("AreTopicsPresentAndValid() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("AreTopicsPresentAndValid() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestInvalidOrNotPresentTopic(t *testing.T) {
	err := &InvalidOrNotPresentTopic{Topic: "topic"}

	require.Contains(t, err.Error(), err.Topic)
}

func TestBootstrapServersArray(t *testing.T) {
	bss := BootstrapServersArray("bs:9091, bs:9000,,bs:9002,")

	require.Contains(t, bss, "bs:9091")
	require.Contains(t, bss, "bs:9000")
	require.Contains(t, bss, "bs:9002")
	require.Len(t, bss, 3)
}

func TestTopicConfigFromConfigMap(t *testing.T) {
	tests := []struct {
		name    string
		data    map[string]string
		want    TopicConfig
		wantErr bool
	}{
		{
			name: "All valid",
			data: map[string]string{
				"default.topic.partitions":         "5",
				"default.topic.replication.factor": "8",
				"bootstrap.servers":                "server1:9092, server2:9092",
			},
			want: TopicConfig{
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
