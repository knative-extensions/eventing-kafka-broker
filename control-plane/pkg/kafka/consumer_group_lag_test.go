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
	"io"
	"io/ioutil"
	"math"
	"net"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/sets"
)

func TestConsumerGroupLagProvider(t *testing.T) {

	const group = "group-1"

	brokerAddr := "localhost:43245"
	broker, closeFunc := fakeKafkaBrokerListener(t, brokerAddr)
	defer closeFunc()

	tt := []struct {
		name    string
		client  *saramaClientMock
		wantLag uint64
	}{
		{
			name: "No Lag",
			client: &saramaClientMock{
				topics: []string{"topic-1"},
				partitionsByTopic: map[string][]Partition{
					"topic-1": {
						{ID: 0, ConsumerOffset: 10, LatestOffset: 10},
						{ID: 1, ConsumerOffset: 0, LatestOffset: 0},
						{ID: 2, ConsumerOffset: 3, LatestOffset: 3},
						{ID: 3, ConsumerOffset: 20, LatestOffset: 20},
						{ID: 4, ConsumerOffset: 12, LatestOffset: 12},
					},
				},
				groups: []string{group},
				broker: broker,
			},
		},
		{
			name: "Single partition Lag",
			client: &saramaClientMock{
				topics: []string{"topic-1", "topic-2", "topic-3"},
				partitionsByTopic: map[string][]Partition{
					"topic-1": {
						{ID: 0, ConsumerOffset: 10, LatestOffset: 10},
						{ID: 1, ConsumerOffset: 0, LatestOffset: 3},
						{ID: 2, ConsumerOffset: 3, LatestOffset: 3},
						{ID: 3, ConsumerOffset: 20, LatestOffset: 20},
						{ID: 4, ConsumerOffset: 12, LatestOffset: 12},
					},
					"topic-2": {
						{ID: 0, ConsumerOffset: 10, LatestOffset: 10},
						{ID: 1, ConsumerOffset: 3, LatestOffset: 3},
						{ID: 2, ConsumerOffset: 3, LatestOffset: 3},
						{ID: 3, ConsumerOffset: 20, LatestOffset: 20},
						{ID: 4, ConsumerOffset: 1, LatestOffset: 12},
					},
					"topic-3": {
						{ID: 0, ConsumerOffset: 1, LatestOffset: 7},
						{ID: 1, ConsumerOffset: 3, LatestOffset: 3},
						{ID: 2, ConsumerOffset: 3, LatestOffset: 3},
						{ID: 3, ConsumerOffset: 20, LatestOffset: 20},
						{ID: 4, ConsumerOffset: 12, LatestOffset: 12},
					},
				},
				groups: []string{group},
				broker: broker,
			},
		},
		{
			name: "Partitions Lag",
			client: &saramaClientMock{
				topics: []string{"topic-1", "topic-2", "topic-3", "topic-4"},
				partitionsByTopic: map[string][]Partition{
					"topic-1": {
						{ID: 0, ConsumerOffset: 7, LatestOffset: 10},
						{ID: 1, ConsumerOffset: 0, LatestOffset: 3},
						{ID: 2, ConsumerOffset: 2, LatestOffset: 3},
						{ID: 3, ConsumerOffset: 20, LatestOffset: 20},
						{ID: 4, ConsumerOffset: 1, LatestOffset: 12},
					},
					"topic-2": {
						{ID: 0, ConsumerOffset: 7, LatestOffset: 10},
					},
					"topic-3": {
						{ID: 0, ConsumerOffset: 0, LatestOffset: 10},
						{ID: 1, ConsumerOffset: 0, LatestOffset: 3},
						{ID: 2, ConsumerOffset: 0, LatestOffset: 3},
						{ID: 3, ConsumerOffset: 0, LatestOffset: 20},
						{ID: 4, ConsumerOffset: 0, LatestOffset: 12},
					},
					"topic-4": {
						{ID: 0, ConsumerOffset: 0, LatestOffset: math.MaxInt64},
						{ID: 1, ConsumerOffset: 0, LatestOffset: math.MaxInt64},
						{ID: 2, ConsumerOffset: 0, LatestOffset: math.MaxInt64},
						{ID: 3, ConsumerOffset: 0, LatestOffset: math.MaxInt64},
						{ID: 4, ConsumerOffset: 0, LatestOffset: math.MaxInt64},
					},
				},
				groups: []string{group},
				broker: broker,
			},
		},
	}

	t.Parallel()
	for _, tc := range tt {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			tc.client.t = t

			for _, topic := range tc.client.topics {
				t.Run(topic, func(t *testing.T) {
					provider := NewConsumerGroupLagProvider(tc.client, func(sarama.Client) (sarama.ClusterAdmin, error) { return tc.client, nil }, -100)

					lag, err := provider.GetLag(topic, group)
					require.Nil(t, err)

					wantLag := uint64(0)
					for _, p := range tc.client.partitionsByTopic[topic] {
						wantLag += uint64(p.LatestOffset - p.ConsumerOffset)
					}

					require.Equal(t, wantLag, lag.Total())

					err = provider.Close()
					require.Nil(t, err)
					require.True(t, tc.client.closed)
				})
			}
		})
	}

}

func TestConsumerGroupLagTotal(t *testing.T) {
	tests := []struct {
		name string
		cgl  ConsumerGroupLag
		want uint64
	}{
		{
			name: "empty",
			cgl: ConsumerGroupLag{
				Topic:       "topic",
				ByPartition: []PartitionLag{},
			},
			want: 0,
		},
		{
			name: "consumer group lags",
			cgl: ConsumerGroupLag{
				Topic: "topic",
				ByPartition: []PartitionLag{
					{LatestOffset: 23, ConsumerOffset: 3},
					{LatestOffset: 22, ConsumerOffset: 1},
					{LatestOffset: 22, ConsumerOffset: 0},
					{LatestOffset: 2, ConsumerOffset: 1},
				},
			},
			want: 20 + 21 + 22 + 1,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.cgl.Total(); got != tt.want {
				t.Errorf("Total() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConsumerGroupLagString(t *testing.T) {
	cgl := ConsumerGroupLag{}
	require.NotEmpty(t, cgl.String())
}

type Partition struct {
	ConsumerOffset int64
	LatestOffset   int64
	ID             int32
}

type saramaClientMock struct {
	t                 *testing.T
	topics            []string
	groups            []string
	partitionsByTopic map[string][]Partition
	broker            *sarama.Broker
	closed            bool
}

func (s saramaClientMock) CreateTopic(topic string, _ *sarama.TopicDetail, _ bool) error {
	s.hasTopic(topic)
	return nil
}

func (s saramaClientMock) ListTopics() (map[string]sarama.TopicDetail, error) {
	m := make(map[string]sarama.TopicDetail, len(s.topics))
	for _, t := range s.topics {
		m[t] = sarama.TopicDetail{
			NumPartitions:     int32(len(s.partitionsByTopic[t])),
			ReplicationFactor: 1,
			ReplicaAssignment: map[int32][]int32{},
			ConfigEntries:     map[string]*string{},
		}
	}
	return m, nil
}

func (s saramaClientMock) DescribeTopics(topics []string) (metadata []*sarama.TopicMetadata, err error) {
	for _, t := range topics {
		s.hasTopic(t)
		partitions := make([]*sarama.PartitionMetadata, 0, len(s.partitionsByTopic[t]))
		for _, p := range s.partitionsByTopic[t] {
			partitions = append(partitions, &sarama.PartitionMetadata{ID: p.ID})
		}
		metadata = append(metadata, &sarama.TopicMetadata{
			Err:        0,
			Name:       t,
			IsInternal: false,
			Partitions: partitions,
		})
	}
	return
}

func (s saramaClientMock) DeleteTopic(topic string) error {
	s.hasTopic(topic)
	return nil
}

func (s saramaClientMock) CreatePartitions(topic string, _ int32, _ [][]int32, _ bool) error {
	s.hasTopic(topic)
	return nil
}

func (s saramaClientMock) AlterPartitionReassignments(topic string, _ [][]int32) error {
	s.hasTopic(topic)
	return nil
}

func (s saramaClientMock) ListPartitionReassignments(topic string, _ []int32) (topicStatus map[string]map[int32]*sarama.PartitionReplicaReassignmentsStatus, err error) {
	s.hasTopic(topic)
	return
}

func (s saramaClientMock) DeleteRecords(topic string, _ map[int32]int64) error {
	s.hasTopic(topic)
	return nil
}

func (s saramaClientMock) DescribeConfig(sarama.ConfigResource) ([]sarama.ConfigEntry, error) {
	panic("implement me")
}

func (s saramaClientMock) AlterConfig(sarama.ConfigResourceType, string, map[string]*string, bool) error {
	panic("implement me")
}

func (s saramaClientMock) CreateACL(sarama.Resource, sarama.Acl) error {
	panic("implement me")
}

func (s saramaClientMock) ListAcls(sarama.AclFilter) ([]sarama.ResourceAcls, error) {
	panic("implement me")
}

func (s saramaClientMock) DeleteACL(sarama.AclFilter, bool) ([]sarama.MatchingAcl, error) {
	panic("implement me")
}

func (s saramaClientMock) ListConsumerGroups() (map[string]string, error) {
	panic("implement me")
}

func (s saramaClientMock) DescribeConsumerGroups([]string) ([]*sarama.GroupDescription, error) {
	panic("implement me")
}

func (s saramaClientMock) ListConsumerGroupOffsets(group string, topicPartitions map[string][]int32) (*sarama.OffsetFetchResponse, error) {
	s.hasGroup(group)

	s.hasTopicPartitions(topicPartitions)

	response := &sarama.OffsetFetchResponse{
		Version: 2,
		Blocks:  map[string]map[int32]*sarama.OffsetFetchResponseBlock{},
	}
	for _, t := range s.topics {
		response.Blocks[t] = map[int32]*sarama.OffsetFetchResponseBlock{}
		for _, p := range s.partitionsByTopic[t] {
			response.Blocks[t][p.ID] = &sarama.OffsetFetchResponseBlock{Offset: p.ConsumerOffset}
		}
	}
	return response, nil
}

func (s saramaClientMock) hasTopicPartitions(topicPartitions map[string][]int32) {
	for topic, partitions := range topicPartitions {
		s.hasTopic(topic)
		for _, p := range partitions {
			s.hasPartition(topic, p)
		}
	}
}

func (s saramaClientMock) DeleteConsumerGroup(group string) error {
	s.hasGroup(group)
	return nil
}

func (s saramaClientMock) DescribeCluster() (brokers []*sarama.Broker, controllerID int32, err error) {
	panic("implement me")
}

func (s saramaClientMock) DescribeLogDirs([]int32) (map[int32][]sarama.DescribeLogDirsResponseDirMetadata, error) {
	panic("implement me")
}

func (s saramaClientMock) Config() *sarama.Config {
	return sarama.NewConfig()
}

func (s saramaClientMock) Controller() (*sarama.Broker, error) {
	return s.broker, nil
}

func (s saramaClientMock) RefreshController() (*sarama.Broker, error) {
	return s.broker, nil
}

func (s saramaClientMock) Brokers() []*sarama.Broker {
	return []*sarama.Broker{s.broker}
}

func (s saramaClientMock) Broker(int32) (*sarama.Broker, error) {
	return s.broker, nil
}

func (s saramaClientMock) Topics() ([]string, error) {
	return s.topics, nil
}

func (s saramaClientMock) Partitions(topic string) ([]int32, error) {
	s.hasTopic(topic)
	return s.getPartitions(topic), nil
}

func (s saramaClientMock) WritablePartitions(topic string) ([]int32, error) {
	s.hasTopic(topic)
	return s.getPartitions(topic), nil
}

func (s saramaClientMock) Leader(topic string, partitionID int32) (*sarama.Broker, error) {
	s.hasTopic(topic)
	s.hasPartition(topic, partitionID)
	return s.broker, nil
}

func (s saramaClientMock) Replicas(topic string, partitionID int32) ([]int32, error) {
	s.hasTopic(topic)
	s.hasPartition(topic, partitionID)
	panic("implement me")
}

func (s saramaClientMock) hasPartition(topic string, partitionID int32) {
	require.True(s.t, sets.NewInt32(s.getPartitions(topic)...).Has(partitionID))
}

func (s saramaClientMock) hasTopic(topic string) {
	require.True(s.t, sets.NewString(s.topics...).Has(topic))
}

func (s saramaClientMock) hasGroup(group string) {
	require.True(s.t, sets.NewString(s.groups...).Has(group))
}

func (s saramaClientMock) InSyncReplicas(topic string, partitionID int32) ([]int32, error) {
	s.hasTopic(topic)
	s.hasPartition(topic, partitionID)
	panic("implement me")
}

func (s saramaClientMock) OfflineReplicas(topic string, partitionID int32) ([]int32, error) {
	s.hasTopic(topic)
	s.hasPartition(topic, partitionID)
	panic("implement me")
}

func (s saramaClientMock) RefreshBrokers([]string) error {
	return nil
}

func (s saramaClientMock) RefreshMetadata(topics ...string) error {
	for _, t := range topics {
		s.hasTopic(t)
	}
	return nil
}

func (s saramaClientMock) GetOffset(topic string, partitionID int32, time int64) (int64, error) {
	require.Equal(s.t, sarama.OffsetNewest, time)
	s.hasTopic(topic)
	s.hasPartition(topic, partitionID)

	for _, p := range s.partitionsByTopic[topic] {
		if p.ID == partitionID {
			return p.LatestOffset, nil
		}
	}
	return -1, nil
}

func (s saramaClientMock) Coordinator(string) (*sarama.Broker, error) {
	return s.broker, nil
}

func (s saramaClientMock) RefreshCoordinator(string) error {
	return nil
}

func (s saramaClientMock) InitProducerID() (*sarama.InitProducerIDResponse, error) {
	panic("implement me")
}

func (s *saramaClientMock) Close() error {
	if !s.closed {
		s.closed = true
	}
	return nil
}

func (s saramaClientMock) Closed() bool {
	return s.closed
}

func (s saramaClientMock) getPartitions(topic string) []int32 {
	partitions := make([]int32, 0, len(s.partitionsByTopic[topic]))
	for _, p := range s.partitionsByTopic[topic] {
		partitions = append(partitions, p.ID)
	}
	return partitions
}

func fakeKafkaBrokerListener(t *testing.T, addr string) (*sarama.Broker, func()) {
	// Fake Kafka broker
	listener, err := net.Listen("tcp", addr)
	require.Nil(t, err)

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		_, _ = io.Copy(ioutil.Discard, conn)
	}()

	broker := sarama.NewBroker(addr)
	err = broker.Open(sarama.NewConfig())
	require.Nil(t, err)

	return broker, func() {
		_ = broker.Close()
		_ = listener.Close()
	}
}
