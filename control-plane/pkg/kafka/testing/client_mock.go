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

package testing

import (
	"fmt"

	"github.com/IBM/sarama"
)

type MockKafkaClient struct {
	CloseError                error
	IsClosed                  bool
	ShouldFailRefreshMetadata bool
	ShouldFailRefreshBrokers  bool
	ShouldFailBrokenPipe      bool
	OnClose                   func()
}

var _ sarama.Client = &MockKafkaClient{}

type brokenPipeError struct{}

func (brokenPipeError) Error() string {
	return "write: broken pipe"
}

func (m MockKafkaClient) Config() *sarama.Config {
	return sarama.NewConfig()
}

func (m MockKafkaClient) Controller() (*sarama.Broker, error) {
	if m.ShouldFailBrokenPipe {
		return nil, brokenPipeError{}
	}

	return &sarama.Broker{}, nil
}

func (m MockKafkaClient) RefreshController() (*sarama.Broker, error) {
	if m.ShouldFailBrokenPipe {
		return nil, brokenPipeError{}
	}
	panic("implement me")
}

func (m MockKafkaClient) Brokers() []*sarama.Broker {
	panic("implement me")
}

func (m MockKafkaClient) Broker(brokerID int32) (*sarama.Broker, error) {
	if m.ShouldFailBrokenPipe {
		return nil, brokenPipeError{}
	}
	panic("implement me")
}

func (m MockKafkaClient) Topics() ([]string, error) {
	if m.ShouldFailBrokenPipe {
		return nil, brokenPipeError{}
	}
	panic("implement me")
}

func (m MockKafkaClient) Partitions(topic string) ([]int32, error) {
	if m.ShouldFailBrokenPipe {
		return nil, brokenPipeError{}
	}
	panic("implement me")
}

func (m MockKafkaClient) WritablePartitions(topic string) ([]int32, error) {
	if m.ShouldFailBrokenPipe {
		return nil, brokenPipeError{}
	}
	panic("implement me")
}

func (m MockKafkaClient) Leader(topic string, partitionID int32) (*sarama.Broker, error) {
	if m.ShouldFailBrokenPipe {
		return nil, brokenPipeError{}
	}
	panic("implement me")
}

func (m MockKafkaClient) LeaderAndEpoch(topic string, partitionID int32) (*sarama.Broker, int32, error) {
	if m.ShouldFailBrokenPipe {
		return nil, 0, brokenPipeError{}
	}
	panic("implement me")
}

func (m MockKafkaClient) Replicas(topic string, partitionID int32) ([]int32, error) {
	if m.ShouldFailBrokenPipe {
		return nil, brokenPipeError{}
	}
	panic("implement me")
}

func (m MockKafkaClient) InSyncReplicas(topic string, partitionID int32) ([]int32, error) {
	if m.ShouldFailBrokenPipe {
		return nil, brokenPipeError{}
	}
	panic("implement me")
}

func (m MockKafkaClient) OfflineReplicas(topic string, partitionID int32) ([]int32, error) {
	if m.ShouldFailBrokenPipe {
		return nil, brokenPipeError{}
	}
	panic("implement me")
}

func (m MockKafkaClient) RefreshBrokers(addrs []string) error {
	if m.ShouldFailBrokenPipe {
		return brokenPipeError{}
	}
	if len(addrs) == 0 {
		return fmt.Errorf("provide at least one broker to refresh them")
	}
	if m.ShouldFailRefreshBrokers {
		return fmt.Errorf("failed to refresh brokers")
	}
	return nil
}

func (m MockKafkaClient) RefreshMetadata(topics ...string) error {
	if m.ShouldFailBrokenPipe {
		return brokenPipeError{}
	}
	if m.ShouldFailRefreshMetadata {
		return fmt.Errorf("failed to refresh metadata")
	}
	return nil
}

func (m MockKafkaClient) GetOffset(topic string, partitionID int32, time int64) (int64, error) {
	if m.ShouldFailBrokenPipe {
		return 0, brokenPipeError{}
	}
	panic("implement me")
}

func (m MockKafkaClient) Coordinator(consumerGroup string) (*sarama.Broker, error) {
	if m.ShouldFailBrokenPipe {
		return nil, brokenPipeError{}
	}
	panic("implement me")
}

func (m MockKafkaClient) RefreshCoordinator(consumerGroup string) error {
	if m.ShouldFailBrokenPipe {
		return brokenPipeError{}
	}
	panic("implement me")
}

func (m MockKafkaClient) TransactionCoordinator(transactionID string) (*sarama.Broker, error) {
	if m.ShouldFailBrokenPipe {
		return nil, brokenPipeError{}
	}
	panic("implement me")
}

func (m MockKafkaClient) RefreshTransactionCoordinator(transactionID string) error {
	if m.ShouldFailBrokenPipe {
		return brokenPipeError{}
	}
	panic("implement me")
}

func (m MockKafkaClient) InitProducerID() (*sarama.InitProducerIDResponse, error) {
	if m.ShouldFailBrokenPipe {
		return nil, brokenPipeError{}
	}
	panic("implement me")
}

func (m MockKafkaClient) LeastLoadedBroker() *sarama.Broker {
	panic("implement me")
}

func (m *MockKafkaClient) Close() error {
	m.IsClosed = true
	if m.OnClose != nil {
		m.OnClose()
	}
	return m.CloseError
}

func (m MockKafkaClient) Closed() bool {
	return m.IsClosed
}
