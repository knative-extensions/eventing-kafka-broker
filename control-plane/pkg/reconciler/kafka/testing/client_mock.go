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
	"github.com/Shopify/sarama"
)

type MockKafkaClient struct {
	CloseError error
	IsClosed   bool
}

var (
	_ sarama.Client = &MockKafkaClient{}
)

func (m MockKafkaClient) Config() *sarama.Config {
	return sarama.NewConfig()
}

func (m MockKafkaClient) Controller() (*sarama.Broker, error) {
	panic("implement me")
}

func (m MockKafkaClient) RefreshController() (*sarama.Broker, error) {
	panic("implement me")
}

func (m MockKafkaClient) Brokers() []*sarama.Broker {
	panic("implement me")
}

func (m MockKafkaClient) Broker(brokerID int32) (*sarama.Broker, error) {
	panic("implement me")
}

func (m MockKafkaClient) Topics() ([]string, error) {
	panic("implement me")
}

func (m MockKafkaClient) Partitions(topic string) ([]int32, error) {
	panic("implement me")
}

func (m MockKafkaClient) WritablePartitions(topic string) ([]int32, error) {
	panic("implement me")
}

func (m MockKafkaClient) Leader(topic string, partitionID int32) (*sarama.Broker, error) {
	panic("implement me")
}

func (m MockKafkaClient) Replicas(topic string, partitionID int32) ([]int32, error) {
	panic("implement me")
}

func (m MockKafkaClient) InSyncReplicas(topic string, partitionID int32) ([]int32, error) {
	panic("implement me")
}

func (m MockKafkaClient) OfflineReplicas(topic string, partitionID int32) ([]int32, error) {
	panic("implement me")
}

func (m MockKafkaClient) RefreshBrokers(addrs []string) error {
	panic("implement me")
}

func (m MockKafkaClient) RefreshMetadata(topics ...string) error {
	panic("implement me")
}

func (m MockKafkaClient) GetOffset(topic string, partitionID int32, time int64) (int64, error) {
	panic("implement me")
}

func (m MockKafkaClient) Coordinator(consumerGroup string) (*sarama.Broker, error) {
	panic("implement me")
}

func (m MockKafkaClient) RefreshCoordinator(consumerGroup string) error {
	panic("implement me")
}

func (m MockKafkaClient) InitProducerID() (*sarama.InitProducerIDResponse, error) {
	panic("implement me")
}

func (m *MockKafkaClient) Close() error {
	m.IsClosed = true
	return m.CloseError
}

func (m MockKafkaClient) Closed() bool {
	return m.IsClosed
}
