/*
 * Copyright 2023 The Knative Authors
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

package clientpool

import (
	"context"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	kafkatesting "knative.dev/eventing-kafka-broker/control-plane/pkg/kafka/testing"
)

func TestGetClient(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)

	cache := NewLRUCache[clientKey, sarama.Client](5*time.Minute, 30*time.Minute)

	clients := &clientPool{
		CachePool: cache,
		newSaramaClient: func(_ []string, _ *sarama.Config) (sarama.Client, error) {
			return &kafkatesting.MockKafkaClient{}, nil
		},
		newClusterAdminFromClient: func(_ sarama.Client) (sarama.ClusterAdmin, error) {
			return &kafkatesting.MockKafkaClusterAdmin{ExpectedTopics: []string{"topic1"}}, nil
		},
		capacityPerClient: 2,
	}

	client1, returnClient1, err := clients.get(ctx, []string{"localhost:9092"}, nil)

	assert.NoError(t, err)

	controller, err := client1.Controller()
	assert.NoError(t, err)
	assert.NotNil(t, controller)

	client2, returnClient2, err := clients.get(ctx, []string{"localhost:9092"}, nil)

	assert.NoError(t, err)

	controller, err = client2.Controller()
	assert.NoError(t, err)
	assert.NotNil(t, controller)

	shortContext, cancelShortContext := context.WithTimeout(context.Background(), time.Millisecond*200)

	client3, returnClient3, err := clients.get(shortContext, []string{"localhost:9092"}, nil)
	returnClient3()
	cancelShortContext()

	assert.Error(t, err)
	assert.Nil(t, client3)

	returnClient1()

	client3, returnClient3, err = clients.get(ctx, []string{"localhost:9092"}, nil)
	assert.NoError(t, err)

	controller, err = client3.Controller()
	assert.NoError(t, err)
	assert.NotNil(t, controller)

	returnClient2()
	returnClient3()

	cancel()
}

func TestGetClusterAdmin(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)

	cache := NewLRUCache[clientKey, sarama.Client](5*time.Minute, 30*time.Minute)

	clients := &clientPool{
		CachePool: cache,
		newSaramaClient: func(_ []string, _ *sarama.Config) (sarama.Client, error) {
			return &kafkatesting.MockKafkaClient{}, nil
		},
		newClusterAdminFromClient: func(_ sarama.Client) (sarama.ClusterAdmin, error) {
			return &kafkatesting.MockKafkaClusterAdmin{ExpectedTopics: []string{"topic1"}}, nil
		},
		capacityPerClient: 2,
	}

	client1, returnClient1, err := clients.getClusterAdmin(ctx, []string{"localhost:9092"}, nil)

	assert.NoError(t, err)

	topics, err := client1.ListTopics()
	assert.NoError(t, err)
	assert.Contains(t, topics, "topic1")

	client2, returnClient2, err := clients.getClusterAdmin(ctx, []string{"localhost:9092"}, nil)

	assert.NoError(t, err)

	topics, err = client2.ListTopics()
	assert.NoError(t, err)
	assert.Contains(t, topics, "topic1")

	shortContext, cancelShortContext := context.WithTimeout(context.Background(), time.Millisecond*200)

	client3, returnClient3, err := clients.getClusterAdmin(shortContext, []string{"localhost:9092"}, nil)
	returnClient3()
	cancelShortContext()

	assert.Error(t, err)
	assert.Nil(t, client3)

	returnClient1()

	client3, returnClient3, err = clients.getClusterAdmin(ctx, []string{"localhost:9092"}, nil)
	assert.NoError(t, err)

	topics, err = client3.ListTopics()
	assert.NoError(t, err)
	assert.Contains(t, topics, "topic1")

	returnClient2()
	returnClient3()

	cancel()
}
