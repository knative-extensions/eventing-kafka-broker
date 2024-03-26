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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/utils/ptr"
	kafkatesting "knative.dev/eventing-kafka-broker/control-plane/pkg/kafka/testing"
)

func setupTestClient(counter *int) *client {
	mockClient := kafkatesting.MockKafkaClient{ShouldFailBrokenPipe: true}
	return &client{
		client: &mockClient,
		isFatalError: func(err error) bool {
			return err != nil && (strings.Contains(err.Error(), "broken pipe") || strings.Contains(err.Error(), "metadata is out of date"))
		},
		onFatalError: func(err error) {
			*counter += 1
		},
	}
}

// TestProxyClientHandlesFatalErrors verifies that the cluster admin proxy identifies
// and runs the provided function when it encounters a fatal error
func TestProxyClientHandlesFatalErrors(t *testing.T) {
	errorCount := ptr.To(0)

	c := setupTestClient(errorCount)

	c.Controller()
	assert.Equal(t, 1, *errorCount)

	c.RefreshController()
	assert.Equal(t, 2, *errorCount)

	c.Broker(0)
	assert.Equal(t, 3, *errorCount)

	c.Topics()
	assert.Equal(t, 4, *errorCount)

	c.Partitions("")
	assert.Equal(t, 5, *errorCount)

	c.WritablePartitions("")
	assert.Equal(t, 6, *errorCount)

	c.Leader("", 0)
	assert.Equal(t, 7, *errorCount)

	c.LeaderAndEpoch("", 0)
	assert.Equal(t, 8, *errorCount)

	c.Replicas("", 0)
	assert.Equal(t, 9, *errorCount)

	c.InSyncReplicas("", 0)
	assert.Equal(t, 10, *errorCount)

	c.OfflineReplicas("", 0)
	assert.Equal(t, 11, *errorCount)

	c.RefreshBrokers([]string{})
	assert.Equal(t, 12, *errorCount)

	c.RefreshMetadata()
	assert.Equal(t, 13, *errorCount)

	c.GetOffset("", 0, 0)
	assert.Equal(t, 14, *errorCount)

	c.Coordinator("")
	assert.Equal(t, 15, *errorCount)

	c.RefreshCoordinator("")
	assert.Equal(t, 16, *errorCount)

	c.TransactionCoordinator("")
	assert.Equal(t, 17, *errorCount)

	c.RefreshTransactionCoordinator("")
	assert.Equal(t, 18, *errorCount)

	c.InitProducerID()
	assert.Equal(t, 19, *errorCount)
}
