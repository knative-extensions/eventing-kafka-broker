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
	"testing"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"k8s.io/utils/ptr"
	kafkatesting "knative.dev/eventing-kafka-broker/control-plane/pkg/kafka/testing"
)

func makeMockClusterAdminFromClient(_ sarama.Client) (sarama.ClusterAdmin, error) {
	return &kafkatesting.MockKafkaClusterAdmin{ErrorBrokenPipe: true}, nil
}

// TestProxyClusterAdminHandlesFatalErrors verifies that the cluster admin proxy identifies
// and runs the provided function when it encounters a fatal error
func TestProxyClusterAdminHandlesFatalErrors(t *testing.T) {
	errorCount := ptr.To(0)

	c := setupTestClient(errorCount)
	a, err := clusterAdminFromClient(c, makeMockClusterAdminFromClient)
	assert.NoError(t, err)

	a.CreateTopic("", nil, false)
	assert.Equal(t, 1, *errorCount)

	a.ListTopics()
	assert.Equal(t, 2, *errorCount)

	a.DescribeTopics([]string{})
	assert.Equal(t, 3, *errorCount)

	a.DeleteTopic("")
	assert.Equal(t, 4, *errorCount)

	a.CreatePartitions("", 0, nil, false)
	assert.Equal(t, 5, *errorCount)

	a.AlterPartitionReassignments("", nil)
	assert.Equal(t, 6, *errorCount)

	a.ListPartitionReassignments("", nil)
	assert.Equal(t, 7, *errorCount)

	a.DeleteRecords("", nil)
	assert.Equal(t, 8, *errorCount)

	a.DescribeConfig(sarama.ConfigResource{})
	assert.Equal(t, 9, *errorCount)

	a.AlterConfig(0, "", nil, false)
	assert.Equal(t, 10, *errorCount)

	a.IncrementalAlterConfig(0, "", nil, false)
	assert.Equal(t, 11, *errorCount)

	a.CreateACL(sarama.Resource{}, sarama.Acl{})
	assert.Equal(t, 12, *errorCount)

	a.CreateACLs(nil)
	assert.Equal(t, 13, *errorCount)

	a.ListAcls(sarama.AclFilter{})
	assert.Equal(t, 14, *errorCount)

	a.DeleteACL(sarama.AclFilter{}, false)
	assert.Equal(t, 15, *errorCount)

	a.ListConsumerGroups()
	assert.Equal(t, 16, *errorCount)

	a.DescribeConsumerGroups(nil)
	assert.Equal(t, 17, *errorCount)

	a.ListConsumerGroupOffsets("", nil)
	assert.Equal(t, 18, *errorCount)

	a.DeleteConsumerGroupOffset("", "", 0)
	assert.Equal(t, 19, *errorCount)

	a.DeleteConsumerGroup("")
	assert.Equal(t, 20, *errorCount)

	a.DescribeCluster()
	assert.Equal(t, 21, *errorCount)

	a.DescribeLogDirs(nil)
	assert.Equal(t, 22, *errorCount)

	a.DescribeUserScramCredentials(nil)
	assert.Equal(t, 23, *errorCount)

	a.DeleteUserScramCredentials(nil)
	assert.Equal(t, 24, *errorCount)

	a.UpsertUserScramCredentials(nil)
	assert.Equal(t, 25, *errorCount)

	a.DescribeClientQuotas(nil, false)
	assert.Equal(t, 26, *errorCount)

	a.AlterClientQuotas(nil, sarama.ClientQuotasOp{}, false)
	assert.Equal(t, 27, *errorCount)

	a.Controller()
	assert.Equal(t, 28, *errorCount)

	a.RemoveMemberFromConsumerGroup("", nil)
	assert.Equal(t, 29, *errorCount)
}
