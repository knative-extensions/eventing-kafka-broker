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
	"testing"

	"github.com/IBM/sarama"
	"github.com/google/go-cmp/cmp"
	"k8s.io/apimachinery/pkg/util/sets"
)

var _ sarama.ClusterAdmin = &MockKafkaClusterAdmin{}

const (
	ErrorOnDeleteConsumerGroupTestKey = "error-on-delete-consumer-group"
)

type MockKafkaClusterAdmin struct {
	// (Create|Delete)Topic
	ExpectedTopicName string

	// CreateTopic
	ExpectedTopicDetail sarama.TopicDetail
	ErrorOnCreateTopic  error

	// DeleteTopic
	ErrorOnDeleteTopic error

	ExpectedClose      bool
	ExpectedCloseError error

	// DescribeTopics
	ExpectedTopics                         []string
	ExpectedErrorOnDescribeTopics          error
	ExpectedTopicsMetadataOnDescribeTopics []*sarama.TopicMetadata

	// DescribeConsumerGroups
	ExpectedConsumerGroups                           []string
	ExpectedErrorOnDescribeConsumerGroups            error
	ExpectedGroupDescriptionOnDescribeConsumerGroups []*sarama.GroupDescription

	ErrorOnDeleteConsumerGroup error

	T *testing.T
}

func (m *MockKafkaClusterAdmin) CreateACLs(acls []*sarama.ResourceAcls) error {
	panic("implement me")
}

func (m *MockKafkaClusterAdmin) DeleteConsumerGroupOffset(group string, topic string, partition int32) error {
	panic("implement me")
}

func (m *MockKafkaClusterAdmin) DescribeClientQuotas(components []sarama.QuotaFilterComponent, strict bool) ([]sarama.DescribeClientQuotasEntry, error) {
	panic("implement me")
}

func (m *MockKafkaClusterAdmin) AlterClientQuotas(entity []sarama.QuotaEntityComponent, op sarama.ClientQuotasOp, validateOnly bool) error {
	panic("implement me")
}

func (m *MockKafkaClusterAdmin) Controller() (*sarama.Broker, error) {
	panic("implement me")
}

func (m *MockKafkaClusterAdmin) DescribeUserScramCredentials(users []string) ([]*sarama.DescribeUserScramCredentialsResult, error) {
	return nil, nil
}

func (m *MockKafkaClusterAdmin) DeleteUserScramCredentials(delete []sarama.AlterUserScramCredentialsDelete) ([]*sarama.AlterUserScramCredentialsResult, error) {
	return nil, nil
}

func (m *MockKafkaClusterAdmin) UpsertUserScramCredentials(upsert []sarama.AlterUserScramCredentialsUpsert) ([]*sarama.AlterUserScramCredentialsResult, error) {
	return nil, nil
}

func (m *MockKafkaClusterAdmin) CreateTopic(topic string, detail *sarama.TopicDetail, validateOnly bool) error {
	if topic != m.ExpectedTopicName {
		m.T.Errorf("expected topic %s got %s", m.ExpectedTopicName, topic)
	}

	if diff := cmp.Diff(*detail, m.ExpectedTopicDetail); diff != "" {
		m.T.Errorf("unexpected topic detail (-want +got) %s", diff)
	}

	return m.ErrorOnCreateTopic
}

func (m *MockKafkaClusterAdmin) ListTopics() (map[string]sarama.TopicDetail, error) {
	panic("implement me")
}

func (m *MockKafkaClusterAdmin) DescribeTopics(topics []string) (metadata []*sarama.TopicMetadata, err error) {

	if !sets.NewString(m.ExpectedTopics...).HasAll(topics...) {
		m.T.Errorf("unexpected topics %v, expected %v", topics, m.ExpectedTopics)
	}

	return m.ExpectedTopicsMetadataOnDescribeTopics, m.ExpectedErrorOnDescribeTopics
}

func (m *MockKafkaClusterAdmin) DeleteTopic(topic string) error {
	if topic != m.ExpectedTopicName {
		m.T.Errorf("expected topic %s got %s", m.ExpectedTopicName, topic)
	}

	return m.ErrorOnDeleteTopic
}

func (m *MockKafkaClusterAdmin) CreatePartitions(topic string, count int32, assignment [][]int32, validateOnly bool) error {
	panic("implement me")
}

func (m *MockKafkaClusterAdmin) AlterPartitionReassignments(topic string, assignment [][]int32) error {
	panic("implement me")
}

func (m *MockKafkaClusterAdmin) ListPartitionReassignments(topics string, partitions []int32) (topicStatus map[string]map[int32]*sarama.PartitionReplicaReassignmentsStatus, err error) {
	panic("implement me")
}

func (m *MockKafkaClusterAdmin) DeleteRecords(topic string, partitionOffsets map[int32]int64) error {
	panic("implement me")
}

func (m *MockKafkaClusterAdmin) DescribeConfig(resource sarama.ConfigResource) ([]sarama.ConfigEntry, error) {
	panic("implement me")
}

func (m *MockKafkaClusterAdmin) AlterConfig(resourceType sarama.ConfigResourceType, name string, entries map[string]*string, validateOnly bool) error {
	panic("implement me")
}

func (m *MockKafkaClusterAdmin) IncrementalAlterConfig(resourceType sarama.ConfigResourceType, name string, entries map[string]sarama.IncrementalAlterConfigsEntry, validateOnly bool) error {
	panic("implement me")
}

func (m *MockKafkaClusterAdmin) CreateACL(resource sarama.Resource, acl sarama.Acl) error {
	panic("implement me")
}

func (m *MockKafkaClusterAdmin) ListAcls(filter sarama.AclFilter) ([]sarama.ResourceAcls, error) {
	panic("implement me")
}

func (m *MockKafkaClusterAdmin) DeleteACL(filter sarama.AclFilter, validateOnly bool) ([]sarama.MatchingAcl, error) {
	panic("implement me")
}

func (m *MockKafkaClusterAdmin) ListConsumerGroups() (map[string]string, error) {
	panic("implement me")
}

func (m *MockKafkaClusterAdmin) DescribeConsumerGroups(groups []string) ([]*sarama.GroupDescription, error) {
	if !sets.NewString(m.ExpectedConsumerGroups...).HasAll(groups...) {
		m.T.Errorf("unexpected consumer groups %v, expected %v", groups, m.ExpectedConsumerGroups)
	}

	return m.ExpectedGroupDescriptionOnDescribeConsumerGroups, m.ExpectedErrorOnDescribeTopics
}

func (m *MockKafkaClusterAdmin) ListConsumerGroupOffsets(group string, topicPartitions map[string][]int32) (*sarama.OffsetFetchResponse, error) {
	panic("implement me")
}

func (m *MockKafkaClusterAdmin) DeleteConsumerGroup(group string) error {
	return m.ErrorOnDeleteConsumerGroup
}

func (m *MockKafkaClusterAdmin) DescribeCluster() (brokers []*sarama.Broker, controllerID int32, err error) {
	panic("implement me")
}

func (m *MockKafkaClusterAdmin) DescribeLogDirs(brokers []int32) (map[int32][]sarama.DescribeLogDirsResponseDirMetadata, error) {
	panic("implement me")
}

func (m *MockKafkaClusterAdmin) RemoveMemberFromConsumerGroup(groupId string, groupInstanceIds []string) (*sarama.LeaveGroupResponse, error) {
	panic("implement me")
}

func (m *MockKafkaClusterAdmin) Close() error {
	m.ExpectedClose = true
	return m.ExpectedCloseError
}
