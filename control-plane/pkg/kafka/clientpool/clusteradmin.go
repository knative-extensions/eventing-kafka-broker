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
	"fmt"

	"github.com/IBM/sarama"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
)

// clusterAdmin is a proxy for sarama.ClusterAdmin
//
// It keeps track of callers that are actively using the cluster admin using incrementCallers and Close()
type clusterAdmin struct {
	client       *client
	clusterAdmin sarama.ClusterAdmin

	isFatalError func(err error) bool
	onFatalError func(err error)
}

func clusterAdminFromClient(saramaClient sarama.Client, makeClusterAdmin kafka.NewClusterAdminFromClientFunc) (*clusterAdmin, error) {
	c, ok := saramaClient.(*client)
	if !ok {
		return nil, fmt.Errorf("failed to make clusterAdmin from client")
	}

	ca, err := makeClusterAdmin(c)
	if err != nil {
		return nil, err
	}

	return &clusterAdmin{
		client:       c,
		clusterAdmin: ca,
		isFatalError: c.isFatalError,
		onFatalError: c.onFatalError,
	}, nil
}

var _ sarama.ClusterAdmin = &clusterAdmin{}

func (a *clusterAdmin) CreateTopic(topic string, detail *sarama.TopicDetail, validateOnly bool) error {
	err := a.clusterAdmin.CreateTopic(topic, detail, validateOnly)
	if a.isFatalError(err) {
		a.onFatalError(err)
	}
	return err
}

func (a *clusterAdmin) ListTopics() (map[string]sarama.TopicDetail, error) {
	x, err := a.clusterAdmin.ListTopics()
	if a.isFatalError(err) {
		a.onFatalError(err)
	}
	return x, err
}

func (a *clusterAdmin) DescribeTopics(topics []string) (metadata []*sarama.TopicMetadata, err error) {
	x, err := a.clusterAdmin.DescribeTopics(topics)
	if a.isFatalError(err) {
		a.onFatalError(err)
	}
	return x, err
}

func (a *clusterAdmin) DeleteTopic(topic string) error {
	err := a.clusterAdmin.DeleteTopic(topic)
	if a.isFatalError(err) {
		a.onFatalError(err)
	}
	return err
}

func (a *clusterAdmin) CreatePartitions(topic string, count int32, assignment [][]int32, validateOnly bool) error {
	err := a.clusterAdmin.CreatePartitions(topic, count, assignment, validateOnly)
	if a.isFatalError(err) {
		a.onFatalError(err)
	}
	return err
}

func (a *clusterAdmin) AlterPartitionReassignments(topic string, assignment [][]int32) error {
	err := a.clusterAdmin.AlterPartitionReassignments(topic, assignment)
	if a.isFatalError(err) {
		a.onFatalError(err)
	}
	return err
}

func (a *clusterAdmin) ListPartitionReassignments(topics string, partitions []int32) (topicStatus map[string]map[int32]*sarama.PartitionReplicaReassignmentsStatus, err error) {
	x, err := a.clusterAdmin.ListPartitionReassignments(topics, partitions)
	if a.isFatalError(err) {
		a.onFatalError(err)
	}
	return x, err
}

func (a *clusterAdmin) DeleteRecords(topic string, partitionOffsets map[int32]int64) error {
	err := a.clusterAdmin.DeleteRecords(topic, partitionOffsets)
	if a.isFatalError(err) {
		a.onFatalError(err)
	}
	return err
}

func (a *clusterAdmin) DescribeConfig(resource sarama.ConfigResource) ([]sarama.ConfigEntry, error) {
	x, err := a.clusterAdmin.DescribeConfig(resource)
	if a.isFatalError(err) {
		a.onFatalError(err)
	}
	return x, err
}

func (a *clusterAdmin) AlterConfig(resourceType sarama.ConfigResourceType, name string, entries map[string]*string, validateOnly bool) error {
	err := a.clusterAdmin.AlterConfig(resourceType, name, entries, validateOnly)
	if a.isFatalError(err) {
		a.onFatalError(err)
	}
	return err
}

func (a *clusterAdmin) IncrementalAlterConfig(resourceType sarama.ConfigResourceType, name string, entries map[string]sarama.IncrementalAlterConfigsEntry, validateOnly bool) error {
	err := a.clusterAdmin.IncrementalAlterConfig(resourceType, name, entries, validateOnly)
	if a.isFatalError(err) {
		a.onFatalError(err)
	}
	return err
}

func (a *clusterAdmin) CreateACL(resource sarama.Resource, acl sarama.Acl) error {
	err := a.clusterAdmin.CreateACL(resource, acl)
	if a.isFatalError(err) {
		a.onFatalError(err)
	}
	return err
}

func (a *clusterAdmin) CreateACLs(resources []*sarama.ResourceAcls) error {
	err := a.clusterAdmin.CreateACLs(resources)
	if a.isFatalError(err) {
		a.onFatalError(err)
	}
	return err
}

func (a *clusterAdmin) ListAcls(filter sarama.AclFilter) ([]sarama.ResourceAcls, error) {
	x, err := a.clusterAdmin.ListAcls(filter)
	if a.isFatalError(err) {
		a.onFatalError(err)
	}
	return x, err
}

func (a *clusterAdmin) DeleteACL(filter sarama.AclFilter, validateOnly bool) ([]sarama.MatchingAcl, error) {
	x, err := a.clusterAdmin.DeleteACL(filter, validateOnly)
	if a.isFatalError(err) {
		a.onFatalError(err)
	}
	return x, err
}

func (a *clusterAdmin) ListConsumerGroups() (map[string]string, error) {
	x, err := a.clusterAdmin.ListConsumerGroups()
	if a.isFatalError(err) {
		a.onFatalError(err)
	}
	return x, err
}

func (a *clusterAdmin) DescribeConsumerGroups(groups []string) ([]*sarama.GroupDescription, error) {
	x, err := a.clusterAdmin.DescribeConsumerGroups(groups)
	if a.isFatalError(err) {
		a.onFatalError(err)
	}
	return x, err
}

func (a *clusterAdmin) ListConsumerGroupOffsets(group string, topicPartitions map[string][]int32) (*sarama.OffsetFetchResponse, error) {
	x, err := a.clusterAdmin.ListConsumerGroupOffsets(group, topicPartitions)
	if a.isFatalError(err) {
		a.onFatalError(err)
	}
	return x, err
}

func (a *clusterAdmin) DeleteConsumerGroupOffset(group string, topic string, partition int32) error {
	err := a.clusterAdmin.DeleteConsumerGroupOffset(group, topic, partition)
	if a.isFatalError(err) {
		a.onFatalError(err)
	}
	return err
}

func (a *clusterAdmin) DeleteConsumerGroup(group string) error {
	err := a.clusterAdmin.DeleteConsumerGroup(group)
	if a.isFatalError(err) {
		a.onFatalError(err)
	}
	return err
}

func (a *clusterAdmin) DescribeCluster() (brokers []*sarama.Broker, controllerID int32, err error) {
	x, y, err := a.clusterAdmin.DescribeCluster()
	if a.isFatalError(err) {
		a.onFatalError(err)
	}
	return x, y, err
}

func (a *clusterAdmin) DescribeLogDirs(brokers []int32) (map[int32][]sarama.DescribeLogDirsResponseDirMetadata, error) {
	x, err := a.clusterAdmin.DescribeLogDirs(brokers)
	if a.isFatalError(err) {
		a.onFatalError(err)
	}
	return x, err
}

func (a *clusterAdmin) DescribeUserScramCredentials(users []string) ([]*sarama.DescribeUserScramCredentialsResult, error) {
	x, err := a.clusterAdmin.DescribeUserScramCredentials(users)
	if a.isFatalError(err) {
		a.onFatalError(err)
	}
	return x, err
}

func (a *clusterAdmin) DeleteUserScramCredentials(delete []sarama.AlterUserScramCredentialsDelete) ([]*sarama.AlterUserScramCredentialsResult, error) {
	x, err := a.clusterAdmin.DeleteUserScramCredentials(delete)
	if a.isFatalError(err) {
		a.onFatalError(err)
	}
	return x, err
}

func (a *clusterAdmin) UpsertUserScramCredentials(upsert []sarama.AlterUserScramCredentialsUpsert) ([]*sarama.AlterUserScramCredentialsResult, error) {
	x, err := a.clusterAdmin.UpsertUserScramCredentials(upsert)
	if a.isFatalError(err) {
		a.onFatalError(err)
	}
	return x, err
}

func (a *clusterAdmin) DescribeClientQuotas(components []sarama.QuotaFilterComponent, strict bool) ([]sarama.DescribeClientQuotasEntry, error) {
	x, err := a.clusterAdmin.DescribeClientQuotas(components, strict)
	if a.isFatalError(err) {
		a.onFatalError(err)
	}
	return x, err
}

func (a *clusterAdmin) AlterClientQuotas(entity []sarama.QuotaEntityComponent, op sarama.ClientQuotasOp, validateOnly bool) error {
	err := a.clusterAdmin.AlterClientQuotas(entity, op, validateOnly)
	if a.isFatalError(err) {
		a.onFatalError(err)
	}
	return err
}

func (a *clusterAdmin) Controller() (*sarama.Broker, error) {
	x, err := a.clusterAdmin.Controller()
	if a.isFatalError(err) {
		a.onFatalError(err)
	}
	return x, err
}

func (a *clusterAdmin) RemoveMemberFromConsumerGroup(groupId string, groupInstanceIds []string) (*sarama.LeaveGroupResponse, error) {
	x, err := a.clusterAdmin.RemoveMemberFromConsumerGroup(groupId, groupInstanceIds)
	if a.isFatalError(err) {
		a.onFatalError(err)
	}
	return x, err
}

func (a *clusterAdmin) Close() error {
	return a.clusterAdmin.Close()
}
