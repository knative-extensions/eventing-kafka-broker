/*
 * Copyright 2020 The Knative Authors
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
	"strings"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TopicConfig contains configurations for creating a topic.
type TopicConfig struct {
	TopicDetail      sarama.TopicDetail
	BootstrapServers []string
}

// GetBootstrapServers returns TopicConfig.BootstrapServers as a comma separated list of bootstrap servers.
func (c TopicConfig) GetBootstrapServers() string {
	return BootstrapServersCommaSeparated(c.BootstrapServers)
}

func BootstrapServersCommaSeparated(bootstrapServers []string) string {
	return strings.Join(bootstrapServers, ",")
}

func BootstrapServersArray(bootstrapServers string) []string {
	return strings.Split(bootstrapServers, ",")
}

// Topic returns a topic name given a topic prefix and a generic object.
func Topic(prefix string, obj metav1.Object) string {
	return fmt.Sprintf("%s%s-%s", prefix, obj.GetNamespace(), obj.GetName())
}

// CreateTopic creates a topic with name 'topic' following the TopicConfig configuration passed as parameter.
//
// It returns the topic name or an error.
//
// If the topic already exists, it will return no errors.
func CreateTopic(admin sarama.ClusterAdmin, logger *zap.Logger, topic string, config *TopicConfig) (string, error) {

	topicDetail := &sarama.TopicDetail{
		NumPartitions:     config.TopicDetail.NumPartitions,
		ReplicationFactor: config.TopicDetail.ReplicationFactor,
	}

	logger.Debug("create topic",
		zap.String("topic", topic),
		zap.Int16("replicationFactor", topicDetail.ReplicationFactor),
		zap.Int32("numPartitions", topicDetail.NumPartitions),
	)

	createTopicError := admin.CreateTopic(topic, topicDetail, false)
	if err, ok := createTopicError.(*sarama.TopicError); ok && err.Err == sarama.ErrTopicAlreadyExists {
		return topic, nil
	}

	return topic, createTopicError
}

func DeleteTopic(admin sarama.ClusterAdmin, topic string) (string, error) {

	if err := admin.DeleteTopic(topic); err != nil {
		topicErr, ok := err.(*sarama.TopicError)
		if err == sarama.ErrUnknownTopicOrPartition || (ok && topicErr.Err == sarama.ErrUnknownTopicOrPartition) {
			return topic, nil
		}

		return topic, fmt.Errorf("failed to delete topic %s: %w", topic, err)
	}

	return topic, nil
}

func (f NewClusterAdminFunc) CreateTopic(logger *zap.Logger, topic string, config *TopicConfig) (string, error) {

	kafkaClusterAdmin, err := GetClusterAdmin(f, config.BootstrapServers)
	if err != nil {
		return topic, err
	}
	defer kafkaClusterAdmin.Close()

	return CreateTopic(kafkaClusterAdmin, logger, topic, config)
}

func (f NewClusterAdminFunc) DeleteTopic(topic string, bootstrapServers []string) (string, error) {

	kafkaClusterAdmin, err := GetClusterAdmin(f, bootstrapServers)
	if err != nil {
		return topic, err
	}
	defer kafkaClusterAdmin.Close()

	return DeleteTopic(kafkaClusterAdmin, topic)
}

func (f NewClusterAdminFunc) IsTopicPresentAndValid(topic string, bootstrapServers []string) (bool, error) {

	kafkaClusterAdmin, err := GetClusterAdmin(f, bootstrapServers)
	if err != nil {
		return false, err
	}
	defer kafkaClusterAdmin.Close()

	metadata, err := kafkaClusterAdmin.DescribeTopics([]string{topic})
	if err != nil {
		return false, fmt.Errorf("failed to describe topic %s: %w", topic, err)
	}

	return isValidSingleTopicMetadata(metadata, topic), nil
}

func isValidSingleTopicMetadata(metadata []*sarama.TopicMetadata, topic string) bool {
	return len(metadata) == 1 && metadata[0].Name == topic && !metadata[0].IsInternal
}
