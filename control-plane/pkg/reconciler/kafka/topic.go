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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/pkg/configmap"
)

const (
	DefaultTopicNumPartitionConfigMapKey      = "default.topic.partitions"
	DefaultTopicReplicationFactorConfigMapKey = "default.topic.replication.factor"
	BootstrapServersConfigMapKey              = "bootstrap.servers"
)

// TopicConfig contains configurations for creating a topic.
type TopicConfig struct {
	TopicDetail      sarama.TopicDetail
	BootstrapServers []string
}

func TopicConfigFromConfigMap(logger *zap.Logger, cm *corev1.ConfigMap) (*TopicConfig, error) {
	config, err := buildTopicConfigFromConfigMap(cm)
	if err != nil {
		return nil, err
	}

	if err := validateTopicConfig(config); err != nil {
		return nil, fmt.Errorf("error validating topic config from configmap %s - ConfigMap data: %v", err, cm.Data)
	}

	logger.Debug("topic config from configmap",
		zap.Int32("numPartitions", config.TopicDetail.NumPartitions),
		zap.Int16("replicationFactor", config.TopicDetail.ReplicationFactor),
		zap.Any("bootstrapServers", config.BootstrapServers),
	)

	return config, nil
}

func buildTopicConfigFromConfigMap(cm *corev1.ConfigMap) (*TopicConfig, error) {
	topicDetail := sarama.TopicDetail{}

	var replicationFactor int32
	var bootstrapServers string

	err := configmap.Parse(cm.Data,
		configmap.AsInt32(DefaultTopicNumPartitionConfigMapKey, &topicDetail.NumPartitions),
		configmap.AsInt32(DefaultTopicReplicationFactorConfigMapKey, &replicationFactor),
		configmap.AsString(BootstrapServersConfigMapKey, &bootstrapServers),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config map %s/%s: %w", cm.Namespace, cm.Name, err)
	}

	topicDetail.ReplicationFactor = int16(replicationFactor)

	config := &TopicConfig{
		TopicDetail:      topicDetail,
		BootstrapServers: BootstrapServersArray(bootstrapServers),
	}
	return config, nil
}

func validateTopicConfig(config *TopicConfig) error {
	if config.TopicDetail.NumPartitions <= 0 || config.TopicDetail.ReplicationFactor <= 0 || len(config.BootstrapServers) == 0 {
		return fmt.Errorf(
			"invalid configuration - numPartitions: %d - replicationFactor: %d - bootstrapServers: %s",
			config.TopicDetail.NumPartitions,
			config.TopicDetail.ReplicationFactor,
			config.BootstrapServers,
		)
	}
	return nil
}

// GetBootstrapServers returns TopicConfig.BootstrapServers as a comma separated list of bootstrap servers.
func (c TopicConfig) GetBootstrapServers() string {
	return BootstrapServersCommaSeparated(c.BootstrapServers)
}

func BootstrapServersCommaSeparated(bootstrapServers []string) string {
	return strings.Join(bootstrapServers, ",")
}

func BootstrapServersArray(bootstrapServers string) []string {
	bss := strings.Split(bootstrapServers, ",")

	j := len(bss)
	for i := 0; i < j; i++ {
		bss[i] = strings.TrimSpace(bss[i])
		if bss[i] == "" {
			j--
			bss[i] = bss[j]
			i--
		}
	}
	return bss[:j]
}

// BrokerTopic returns a topic name given a topic prefix and a Broker.
func BrokerTopic(prefix string, obj metav1.Object) string {
	return fmt.Sprintf("%s%s-%s", prefix, obj.GetNamespace(), obj.GetName())
}

// ChannelTopic returns a topic name given a topic prefix and a KafkaChannel.
func ChannelTopic(prefix string, obj metav1.Object) string {
	return fmt.Sprintf("%s.%s.%s", prefix, obj.GetNamespace(), obj.GetName())
}

// CreateTopicIfDoesntExist creates a topic with name 'topic' following the TopicConfig configuration passed as parameter.
//
// It returns the topic name or an error.
//
// If the topic already exists, it will return no errors.
// TODO: what happens if the topic exists but it has a different config?
func CreateTopicIfDoesntExist(admin sarama.ClusterAdmin, logger *zap.Logger, topic string, config *TopicConfig) (string, error) {

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

func AreTopicsPresentAndValid(kafkaClusterAdmin sarama.ClusterAdmin, topics ...string) (bool, error) {
	if len(topics) == 0 {
		return false, fmt.Errorf("expected at least one topic, got 0")
	}

	metadata, err := kafkaClusterAdmin.DescribeTopics(topics)
	if err != nil {
		return false, fmt.Errorf("failed to describe topics %v: %w", topics, err)
	}

	metadataByTopic := make(map[string]*sarama.TopicMetadata, len(metadata))
	for _, m := range metadata {
		metadataByTopic[m.Name] = m
	}

	for _, t := range topics {
		m, ok := metadataByTopic[t]
		if !ok || !isValidSingleTopicMetadata(m, t) {
			return false, InvalidOrNotPresentTopic{Topic: t}
		}
	}
	return true, nil
}

func isValidSingleTopicMetadata(metadata *sarama.TopicMetadata, topic string) bool {
	return len(metadata.Partitions) > 0 && metadata.Name == topic && !metadata.IsInternal
}

type InvalidOrNotPresentTopic struct {
	Topic string
}

func (it InvalidOrNotPresentTopic) Error() string {
	return fmt.Sprintf("invalid topic %s", it.Topic)
}
