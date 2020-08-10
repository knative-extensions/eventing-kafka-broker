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

package broker

import (
	"fmt"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1beta1"
)

func (r *Reconciler) CreateTopic(logger *zap.Logger, topic string, config *Config) (string, error) {

	kafkaClusterAdmin, err := r.getKafkaClusterAdmin(config.BootstrapServers)
	if err != nil {
		return "", err
	}
	defer kafkaClusterAdmin.Close()

	topicDetail := &sarama.TopicDetail{
		NumPartitions:     config.TopicDetail.NumPartitions,
		ReplicationFactor: config.TopicDetail.ReplicationFactor,
	}

	logger.Debug("create topic",
		zap.String("topic", topic),
		zap.Int16("replicationFactor", topicDetail.ReplicationFactor),
		zap.Int32("numPartitions", topicDetail.NumPartitions),
	)

	createTopicError := kafkaClusterAdmin.CreateTopic(topic, topicDetail, false)
	if err, ok := createTopicError.(*sarama.TopicError); ok && err.Err == sarama.ErrTopicAlreadyExists {
		return topic, nil
	}

	return topic, createTopicError
}

func (r *Reconciler) deleteTopic(topic string, bootstrapServers []string) (string, error) {
	kafkaClusterAdmin, err := r.getKafkaClusterAdmin(bootstrapServers)
	if err != nil {
		return "", err
	}
	defer kafkaClusterAdmin.Close()

	err = kafkaClusterAdmin.DeleteTopic(topic)
	if sarama.ErrUnknownTopicOrPartition == err {
		return topic, nil
	}
	if err != nil {
		return topic, err
	}

	return topic, nil
}

func Topic(broker *eventing.Broker) string {
	return fmt.Sprintf("%s%s-%s", TopicPrefix, broker.Namespace, broker.Name)
}
