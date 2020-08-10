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
	"strings"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"knative.dev/pkg/configmap"
)

type Config struct {
	TopicDetail      sarama.TopicDetail
	BootstrapServers []string
}

func configFromConfigMap(logger *zap.Logger, cm *corev1.ConfigMap) (*Config, error) {

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

	if topicDetail.NumPartitions <= 0 || replicationFactor <= 0 || bootstrapServers == "" {
		return nil, fmt.Errorf(
			"invalid configuration - numPartitions: %d - replicationFactor: %d - bootstrapServers: %s",
			topicDetail.NumPartitions,
			replicationFactor,
			bootstrapServers)
	}

	topicDetail.ReplicationFactor = int16(replicationFactor)

	config := &Config{
		TopicDetail:      topicDetail,
		BootstrapServers: bootstrapServersArray(bootstrapServers),
	}

	logger.Debug("got broker config from config map", zap.Any("config", config))

	return config, nil
}

func (c Config) getBootstrapServers() string {
	return strings.Join(c.BootstrapServers, ",")
}
