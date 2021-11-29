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

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka"
)

func TopicConfigFromConfigMap(logger *zap.Logger, cm *corev1.ConfigMap) (*kafka.TopicConfig, error) {

	config, err := kafka.TopicConfigFromConfigMap(logger, cm)

	if err != nil {
		return nil, fmt.Errorf("failed to build broker config from configmap %s/%s: %w", cm.Namespace, cm.Name, err)
	}

	if config.TopicDetail.NumPartitions <= 0 || config.TopicDetail.ReplicationFactor <= 0 || len(config.BootstrapServers) == 0 {
		return nil, fmt.Errorf(
			"invalid configuration - numPartitions: %d - replicationFactor: %d - bootstrapServers: %s - ConfigMap data: %v",
			config.TopicDetail.NumPartitions,
			config.TopicDetail.ReplicationFactor,
			config.BootstrapServers,
			cm.Data,
		)
	}

	logger.Debug("got broker config from config map", zap.Any("config", config))

	return config, nil
}
