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

package kafka

import (
	"github.com/IBM/sarama"
	"github.com/rcrowley/go-metrics"
)

func init() {
	// Disable Sarama metrics
	metrics.UseNilMetrics = true
}

type ConfigOption func(config *sarama.Config) error

func Options(config *sarama.Config, options ...ConfigOption) error {
	for _, opt := range options {
		if err := opt(config); err != nil {
			return err
		}
	}
	return nil
}

// NoOpConfigOption is a no-op ConfigOption.
func NoOpConfigOption(*sarama.Config) error {
	return nil
}

func DisableOffsetAutoCommitConfigOption(config *sarama.Config) error {
	config.Consumer.Offsets.AutoCommit.Enable = false
	return nil
}

// NewClusterAdminClientFunc creates new sarama.ClusterAdmin.
type NewClusterAdminClientFunc func(addrs []string, config *sarama.Config) (sarama.ClusterAdmin, error)

// NewClientFunc creates new sarama.Client.
type NewClientFunc func(addrs []string, config *sarama.Config) (sarama.Client, error)

// GetSaramaConfig returns Kafka Client configuration with the given options applied.
func GetSaramaConfig(configOptions ...ConfigOption) (*sarama.Config, error) {
	config := sarama.NewConfig()
	config.Version = sarama.DefaultVersion

	err := Options(config, configOptions...)
	if err != nil {
		return nil, err
	}

	return config, nil
}
