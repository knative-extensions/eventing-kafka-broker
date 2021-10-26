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
	"github.com/Shopify/sarama"
)

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

// NewClusterAdminFunc creates new sarama.ClusterAdmin.
type NewClusterAdminFunc func(addrs []string, config *sarama.Config) (sarama.ClusterAdmin, error)

// GetClusterAdminSaramaConfig returns Kafka Admin configurations that the ConfigOptions are applied to
func GetClusterAdminSaramaConfig(configOption ConfigOption) (*sarama.Config, error) {
	config := sarama.NewConfig()
	config.Version = sarama.MaxVersion

	err := configOption(config)
	if err != nil {
		return nil, err
	}

	return config, nil
}

// NewClientFunc creates new sarama.Client.
type NewClientFunc func(addrs []string, config *sarama.Config) (sarama.Client, error)

// TODO unify once the hardcoded config below is converted to an option

// GetClientSaramaConfig returns Kafka Client configurations.
func GetClientSaramaConfig(configOption ConfigOption) (*sarama.Config, error) {
	config := sarama.NewConfig()
	config.Version = sarama.MaxVersion

	err := configOption(config)
	if err != nil {
		return nil, err
	}

	// TODO: convert to option
	// Manually commit the offsets in KafkaChannel controller.
	// That's because we want to make sure we initialize the offsets within the controller
	// before dispatcher actually starts consuming messages.
	config.Consumer.Offsets.AutoCommit.Enable = false

	return config, nil
}
