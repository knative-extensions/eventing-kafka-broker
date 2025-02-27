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
	"log"
	"os"

	"github.com/IBM/sarama"
	"github.com/rcrowley/go-metrics"
)

var configVersion sarama.KafkaVersion = sarama.DefaultVersion

func init() {
	// Disable Sarama metrics
	metrics.UseNilMetrics = true

	configVersion = getSaramaConfigVersion()
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

// NewClusterAdminFromClientFunc creates new sarama.ClusterAdmin from sarama.Client
type NewClusterAdminFromClientFunc func(sarama.Client) (sarama.ClusterAdmin, error)

func getSaramaConfigVersion() sarama.KafkaVersion {
	saramaVersion := sarama.DefaultVersion
	version, exists := os.LookupEnv("SARAMA_CONFIG_VERSION")
	if exists {
		for _, v := range sarama.SupportedVersions {
			if v.String() == version {
				saramaVersion = v
			}
		}
	}
	log.Printf("sarama version set to %s", saramaVersion.String())
	return saramaVersion
}

// GetSaramaConfig returns Kafka Client configuration with the given options applied.
func GetSaramaConfig(configOptions ...ConfigOption) (*sarama.Config, error) {
	config := sarama.NewConfig()
	config.Version = configVersion

	err := Options(config, configOptions...)
	if err != nil {
		return nil, err
	}

	return config, nil
}
