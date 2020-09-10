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

	"github.com/Shopify/sarama"
)

// NewClusterAdminFunc creates new sarama.ClusterAdmin.
type NewClusterAdminFunc func(addrs []string, config *sarama.Config) (sarama.ClusterAdmin, error)

// AdminConfig returns Kafka Admin configurations.
func AdminConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Version = sarama.MaxVersion

	return config
}

// GetClusterAdmin creates a new sarama.ClusterAdmin.
//
// The caller is responsible for closing the sarama.ClusterAdmin.
func GetClusterAdmin(adminFunc NewClusterAdminFunc, bootstrapServers []string) (sarama.ClusterAdmin, error) {
	return GetClusterAdminFromConfig(adminFunc, AdminConfig(), bootstrapServers)
}

// GetClusterAdminFromConfig creates a new sarama.ClusterAdmin.
//
// The caller is responsible for closing the sarama.ClusterAdmin.
func GetClusterAdminFromConfig(adminFunc NewClusterAdminFunc, config *sarama.Config, bootstrapServers []string) (sarama.ClusterAdmin, error) {

	kafkaClusterAdmin, err := adminFunc(bootstrapServers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create cluster admin: %w", err)
	}

	return kafkaClusterAdmin, nil
}
