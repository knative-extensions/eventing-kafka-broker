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
	"knative.dev/eventing-kafka-broker/control-plane/pkg/security"
)

// NewClusterAdminFunc creates new sarama.ClusterAdmin.
type NewClusterAdminFunc func(addrs []string, config *sarama.Config) (sarama.ClusterAdmin, error)

// GetClusterAdminSaramaConfig returns Kafka Admin configurations that the ConfigOptions are applied to
func GetClusterAdminSaramaConfig(secOptions security.ConfigOption) (*sarama.Config, error) {
	config := sarama.NewConfig()
	config.Version = sarama.MaxVersion

	err := secOptions(config)
	if err != nil {
		return nil, err
	}

	return config, nil
}
