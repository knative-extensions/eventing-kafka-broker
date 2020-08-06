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

package main

import (
	"log"

	"github.com/Shopify/sarama"
	"github.com/kelseyhightower/envconfig"

	kafkatest "knative.dev/eventing-kafka-broker/test/pkg/kafka"
)

func main() {
	envConfig := &kafkatest.Config{}

	if err := envconfig.Process("", envConfig); err != nil {
		log.Fatalf("failed to process env variables: %v", err)
	}

	config := sarama.NewConfig()
	config.Version = sarama.MaxVersion

	admin, err := sarama.NewClusterAdmin([]string{envConfig.BootstrapServers}, config)
	if err != nil {
		log.Fatalf("failed to create cluster admin: %v", err)
	}

	topics, err := admin.ListTopics()
	if err != nil {
		log.Fatalf("failed to list topics: %v", err)
	}

	td, ok := topics[envConfig.Topic]
	if !ok {
		log.Fatalf("topic %s hasn't been created", envConfig.Topic)
	}
	if td.NumPartitions != envConfig.NumPartitions {
		log.Fatalf("expected num partitions %d got %d", envConfig.NumPartitions, td.NumPartitions)
	}
	if td.ReplicationFactor != envConfig.ReplicationFactor {
		log.Fatalf("expected replication factor %d got %d", envConfig.ReplicationFactor, td.ReplicationFactor)
	}
}
