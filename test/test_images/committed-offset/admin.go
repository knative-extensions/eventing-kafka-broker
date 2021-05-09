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

package main

import (
	"encoding/json"
	"log"

	"github.com/Shopify/sarama"
	"github.com/kelseyhightower/envconfig"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	kafkatest "knative.dev/eventing-kafka-broker/test/pkg/kafka"
)

func main() {

	adminConfig := sarama.NewConfig()
	adminConfig.Version = sarama.V2_0_0_0

	envConfig := &kafkatest.AdminConfig{}

	if err := envconfig.Process("", envConfig); err != nil {
		log.Fatal("Failed to process environment variables", err)
	}

	j, _ := json.MarshalIndent(envConfig, "", " ")
	log.Println(string(j))

	client, err := sarama.NewClient([]string{envConfig.BootstrapServers}, adminConfig)
	if err != nil {
		log.Fatal("Failed to create sarama client", err)
	}
	consumerGroupLagProvider := kafka.NewConsumerGroupLagProvider(client, sarama.NewClusterAdminFromClient, sarama.OffsetOldest)
	defer consumerGroupLagProvider.Close()

	lag, err := consumerGroupLagProvider.GetLag(envConfig.Topic, envConfig.Group)
	if err != nil {
		log.Fatal("Failed to get lag", err)
	}

	if lag.Total() != 0 {
		log.Fatal("Lag is not 0", lag)
	}
}
