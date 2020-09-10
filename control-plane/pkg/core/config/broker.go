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

package config

import (
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/types"
)

const (
	// NoBroker signals that the broker hasn't been found.
	NoBroker = -1
)

// FindBroker finds the broker with the given UID in the given brokers list.
func FindBroker(brokers *Brokers, broker types.UID) int {
	// Find broker in brokersTriggers.
	brokerIndex := NoBroker
	for i, b := range brokers.Brokers {
		if b.Id == string(broker) {
			brokerIndex = i
			break
		}
	}
	return brokerIndex
}

// AddOrUpdateBrokersConfig adds or updates the given brokerConfig to the given brokers at the specified index.
func AddOrUpdateBrokersConfig(brokers *Brokers, brokerConfig *Broker, index int, logger *zap.Logger) {

	if index != NoBroker {
		brokerConfig.Triggers = brokers.Brokers[index].Triggers
		brokers.Brokers[index] = brokerConfig

		logger.Debug("Sink exists", zap.Int("index", index))

	} else {
		brokers.Brokers = append(brokers.Brokers, brokerConfig)

		logger.Debug("Sink doesn't exist")
	}
}
