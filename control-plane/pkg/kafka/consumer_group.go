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
	"context"
	"fmt"

	"github.com/IBM/sarama"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka/offset"
)

// InitOffsetsFunc initialize offsets for a provided set of topics and a provided consumer group id.
type InitOffsetsFunc func(ctx context.Context, kafkaClient sarama.Client, kafkaAdminClient sarama.ClusterAdmin, topics []string, consumerGroup string) (int32, error)

var (
	_ InitOffsetsFunc = offset.InitOffsets
)

const (
	GroupIDAnnotation = "group.id"
)

func AreConsumerGroupsPresentAndValid(kafkaClusterAdmin sarama.ClusterAdmin, consumerGroups ...string) (bool, error) {
	if len(consumerGroups) == 0 {
		return false, fmt.Errorf("expected at least one consumergroup, got 0")
	}

	consumerGroupDescription, err := kafkaClusterAdmin.DescribeConsumerGroups(consumerGroups)
	if err != nil {
		return false, fmt.Errorf("failed to describe consumer groups %v: %w", consumerGroups, err)
	}

	descriptionByConsumerGroup := make(map[string]*sarama.GroupDescription, len(consumerGroupDescription))
	for _, d := range consumerGroupDescription {
		descriptionByConsumerGroup[d.GroupId] = d
	}

	for _, t := range consumerGroups {
		d, ok := descriptionByConsumerGroup[t]
		if !ok {
			return false, fmt.Errorf("kafka did not respond with consumer group metadata")
		}
		if !isValidSingleConsumerGroup(d) {
			return false, nil
		}
	}
	return true, nil
}

func isValidSingleConsumerGroup(metadata *sarama.GroupDescription) bool {
	if metadata == nil {
		return false
	}

	if metadata.State == "Dead" {
		return false
	}

	return true
}
