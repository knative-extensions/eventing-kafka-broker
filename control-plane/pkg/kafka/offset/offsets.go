/*
Copyright 2021 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package offset

import (
	"context"
	"errors"
	"fmt"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
	"knative.dev/pkg/logging"
)

// InitOffsets initialize the offsets of the given consumer group for the given topics to the newest
// offsets of all topic-partitions.
//
// We want to make sure that ALL consumer group offsets are set before marking
// the resource as ready, to avoid "losing" events in case the consumer group session
// is closed before at least one message is consumed from ALL partitions.
// Without InitOffsets, an event sent to a partition with an uninitialized offset
// will not be forwarded when the session is closed (or a rebalancing is in progress).
func InitOffsets(ctx context.Context, kafkaClient sarama.Client, kafkaAdminClient sarama.ClusterAdmin, topics []string, consumerGroup string) (int32, error) {
	offsetManager, err := sarama.NewOffsetManagerFromClient(consumerGroup, kafkaClient)
	if err != nil {
		return -1, err
	}
	defer offsetManager.Close()

	totalPartitions, topicPartitions, err := retrieveAllPartitions(topics, kafkaClient)
	if err != nil {
		return -1, err
	}

	// Fetch topic offsets
	topicOffsets, err := getOffsets(kafkaClient, topicPartitions, sarama.OffsetNewest)
	if err != nil {
		return -1, fmt.Errorf("failed to get the topic offsets: %w", err)
	}

	// Look for uninitialized offset (-1)
	offsets, err := kafkaAdminClient.ListConsumerGroupOffsets(consumerGroup, topicPartitions)
	if err != nil {
		return -1, err
	}

	dirty := false
	for topic, partitions := range offsets.Blocks {
		for partitionID, block := range partitions {
			if block.Offset < 0 {
				partitionsOffsets, ok := topicOffsets[topic]
				if !ok {
					// topic may have been deleted. ignore.
					continue
				}

				offset, ok := partitionsOffsets[partitionID]
				if !ok {
					// partition may have been deleted. ignore.
					continue
				}

				logging.FromContext(ctx).Infow("initializing offset", zap.String("topic", topic), zap.Int32("partition", partitionID), zap.Int64("offset", offset))

				pm, err := offsetManager.ManagePartition(topic, partitionID)
				if err != nil {
					return -1, fmt.Errorf("failed to create the partition manager for topic %s and partition %d: %w", topic, partitionID, err)
				}

				pm.MarkOffset(offset, "")
				dirty = true
			}
		}
	}

	if dirty {
		offsetManager.Commit()
		logging.FromContext(ctx).Infow("consumer group offsets committed", zap.String("consumergroup", consumerGroup))
	}

	// At this stage the resource is considered Ready
	return int32(totalPartitions), nil

}

func AreOffsetsInitialized(ctx context.Context, kafkaClient sarama.Client, kafkaAdminClient sarama.ClusterAdmin, topics []string, consumerGroup string) (bool, error) {
	_, topicPartitions, err := retrieveAllPartitions(topics, kafkaClient)
	if err != nil {
		return false, err
	}

	// Look for uninitialized offset (-1)
	offsets, err := kafkaAdminClient.ListConsumerGroupOffsets(consumerGroup, topicPartitions)
	if err != nil {
		return false, err
	}

	for _, partitions := range offsets.Blocks {
		for _, block := range partitions {
			if block.Offset < 0 {
				return false, nil
			}
		}
	}

	return true, nil
}

func retrieveAllPartitions(topics []string, kafkaClient sarama.Client) (int, map[string][]int32, error) {
	totalPartitions := 0

	if err := kafkaClient.RefreshMetadata(topics...); err != nil {
		return -1, nil, fmt.Errorf("failed to refresh metadata for topics %#v: %w", topics, err)
	}

	// Retrieve all partitions
	topicPartitions := make(map[string][]int32)
	for _, topic := range topics {
		partitions, err := kafkaClient.Partitions(topic)
		totalPartitions += len(partitions)
		if err != nil {
			return -1, nil, fmt.Errorf("failed to get partitions for topic %s: %w", topic, err)
		}

		// return a copy of the partitions array in the map
		// Sarama is caching this array and we don't want nobody to mess with it
		clone := make([]int32, len(partitions))
		copy(clone, partitions)
		topicPartitions[topic] = clone
	}
	return totalPartitions, topicPartitions, nil
}

// getOffsets queries the cluster to get the most recent available offset at the
// given time (in milliseconds) for the given topics and partition
// Time should be OffsetOldest for the earliest available offset,
// OffsetNewest for the offset of the message that will be produced next, or a time.
//
// See sarama.Client.GetOffset for getting the offset of a single topic/partition combination
func getOffsets(client sarama.Client, topicPartitions map[string][]int32, time int64) (map[string]map[int32]int64, error) {
	if client.Closed() {
		return nil, sarama.ErrClosedClient
	}
	offsets, err := retrieveOffsets(client, topicPartitions, time)
	if err != nil {
		if err := client.RefreshMetadata(mapKeys(topicPartitions)...); err != nil {
			return nil, err
		}
		return retrieveOffsets(client, topicPartitions, time)
	}

	return offsets, nil
}

func retrieveOffsets(client sarama.Client, topicPartitions map[string][]int32, time int64) (map[string]map[int32]int64, error) {
	version := int16(0)
	if client.Config().Version.IsAtLeast(sarama.V0_10_1_0) {
		version = 1
	}

	offsets := make(map[string]map[int32]int64)

	// Step 1: build one OffsetRequest instance per broker.
	requests := make(map[*sarama.Broker]*sarama.OffsetRequest)

	for topic, partitions := range topicPartitions {
		offsets[topic] = make(map[int32]int64)

		for _, partitionID := range partitions {
			broker, err := client.Leader(topic, partitionID)
			if err != nil {
				return nil, err
			}

			request, ok := requests[broker]
			if !ok {
				request = &sarama.OffsetRequest{Version: version}
				requests[broker] = request
			}

			request.AddBlock(topic, partitionID, time, 1)
		}
	}

	// Step 2: send requests, one per broker, and collect offsets
	for broker, request := range requests {
		response, err := broker.GetAvailableOffsets(request)

		if err != nil {
			return nil, err
		}

		for topic, blocks := range response.Blocks {
			for partitionID, block := range blocks {
				if !errors.Is(block.Err, sarama.ErrNoError) {
					return nil, block.Err
				}

				offsets[topic][partitionID] = block.Offset
			}
		}
	}

	return offsets, nil

}

func mapKeys(m map[string][]int32) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
