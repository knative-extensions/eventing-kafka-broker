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

package sarama

import (
	"github.com/Shopify/sarama"
)

// GetOffsets queries the cluster to get the most recent available offset at the
// given time (in milliseconds) for the given topics and partition
// Time should be OffsetOldest for the earliest available offset,
// OffsetNewest for the offset of the message that will be produced next, or a time.
//
// See sarama.Client.GetOffset for getting the offset of a single topic/partition combination
func GetOffsets(client sarama.Client, topicPartitions map[string][]int32, time int64) (map[string]map[int32]int64, error) {
	if client.Closed() {
		return nil, sarama.ErrClosedClient
	}

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
				if block.Err != sarama.ErrNoError {
					return nil, block.Err
				}

				offsets[topic][partitionID] = block.Offset
			}
		}
	}

	return offsets, nil
}
