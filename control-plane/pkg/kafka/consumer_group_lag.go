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
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
)

const (
	invalidOffset = -1
)

// ConsumerGroupLag is the sum of the differences between the latest published message and the latest committed offset
// of each partition in a topic
type ConsumerGroupLag struct {
	Topic       string
	ByPartition map[int32]uint64
}

// Total returns the sum of each partition lag.
func (cgl ConsumerGroupLag) Total() uint64 {
	var total uint64
	for _, lag := range cgl.ByPartition {
		total += lag
	}
	return total
}

func (cgl ConsumerGroupLag) String() string {
	sb := strings.Builder{}
	header := "partition: lag\n"
	sb.WriteString(header)
	sb.WriteString(strings.Repeat("-", len(header)-1) + "\n")
	for p, l := range cgl.ByPartition {
		sb.WriteString(fmt.Sprintf("%d: %d\n", p, l))
	}
	return sb.String()
}

// ConsumerGroupLagProvider provides consumer group lags for a given topic.
type ConsumerGroupLagProvider struct {
	client sarama.Client
	topic  string
}

// NewConsumerGroupLagProvider creates a new ConsumerGroupLagProvider
func NewConsumerGroupLagProvider(client sarama.Client, topic string) ConsumerGroupLagProvider {
	return ConsumerGroupLagProvider{client: client, topic: topic}
}

// GetLag returns consumer group lag for the given group.
func (p *ConsumerGroupLagProvider) GetLag(group string) (ConsumerGroupLag, error) {

	admin, err := sarama.NewClusterAdminFromClient(p.client)
	if err != nil {
		return ConsumerGroupLag{}, fmt.Errorf("failed to create admin client: %w", err)
	}

	partitions, err := getPartitionsForTopic(admin, p.topic)
	if err != nil {
		return ConsumerGroupLag{}, err
	}

	offsets, err := admin.ListConsumerGroupOffsets(group, map[string][]int32{p.topic: partitions})
	if err != nil {
		return ConsumerGroupLag{}, fmt.Errorf("error listing consumer group offsets: %s", err)
	}

	consumerGroupLag := ConsumerGroupLag{
		Topic:       p.topic,
		ByPartition: make(map[int32]uint64, len(partitions)),
	}
	for _, partition := range partitions {
		lag, err := p.getLagForPartition(partition, offsets)
		if err != nil {
			return ConsumerGroupLag{}, fmt.Errorf("failed to get lag for partition %d: %v", partition, err)
		}
		consumerGroupLag.ByPartition[partition] = lag
	}
	return consumerGroupLag, nil
}

func getPartitionsForTopic(admin sarama.ClusterAdmin, topic string) ([]int32, error) {

	topicsMetadata, err := admin.DescribeTopics([]string{topic})
	if err != nil {
		return nil, fmt.Errorf("failed to describe topic %s: %w", topic, err)
	}
	if len(topicsMetadata) != 1 {
		return nil, fmt.Errorf("unexpected %d topic metadata", len(topicsMetadata))
	}

	partitionMetadata := topicsMetadata[0].Partitions
	partitions := make([]int32, len(partitionMetadata))
	for i, p := range partitionMetadata {
		partitions[i] = p.ID
	}

	return partitions, nil
}

func (p *ConsumerGroupLagProvider) getLagForPartition(partition int32, offsets *sarama.OffsetFetchResponse) (uint64, error) {

	// Get the latest committed offset.
	block := offsets.GetBlock(p.topic, partition)
	if block == nil {
		return 0, fmt.Errorf("failed to find offset block for partition %d", partition)
	}
	consumerOffset := block.Offset
	if consumerOffset <= invalidOffset {
		return 0, fmt.Errorf("invalid consumer offset %d", consumerOffset)
	}

	// Get the offset of the message that will be produced next.
	latestOffset, err := p.client.GetOffset(p.topic, partition, sarama.OffsetNewest)
	if err != nil {
		return 0, fmt.Errorf("failed to find latest offset for topic %s and partition %d", p.topic, partition)
	}
	if latestOffset <= invalidOffset {
		return 0, fmt.Errorf("invalid latest offset %d", latestOffset)
	}

	return uint64(latestOffset - consumerOffset), nil
}
