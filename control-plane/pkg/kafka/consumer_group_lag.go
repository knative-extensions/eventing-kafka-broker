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
	"errors"
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
)

const (
	invalidOffset = -1
)

// ConsumerGroupLagProvider provides consumer group lags.
type ConsumerGroupLagProvider interface {
	// GetLag returns consumer group lag for a given topic and a given consumer group.
	GetLag(topic, consumerGroup string) (ConsumerGroupLag, error)

	// Close closes the consumer group lag provider.
	Close() error
}

// ConsumerGroupLag contains partition lag of a topic.
type ConsumerGroupLag struct {
	Topic         string
	ConsumerGroup string
	ByPartition   []PartitionLag
}

// PartitionLag contains consumer lag information of a partition.
type PartitionLag struct {
	LatestOffset   int64 // Offset that will be produced next.
	ConsumerOffset int64 // Offset that will be consumed next.
	Lag            int64 // LatestOffset - ConsumerOffset
}

type adminFunc func(client sarama.Client) (sarama.ClusterAdmin, error)

type consumerGroupLagProvider struct {
	client    sarama.Client
	adminFunc adminFunc
}

// NewConsumerGroupLagProvider creates a new ConsumerGroupLagProvider.
func NewConsumerGroupLagProvider(client sarama.Client, adminFunc adminFunc) ConsumerGroupLagProvider {
	return &consumerGroupLagProvider{client: client, adminFunc: adminFunc}
}

// GetLag returns consumer group lag for a given group.
func (p *consumerGroupLagProvider) GetLag(topic, consumerGroup string) (ConsumerGroupLag, error) {

	admin, err := p.adminFunc(p.client)
	if err != nil {
		return ConsumerGroupLag{}, fmt.Errorf("failed to create admin client: %w", err)
	}
	// Note: Do not close admin since it closes the underlying client.

	partitions, err := getPartitionsForTopic(admin, topic)
	if err != nil {
		return ConsumerGroupLag{}, err
	}

	offsets, err := admin.ListConsumerGroupOffsets(consumerGroup, map[string][]int32{topic: partitions})
	if err != nil {
		return ConsumerGroupLag{}, fmt.Errorf("failed to list consumer group offsets: %w", err)
	}

	consumerGroupLag := ConsumerGroupLag{
		Topic:         topic,
		ConsumerGroup: consumerGroup,
		ByPartition:   make([]PartitionLag, len(partitions)),
	}
	for _, partition := range partitions {
		partitionLag, err := p.getPartitionLag(partition, topic, offsets)
		if err != nil {
			return consumerGroupLag, fmt.Errorf("failed to get lag for partition %d: %w", partition, err)
		}
		consumerGroupLag.ByPartition[partition] = partitionLag
	}
	return consumerGroupLag, nil
}

func getPartitionsForTopic(admin sarama.ClusterAdmin, topic string) ([]int32, error) {

	topicsMetadata, err := admin.DescribeTopics([]string{topic})
	if err != nil {
		return nil, fmt.Errorf("failed to describe topic %s: %w", topic, err)
	}
	if len(topicsMetadata) != 1 {
		return nil, fmt.Errorf("unexpected number of topic metadata for topic %s: %d", topic, len(topicsMetadata))
	}

	partitionMetadata := topicsMetadata[0].Partitions
	partitions := make([]int32, 0, len(partitionMetadata))
	for _, p := range partitionMetadata {
		partitions = append(partitions, p.ID)
	}
	return partitions, nil
}

func (p *consumerGroupLagProvider) getPartitionLag(partition int32, topic string, offsets *sarama.OffsetFetchResponse) (PartitionLag, error) {

	// Get the offset of the message that will be consumed next.
	block := offsets.GetBlock(topic, partition)
	if block == nil {
		return PartitionLag{}, fmt.Errorf("failed to find offset block for partition %d of topic %s", partition, topic)
	}
	consumerOffset := block.Offset
	if consumerOffset <= invalidOffset {
		return PartitionLag{}, fmt.Errorf("received invalid consumer offset for topic %s offset: %d", topic, consumerOffset)
	}

	// Get the offset of the message that will be produced next.
	latestOffset, err := p.client.GetOffset(topic, partition, sarama.OffsetNewest)
	if err != nil {
		return PartitionLag{}, fmt.Errorf("failed to find latest offset for topic %s and partition %d", topic, partition)
	}
	if latestOffset <= invalidOffset {
		return PartitionLag{}, fmt.Errorf("received invalid latest for topic %s offset: %d", topic, latestOffset)
	}

	pl := PartitionLag{
		LatestOffset:   latestOffset,
		ConsumerOffset: consumerOffset,
		Lag:            latestOffset - consumerOffset,
	}
	return pl, nil
}

func (p *consumerGroupLagProvider) Close() error {
	if err := p.client.Close(); err != nil && !errors.Is(err, sarama.ErrClosedClient) {
		return err
	}
	return nil
}

// Total returns the sum of each partition lag.
func (cgl ConsumerGroupLag) Total() uint64 {
	var total uint64
	for _, lag := range cgl.ByPartition {
		total += uint64(lag.Lag)
	}
	return total
}

func (cgl ConsumerGroupLag) String() string {
	sb := &strings.Builder{}

	writeSeparator := func(n int, sb *strings.Builder) {
		sb.WriteString(strings.Repeat("-", n))
		sb.WriteRune('\n')
	}

	// Write header
	// --------------
	// partition: lag
	// --------------
	header := "partition: lag\n"
	n := len(header)

	sb.WriteString(fmt.Sprintf("Topic: %s\n", cgl.Topic))
	sb.WriteString(fmt.Sprintf("Consumer group: %s\n", cgl.ConsumerGroup))
	writeSeparator(n, sb)
	sb.WriteString(header)
	writeSeparator(n, sb)

	for p, l := range cgl.ByPartition {
		sb.WriteString(fmt.Sprintf("%d: %d\n", p, l))
	}
	sb.WriteString(fmt.Sprintf("Total lag: %d\n", cgl.Total()))
	return sb.String()
}

func (pl PartitionLag) String() string {
	return fmt.Sprintf("latest offset %d consumer offset %d lag %d", pl.LatestOffset, pl.ConsumerOffset, pl.Lag)
}
