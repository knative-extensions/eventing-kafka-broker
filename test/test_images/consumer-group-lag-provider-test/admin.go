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
	"context"
	"log"

	"github.com/Shopify/sarama"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	testingpkg "knative.dev/eventing-kafka-broker/test/pkg/testing"
)

func main() {

	const (
		topic         = "test-consumer-group-lag-provider"
		consumerGroup = topic
		key           = "msg-key"
		value         = "msg-value"
	)

	ctx := context.Background()

	client, err := sarama.NewClient(testingpkg.BootstrapServersPlaintextArr, sarama.NewConfig())
	mustBeNil(err)

	admin, err := sarama.NewClusterAdminFromClient(client)
	mustBeNil(err)
	// Do not close admin since it closes the underlying client

	topicDetail := &sarama.TopicDetail{NumPartitions: 2, ReplicationFactor: 2}
	err = admin.CreateTopic(topic, topicDetail, false)
	mustBeNil(err)

	producerConfig := sarama.NewConfig()
	// Disable buffering
	producerConfig.Producer.Flush.Bytes = 1
	producerConfig.Producer.Flush.MaxMessages = 1
	producerConfig.Producer.Flush.Messages = 1
	// Disable potential duplication
	producerConfig.Producer.Idempotent = true
	producer, err := sarama.NewSyncProducer(testingpkg.BootstrapServersPlaintextArr, producerConfig)
	mustBeNil(err)

	// Produce messages with the same key.
	n := 3
	producedMsgPartition := int32(-1)
	lastOffset := int64(-1)
	for i := 0; i < n; i++ {
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(key),
			Value: sarama.StringEncoder(value),
		}
		partition, offset, err := producer.SendMessage(msg)
		mustBeNil(err)
		if producedMsgPartition == -1 {
			producedMsgPartition = partition
		}
		lastOffset = offset
	}
	if int64(n) != lastOffset+1 { // Consistency check
		log.Fatalf("Expected last offset to be equal to %d got %d", n, lastOffset+1)
	}

	consumerConfig := sarama.NewConfig()
	consumerConfig.Consumer.Fetch.Min = 1
	consumerConfig.Consumer.Offsets.AutoCommit.Enable = false

	consumer, err := sarama.NewConsumerGroup(testingpkg.BootstrapServersPlaintextArr, consumerGroup, consumerConfig)
	mustBeNil(err)

	msgs := make(chan *sarama.ConsumerMessage, n)
	handler := &handler{msgs: msgs}

	count := 0
	func() {
		go func() {
			_ = consumer.Consume(ctx, []string{topic}, handler)
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case <-msgs:
				count++
				if count == n {
					return
				}
			}
		}
	}()
	err = consumer.Close()
	mustBeNil(err)

	consumerGroupLagProvider := kafka.NewConsumerGroupLagProvider(client)
	defer func() { mustBeNil(consumerGroupLagProvider.Close()) }()

	consumerGroupLag, err := consumerGroupLagProvider.GetLag(topic, consumerGroup)
	mustBeNil(err)

	nNonZeroOffsets := 0
	for _, l := range consumerGroupLag.ByPartition {
		if l.ConsumerOffset > 0 {
			nNonZeroOffsets++
		}
		if l.ConsumerOffset < 0 {
			log.Fatal("Consumer offset cannot be negative")
		}
		if l.LatestOffset < 0 {
			log.Fatal("Latest offset cannot be negative")
		}
		if l.LatestOffset-l.ConsumerOffset != l.Lag {
			log.Fatal("Expected Lag to be equal to LatestOffset - ConsumerOffset got", l)
		}
	}
	if nNonZeroOffsets != 1 { // consistency check
		log.Fatal("Only 1 partition is expected to have records since test uses the same key for all of them", nNonZeroOffsets)
	}

	total := consumerGroupLag.Total()
	if total != 0 {
		log.Fatal("Expected total to be 0 got", total)
	}
}

type handler struct {
	msgs chan<- *sarama.ConsumerMessage
}

func (h *handler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *handler) Cleanup(session sarama.ConsumerGroupSession) error {
	session.Commit()
	return nil
}

func (h *handler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	for msg := range claim.Messages() {
		session.MarkMessage(msg, "")
		h.msgs <- msg
	}

	return nil
}

func mustBeNil(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
