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
	"time"

	"github.com/Shopify/sarama"
	"k8s.io/apimachinery/pkg/util/wait"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	testingpkg "knative.dev/eventing-kafka-broker/test/pkg/testing"
)

func main() {

	defer func() {
		if err := recover(); err != nil {
			log.Fatal("Panic", err)
		}
	}()

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
	// Sync producer config
	producerConfig.Producer.Return.Successes = true
	producerConfig.Producer.Return.Errors = true

	producer, err := sarama.NewSyncProducer(testingpkg.BootstrapServersPlaintextArr, producerConfig)
	mustBeNil(err)
	defer producer.Close()

	// Produce messages with the same key.
	n := 3
	producedMsgPartition := int32(-1)
	lastOffset := int64(-1)

	log.Println("Sending events to topic", topic)

	for i := 0; i < n; i++ {
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(key),
			Value: sarama.StringEncoder(value),
		}
		// Send message might fail with:
		// "kafka server: Request was for a topic or partition that does not exist on this broker."
		err := wait.PollImmediateUntil(time.Minute, func() (done bool, err error) {
			partition, offset, err := producer.SendMessage(msg)
			if err != nil {
				return false, nil
			}
			log.Printf("Event sent partition %d offset %d", partition, offset)
			if producedMsgPartition == -1 {
				producedMsgPartition = partition
			}
			lastOffset = offset
			return true, nil
		}, ctx.Done())
		mustBeNil(err)
	}
	if int64(n) != lastOffset+1 { // Consistency check
		log.Fatalf("Expected last offset to be equal to %d got %d", n, lastOffset+1)
	}

	log.Println("Consuming events from topic", topic)

	consumerConfig := sarama.NewConfig()
	consumerConfig.Consumer.Fetch.Min = 1
	consumerConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
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
				log.Println("Count", n)
				if count == n {
					return
				}
			}
		}
	}()
	err = consumer.Close()
	mustBeNil(err)

	// Wait for propagation of the committed offset
	err = wait.PollImmediateUntil(time.Minute, func() (done bool, err error) {
		log.Println("Starting consumer group lag provider")

		consumerGroupLagProvider := kafka.NewConsumerGroupLagProvider(client, sarama.NewClusterAdminFromClient, sarama.OffsetOldest)
		defer consumerGroupLagProvider.Close()

		log.Printf("Getting lag for topic %s and consumer group %s\n", topic, consumerGroup)
		consumerGroupLag, err := consumerGroupLagProvider.GetLag(topic, consumerGroup)
		mustBeNil(err)

		log.Println("ConsumerGroupLag", consumerGroupLag)

		nNonZeroOffsets := 0
		for p, l := range consumerGroupLag.ByPartition {
			log.Printf("Partition %d Lag %s\n", p, l)
			if l.ConsumerOffset > 0 {
				nNonZeroOffsets++
			}
			if l.ConsumerOffset < 0 {
				log.Fatal("Consumer offset cannot be negative")
			}
			if l.LatestOffset < 0 {
				log.Fatal("Latest offset cannot be negative")
			}
			if l.LatestOffset-l.ConsumerOffset != l.Lag() {
				log.Fatal("Expected Lag to be equal to LatestOffset - ConsumerOffset got", l)
			}
		}
		if nNonZeroOffsets != 1 { // consistency check
			log.Println("Only 1 partition is expected to have records since test uses the same key for all of them ", nNonZeroOffsets)
			return false, nil
		}

		total := consumerGroupLag.Total()
		log.Println("Total lag", total)
		if total != 0 {
			return false, nil
		}
		return true, nil
	}, ctx.Done())
	mustBeNil(err)
}

type handler struct {
	msgs chan<- *sarama.ConsumerMessage
}

func (h *handler) Setup(sarama.ConsumerGroupSession) error {
	log.Println("Consumer started")
	return nil
}

func (h *handler) Cleanup(session sarama.ConsumerGroupSession) error {
	session.Commit()
	return nil
}

func (h *handler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	for msg := range claim.Messages() {
		session.MarkMessage(msg, "")
		log.Println("Message received", string(msg.Key), string(msg.Value))
		h.msgs <- msg
	}

	return nil
}

func mustBeNil(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
