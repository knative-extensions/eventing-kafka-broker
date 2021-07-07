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
	"encoding/base64"
	"go.uber.org/zap"
	"knative.dev/pkg/logging"
	"knative.dev/reconciler-test/pkg/eventshub"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	protocolkafka "github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2"
	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/google/uuid"
	"github.com/kelseyhightower/envconfig"
)

type ConsumerConfig struct {
	BootstrapServers []string `envconfig:"BOOTSTRAP_SERVERS" required:"true"`
	Topic            string   `envconfig:"TOPIC" required:"true"`
}

func StartConsumer(ctx context.Context, logs *eventshub.EventLogs) error {
	envConfig := &ConsumerConfig{}

	if err := envconfig.Process("", envConfig); err != nil {
		logging.FromContext(ctx).Fatal("Failed to process environment variables", err)
	}

	consumerConfig := sarama.NewConfig()
	consumerConfig.Version = sarama.V2_0_0_0
	consumerConfig.Consumer.Return.Errors = true
	consumerConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer, err := sarama.NewConsumerGroup(envConfig.BootstrapServers, uuid.New().String(), consumerConfig)
	if err != nil {
		logging.FromContext(ctx).Fatal("Failed to create consumer", err)
	}

	handler := &kafkaConsumerHandler{
		eventLogs: logs,
		logger:    logging.FromContext(ctx),
	}

	// goroutine to listen for errors
	go func() {
		for err := range consumer.Errors() {
			logging.FromContext(ctx).Warnw("Error in consumer group error channel", zap.Error(err))
		}
	}()

	// goroutine to close the consumer group
	go func() {
		<-ctx.Done()
		err := consumer.Close()
		if err != nil {
			logging.FromContext(ctx).Warnw("Error while closing consumer group", zap.Error(err))
		}
	}()

	// loop to consume the consumer group
	for {
		err := consumer.Consume(ctx, []string{envConfig.Topic}, handler)
		if err == sarama.ErrClosedConsumerGroup {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

type kafkaConsumerHandler struct {
	eventLogs *eventshub.EventLogs
	logger    *zap.SugaredLogger

	seq uint64
}

func (h *kafkaConsumerHandler) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (h *kafkaConsumerHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (h *kafkaConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	h.logger.Info("Starting to read messages")

	for message := range claim.Messages() {
		ceMessage := protocolkafka.NewMessageFromConsumerMessage(message)
		event, eventErr := binding.ToEvent(session.Context(), ceMessage)

		eventErrStr := ""
		if eventErr != nil {
			eventErrStr = eventErr.Error()
		}
		s := atomic.AddUint64(&h.seq, 1)

		eventInfo := eventshub.EventInfo{
			Error:    eventErrStr,
			Event:    event,
			Observer: "kafka-receiver",
			Time:     time.Now(),
			Sequence: s,
			Kind:     eventshub.EventReceived,
			AdditionalInfo: map[string]interface{}{
				"topic":     message.Topic,
				"key":       base64.StdEncoding.EncodeToString(message.Key),
				"partition": message.Partition,
				"offset":    message.Offset,
				"timestamp": message.Timestamp.Format(time.RFC3339Nano),
			},
		}

		if err := h.eventLogs.Vent(eventInfo); err != nil {
			h.logger.Fatalw("Error while venting the recorded event", zap.Error(err))
		}
	}

	return nil
}
