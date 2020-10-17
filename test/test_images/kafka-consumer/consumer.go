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

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/Shopify/sarama"
	protocolkafka "github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/google/uuid"
	"github.com/kelseyhightower/envconfig"
	"k8s.io/apimachinery/pkg/util/sets"
	"knative.dev/pkg/signals"

	eventing "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/eventing/v1alpha1"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka"
	kafkatest "knative.dev/eventing-kafka-broker/test/pkg/kafka"
)

func main() {

	ctx, cancel := context.WithCancel(signals.NewContext())
	defer cancel()

	envConfig := &kafkatest.ConsumerConfig{}

	if err := envconfig.Process("", envConfig); err != nil {
		log.Fatal("Failed to process environment variables", err)
	}

	j, _ := json.MarshalIndent(envConfig, "", " ")
	log.Println(string(j))

	consumerConfig := sarama.NewConfig()
	consumerConfig.Version = sarama.V2_0_0_0
	consumerConfig.Consumer.Return.Errors = true
	consumerConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer, err := sarama.NewConsumerGroup(kafka.BootstrapServersArray(envConfig.BootstrapServers), uuid.New().String(), consumerConfig)
	if err != nil {
		log.Fatal("Failed to create consumer", err)
	}
	defer consumer.Close()

	events := make(chan cloudevents.Event)

	encoding := stringToEncoding(envConfig.ContentMode)
	if encoding == binding.EncodingUnknown {
		log.Fatalf("Unknown encoding in environment configuration: %s\n", envConfig.ContentMode)
	}

	handler := &handler{
		events:   events,
		encoding: stringToEncoding(envConfig.ContentMode),
	}

	exitError := make(chan error)

	go func() {
		for {
			err := consumer.Consume(ctx, []string{envConfig.Topic}, handler)
			if err == sarama.ErrClosedConsumerGroup {
				return
			}
			if err != nil {
				exitError <- err
				return
			}
		}
	}()

	go func() {
		for e := range consumer.Errors() {
			log.Println(e)
		}
	}()

	ids := strings.Split(envConfig.IDS, ",")
	set := sets.NewString(ids...)
	defer func() {
		log.Println("Remaining", set)
	}()

	for set.Len() == 0 {
		select {
		case err := <-exitError:
			log.Fatal("Failed to consume", err)
		case e := <-events:
			if set.Has(e.ID()) {
				set.Delete(e.ID())
				log.Printf("Event matching received: %s\nRemaining: %v\n", e.ID(), set)
			} else {
				log.Fatalf("Failed to match event: %s\n%v\n", e.String(), set.List())
			}
		}
	}
}

type handler struct {
	events   chan<- cloudevents.Event
	encoding binding.Encoding
}

func (h *handler) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (h *handler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (h *handler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	log.Println("starting to read messages")

	for message := range claim.Messages() {

		message := protocolkafka.NewMessageFromConsumerMessage(message)

		log.Println("received a message", message.ReadEncoding())

		if message.ReadEncoding() != h.encoding {
			return fmt.Errorf("message received with different encoding: want %s got %s", h.encoding.String(), message.ReadEncoding().String())
		}

		event, err := binding.ToEvent(session.Context(), message)
		if err != nil {
			log.Println(err)
			return fmt.Errorf("failed to convert message to event: %w", err)
		}

		h.events <- *event
	}

	return nil
}

func stringToEncoding(encoding string) binding.Encoding {
	switch encoding {
	case eventing.ModeBinary:
		return binding.EncodingBinary
	case eventing.ModeStructured:
		return binding.EncodingStructured
	default:
		return binding.EncodingUnknown
	}
}
