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

package continual

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
	protocolkafka "github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/binding"
	"go.uber.org/zap"
	"knative.dev/eventing-kafka/pkg/common/client"
	"knative.dev/eventing/test/upgrade/prober/wathola/sender"
)

var (
	// ErrIllegalEndpointFormat if given endpoint structure is illegal and can't
	// be used.
	ErrIllegalEndpointFormat = errors.New(
		"illegal format for Kafka topic endpoint")

	// ErrCantConnectToKafka if connection to kafka can't be established.
	ErrCantConnectToKafka = errors.New(
		"unable to connect to Kafka bootstrap servers")

	// ErrCantSend if event can't be sent to given Kafka topic
	ErrCantSend = errors.New("can't send event to kafka topic")
)

// CreateKafkaSender will create a wathola sender that sends events to Kafka
// topic directly.
func CreateKafkaSender(ctx context.Context, log *zap.SugaredLogger) sender.EventSender {
	return &kafkaSender{
		ctx: ctx,
		log: log,
	}
}

func (k *kafkaSender) Supports(endpoint interface{}) bool {
	switch endpoint.(type) {
	case map[string]interface{}:
		_, err := castAsTopicEndpoint(endpoint)
		return err == nil
	default:
		return false
	}
}

func (k *kafkaSender) SendEvent(ce cloudevents.Event, rawEndpoint interface{}) error {
	endpoint, err := castAsTopicEndpoint(rawEndpoint)
	if err != nil {
		// this should never happen, as Supports func should be called first.
		return err
	}
	conf, err := client.NewConfigBuilder().
		WithClientId("continualtests-kafka-sender").
		WithDefaults().
		Build(k.ctx)
	if err != nil {
		return err
	}
	producer, err := sarama.NewSyncProducer(endpoint.bootstrapServersSlice(), conf)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrCantConnectToKafka, err)
	}
	message := binding.ToMessage(&ce)
	kafkaProducerMessage := &sarama.ProducerMessage{
		Topic: endpoint.TopicName,
	}
	transformers := make([]binding.Transformer, 0)
	err = protocolkafka.WriteProducerMessage(k.ctx, message, kafkaProducerMessage, transformers...)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrCantSend, err)
	}
	part, offset, err := producer.SendMessage(kafkaProducerMessage)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrCantSend, err)
	}
	k.log.Infof("Event %s has been send to kafka topic %s (partition: %d, offset: %d)",
		ce.ID(), endpoint.TopicName, part, offset)
	return nil
}

type kafkaSender struct {
	log *zap.SugaredLogger
	ctx context.Context
}

type kafkaTopicEndpoint struct {
	BootstrapServers string
	TopicName        string
}

func (e kafkaTopicEndpoint) bootstrapServersSlice() []string {
	return strings.Split(e.BootstrapServers, ",")
}

func castAsTopicEndpoint(endpoint interface{}) (kafkaTopicEndpoint, error) {
	m, ok := endpoint.(map[string]interface{})
	if !ok {
		return kafkaTopicEndpoint{}, ErrIllegalEndpointFormat
	}
	servers := m["bootstrapServers"]
	if servers == "" {
		return kafkaTopicEndpoint{}, ErrIllegalEndpointFormat
	}
	topic := m["topicName"]
	if topic == "" {
		return kafkaTopicEndpoint{}, ErrIllegalEndpointFormat
	}
	return kafkaTopicEndpoint{
		BootstrapServers: fmt.Sprintf("%v", servers),
		TopicName:        fmt.Sprintf("%v", topic),
	}, nil
}
