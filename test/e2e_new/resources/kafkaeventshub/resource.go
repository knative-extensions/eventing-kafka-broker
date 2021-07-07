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

package kafkaeventshub

import (
	"context"
	"strings"

	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/eventshub"
)

func init() {
	environment.RegisterPackage(testImagePackage)
}

const (
	testImagePackage    = "knative.dev/eventing-kafka-broker/test/test_images/kafkaeventshub"
	kafkaEventsHubImage = "ko://" + testImagePackage
)

// WithKafkaEventsHub uses the kafka eventshub image when invoking eventshub.Install
var WithKafkaEventsHub = eventshub.WithCustomImage(kafkaEventsHubImage)

// StartKafkaConsumer starts a Kafka consumer in the eventshub image to read a topic
func StartKafkaConsumer(bootstrapServers []string, topic string) eventshub.EventsHubOption {
	return func(ctx context.Context, m map[string]string) error {
		if generators, ok := m["EVENT_GENERATORS"]; ok {
			m["EVENT_GENERATORS"] = generators + "," + "kafkaconsumer"
		} else {
			m["EVENT_GENERATORS"] = "kafkaconsumer"
		}

		m["BOOTSTRAP_SERVERS"] = strings.Join(bootstrapServers, ",")
		m["TOPIC"] = topic
		return nil
	}
}
