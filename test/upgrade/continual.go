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

package upgrade

import (
	pkgupgrade "knative.dev/pkg/test/upgrade"

	"knative.dev/eventing-kafka-broker/test/upgrade/continual"
	eventingkafkacontinual "knative.dev/eventing-kafka/test/upgrade/continual"
)

// ContinualTests returns all continual tests.
func ContinualTests() []pkgupgrade.BackgroundOperation {
	c := BrokerContinualTests()
	c = append(c, ChannelContinualTests()...)
	c = append(c, SinkContinualTests()...)
	return c
}

// BrokerContinualTests returns background operations to test broker
// functionality in continual manner during the whole upgrade and downgrade
// process asserting that all events are propagated.
func BrokerContinualTests() []pkgupgrade.BackgroundOperation {
	return []pkgupgrade.BackgroundOperation{
		continual.BrokerTest(continual.KafkaBrokerTestOptions{}),
	}
}

// ChannelContinualTests returns background operations to test KafkaChannel
// functionality in continual manner during the whole upgrade and downgrade
// process asserting that all events are propagated.
func ChannelContinualTests() []pkgupgrade.BackgroundOperation {
	// don't want to run the Broker-Backed-By-KafkaChannel tests,
	// so, we don't call the test method directly
	return []pkgupgrade.BackgroundOperation{
		eventingkafkacontinual.ChannelTest(eventingkafkacontinual.ChannelTestOptions{}),
	}
}

// SinkContinualTests returns background operations to test KafkaSink
// functionality in continual manner during the whole upgrade and downgrade
// process asserting that all events are propagated.
func SinkContinualTests() []pkgupgrade.BackgroundOperation {
	return []pkgupgrade.BackgroundOperation{
		continual.SinkSourceTest(continual.KafkaSinkSourceTestOptions{}),
	}
}
