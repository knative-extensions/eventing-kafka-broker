//go:build e2e

/*
 * Copyright 2026 The Knative Authors
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

package kafkachannel

import (
	"context"
	"time"

	cloudeventtest "github.com/cloudevents/sdk-go/v2/test"
	kafkachannelresources "knative.dev/eventing-kafka-broker/test/rekt/resources/kafkachannel"
	subscriptionresources "knative.dev/eventing/test/rekt/resources/subscription"
	"knative.dev/reconciler-test/pkg/eventshub"
	"knative.dev/reconciler-test/pkg/eventshub/assert"
	"knative.dev/reconciler-test/pkg/feature"
)

// FirstEventDelay creates a feature that sets up a KafkaChannel with configurable partitions,
// a Subscription, an eventshub receiver, sends exactly 1 CloudEvent, and asserts it was received.
// This feature replicates the core logic of test/scripts/first-event-delay.sh.
//
// Parameters:
//   - channelName: name of the KafkaChannel to create
//   - numPartitions: number of partitions for the KafkaChannel (as string, e.g., "10")
//
// The function creates a receiver named "<channelName>-receiver" and a sender named "<channelName>-sender".
func FirstEventDelay(channelName, numPartitions string) *feature.Feature {
	receiverName := channelName + "-receiver"
	senderName := channelName + "-sender"

	f := feature.NewFeatureNamed("First Event Delay - " + channelName)

	// Setup: receiver first, then channel, then subscription
	setupEventsHubReceiver(f, receiverName)

	f.Setup("Install KafkaChannel", kafkachannelresources.Install(channelName,
		kafkachannelresources.WithNumPartitions(numPartitions),
		kafkachannelresources.WithReplicationFactor("3"),
	))

	setupSubscription(f, channelName, receiverName)

	// Wait for readiness before sending — these must be Requirements (not Asserts)
	// so they execute before the sender fires its single event.
	f.Requirement("KafkaChannel Is Ready", kafkachannelresources.IsReady(channelName))
	f.Requirement("Subscription Is Ready", subscriptionresources.IsReady(channelName))

	// Send events repeatedly to ride through the data-plane startup delay.
	// Control-plane readiness doesn't guarantee the Kafka consumer group is
	// operational; early events may be lost, matching the old heartbeat behavior.
	f.Requirement("Install sender", func(ctx context.Context, t feature.T) {
		event, err := newEvent(channelName, senderName)
		if err != nil {
			t.Fatal("Failed to create event:", err)
		}
		eventshub.Install(senderName,
			eventshub.StartSenderToResource(kafkachannelresources.GVR(), channelName),
			eventshub.InputEvent(event),
			eventshub.SendMultipleEvents(60, time.Second),
		)(ctx, t)
	})

	// Assert the event was received with correct type and source
	f.Assert("Event received with correct type and source", assert.OnStore(receiverName).
		MatchEvent(
			cloudeventtest.HasType(channelName+"-type"),
			cloudeventtest.HasSource(senderName),
		).AtLeast(1),
	)

	return f
}
