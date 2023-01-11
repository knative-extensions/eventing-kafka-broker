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

package kafkachannel

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"knative.dev/reconciler-test/pkg/feature"
)

// ConfigureDataPlane creates a Feature which sets up the specified KafkaChannel
// Subscription and EventsHub Receiver so that it is ready to receive CloudEvents.
func ConfigureDataPlane(ctx context.Context, t *testing.T) *feature.Feature {

	// Get Test Names From Context
	testName := TestName(ctx, t)
	receiverName := ReceiverName(ctx, t)

	// Create A Feature To Configure The DataPlane (KafkaChannel, Subscription, Receiver)
	f := feature.NewFeatureNamed("Configure Data-Plane")
	setupEventsHubReceiver(f, receiverName)
	setupKafkaChannel(f, testName)
	setupSubscription(f, testName, receiverName)
	assertKafkaChannelReady(f, testName)
	assertSubscriptionReady(f, testName)

	// Return The ConfigureDataPlane Feature
	return f
}

// SendEvents creates a Feature which sends a number of CloudEvents to the specified
// KafkaChannel and verifies their receipt in the corresponding EventsHub Receiver.
// It is assumed that the backing KafkaChannel / Subscription / Receiver are in place
// and ready to receive these events.
func SendEvents(ctx context.Context, t *testing.T, eventCount int, startId int, endId int) *feature.Feature {

	// Get Test Names From Context
	testName := TestName(ctx, t)
	senderName := SenderName(ctx, t)
	senderSink := SenderSink(ctx, t)
	receiverName := ReceiverName(ctx, t)

	// Create The Base CloudEvent To Send (ID will be set by the EventsHub Sender)
	event, err := newEvent(testName, senderName)
	assert.Nil(t, err)

	// Create A New Feature To Send Events And Verify Receipt
	f := feature.NewFeatureNamed("Send Events")
	setupEventsHubSender(f, senderName, senderSink, event, eventCount)
	assertEventsReceived(f, receiverName, event, eventCount, startId, endId)

	// Return The SendEvents Feature
	return f
}

// ReplayEvents creates a Feature which adjusts a KafkaChannel Subscription to a specific
// offset time by creating a ResetOffset and verifying the expected final event count.
// The actual count is dependent upon the number of events in the KafkaChannel (Topic)
// related to the specified offsetTime.
func ReplayEvents(ctx context.Context, t *testing.T, offsetTime string, eventCount int, startId int, endId int) *feature.Feature {

	// Get Test Names From Context
	testName := TestName(ctx, t)
	senderName := SenderName(ctx, t)
	receiverName := ReceiverName(ctx, t)

	// Create The Base CloudEvent To Send (ID will be set by the EventsHub Sender)
	event, err := newEvent(testName, senderName)
	assert.Nil(t, err)

	// Create A New Feature To Replay Events And Verify Receipt
	f := feature.NewFeatureNamed("Replay Events")
	setupResetOffset(f, testName, offsetTime)
	assertResetOffsetSucceeded(f, testName)
	assertEventsReceived(f, receiverName, event, eventCount, startId, endId)

	// Return The ReplayEvents Feature
	return f
}
