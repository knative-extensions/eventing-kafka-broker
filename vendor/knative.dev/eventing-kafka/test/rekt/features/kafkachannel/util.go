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
	"fmt"
	"strconv"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	cloudeventtest "github.com/cloudevents/sdk-go/v2/test"
	subscriptionresources "knative.dev/eventing/test/rekt/resources/subscription"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/reconciler-test/pkg/eventshub"
	"knative.dev/reconciler-test/pkg/eventshub/assert"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/state"

	kafkachannelresources "knative.dev/eventing-kafka/test/rekt/resources/kafkachannel"
	resetoffsetresources "knative.dev/eventing-kafka/test/rekt/resources/resetoffset"
)

const (
	TestNameKey     = "TestNameKey"
	ReceiverNameKey = "ReceiverNameKey"
	SenderNameKey   = "SenderNameKey"
	SenderSinkKey   = "SenderSinkKey"
)

//
// State Utilities
//

// TestName returns the name of the Test associated with the Context for the test.
func TestName(ctx context.Context, t *testing.T) string {
	return state.GetStringOrFail(ctx, t, TestNameKey)
}

// SenderName returns the name of the EventsHub Sender associated with the Context for the test.
func SenderName(ctx context.Context, t *testing.T) string {
	return state.GetStringOrFail(ctx, t, SenderNameKey)
}

// ReceiverName returns the name of the EventsHub Receiver associated with the Context for the test.
func ReceiverName(ctx context.Context, t *testing.T) string {
	return state.GetStringOrFail(ctx, t, ReceiverNameKey)
}

// SenderSink returns the destination sink of the EventsHub Sender's to which events will be sent.
func SenderSink(ctx context.Context, t *testing.T) string {
	return state.GetStringOrFail(ctx, t, SenderSinkKey)
}

//
// Setup() Utilities
//

// setupKafkaChannel adds a Setup() to the specified Feature to create a KafkaChannel.
func setupKafkaChannel(f *feature.Feature, name string) {
	f.Setup("Install A KafkaChannel", kafkachannelresources.Install(name,
		kafkachannelresources.WithNumPartitions("3"),
		kafkachannelresources.WithReplicationFactor("1"),
		kafkachannelresources.WithRetentionDuration("P1D"),
	))
}

// setupSubscription adds a Setup() to the specified Feature to create a Subscription to a KafkaChannel.
func setupSubscription(f *feature.Feature, name string, receiverName string) {
	f.Setup("Install A Subscription", subscriptionresources.Install(name,
		subscriptionresources.WithChannel(&duckv1.KReference{
			Kind:       "KafkaChannel",
			Name:       name,
			APIVersion: kafkachannelresources.GVR().GroupVersion().String(),
		}),
		subscriptionresources.WithSubscriber(&duckv1.KReference{
			Kind:       "Service",
			Name:       receiverName,
			APIVersion: "v1",
		}, ""),
	))
}

// setupResetOffset adds a Setup() to the specified Feature to create a ResetOffset for a KafkaChannel Subscription.
func setupResetOffset(f *feature.Feature, name string, offsetTime string) {
	f.Setup("Install A ResetOffset", resetoffsetresources.Install(name,
		resetoffsetresources.WithOffsetTime(offsetTime),
		resetoffsetresources.WithRef(&duckv1.KReference{
			Kind:       "Subscription",
			Name:       name,
			APIVersion: "messaging.knative.dev/v1", // gvr() in knative.dev/eventing/test/rekt/resources/subscription is private
		}),
	))
}

// setupEventsHubSender adds a Setup() to the specified Feature to create an EventsHub Sender to send CloudEvents.
func setupEventsHubSender(f *feature.Feature, senderName string, senderSink string, event cloudevents.Event, eventCount int) {
	f.Setup("Install An EventsHub Sender", eventshub.Install(senderName,
		eventshub.StartSender(senderSink),
		eventshub.InitialSenderDelay(5*time.Second),
		eventshub.InputEvent(event),
		eventshub.SendMultipleEvents(eventCount, 200*time.Millisecond),
		eventshub.EnableIncrementalId,
	))
}

// setupEventsHubReceiver adds a Setup() to the specified Feature to create an EventsHub Receiver to receive CloudEvents.
func setupEventsHubReceiver(f *feature.Feature, receiverName string) {
	f.Setup("Install An EventsHub Receiver", eventshub.Install(receiverName,
		eventshub.StartReceiver,
		eventshub.EchoEvent,
	))
}

//
// Assert Utilities
//

// assertKafkaChannelReady adds an Assert() to the specified Feature to verify the KafkaChannel is READY.
func assertKafkaChannelReady(f *feature.Feature, name string) {
	f.Assert("KafkaChannel Is Ready", kafkachannelresources.IsReady(name))
}

// assertSubscriptionReady adds an Assert() to the specified Feature to verify the Subscription is READY.
func assertSubscriptionReady(f *feature.Feature, name string) {
	f.Assert("Subscription Is Ready", subscriptionresources.IsReady(name))
}

// assertResetOffsetSucceeded adds an Assert() to the specified Feature to verify the ResetOffset is SUCCEEDED
func assertResetOffsetSucceeded(f *feature.Feature, name string) {
	f.Assert("ResetOffset Is Succeeded", resetoffsetresources.IsSucceeded(name))
}

// assertEventsReceived adds an Assert() to the specified Feature to verify the expected CloudEvents were received successfully.
func assertEventsReceived(f *feature.Feature, receiverName string, event cloudevents.Event, eventCount int, startId int, endId int) {
	matchers := newEventMatcher(event, startId, endId)
	f.Assert("Events Received", assert.OnStore(receiverName).MatchEvent(matchers).Exact(eventCount))
}

//
// CloudEvent Utilities
//

// newEvent returns a new CloudEvent suitable for testing.
func newEvent(name string, senderName string) (cloudevents.Event, error) {
	event := cloudevents.NewEvent()
	event.SetSource(senderName)
	event.SetType(name + "-type")
	event.SetSubject(name + "-subject")
	err := event.SetData(cloudevents.ApplicationJSON, map[string]string{"name": name})
	return event, err
}

// newEventMatcher returns a new CloudEvents Matcher based on the specified CloudEvent.
func newEventMatcher(event cloudevents.Event, startId int, endId int) cloudeventtest.EventMatcher {
	return cloudeventtest.AllOf(
		cloudeventtest.HasSpecVersion(event.SpecVersion()),
		cloudeventtest.HasSource(event.Source()),
		cloudeventtest.HasType(event.Type()),
		cloudeventtest.HasSubject(event.Subject()),
		cloudeventtest.HasDataContentType(event.DataContentType()),
		cloudeventtest.HasData(event.Data()),
		eventIdRangeMatcher(startId, endId),
	)
}

// eventIdRangeMatcher returns an EventMatcher capable of verifying CloudEvent IDs are within a certain range.
func eventIdRangeMatcher(startId int, endId int) cloudeventtest.EventMatcher {
	return func(have cloudevents.Event) error {
		eventId, err := strconv.Atoi(have.ID())
		if err != nil {
			return err
		} else if eventId < startId || eventId > endId {
			return fmt.Errorf("event ID '%d' outside expected range (%d - %d)", eventId, startId, endId)
		}
		return nil
	}
}
