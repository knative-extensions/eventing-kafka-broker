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

	"knative.dev/reconciler-test/pkg/feature"
)

// UnsubscribedKafkaChannelReadiness returns a Feature testing if an unsubscribed KafkaChannel becomes ready.
func UnsubscribedKafkaChannelReadiness(ctx context.Context, t *testing.T) *feature.Feature {
	testName := TestName(ctx, t)
	f := feature.NewFeatureNamed("Unsubscribed KafkaChannel goes ready")
	setupKafkaChannel(f, testName)
	assertKafkaChannelReady(f, testName)
	return f
}

// SubscribedKafkaChannelReadiness returns a Feature testing if a subscribed KafkaChannel becomes ready.
func SubscribedKafkaChannelReadiness(ctx context.Context, t *testing.T) *feature.Feature {
	testName := TestName(ctx, t)
	receiverName := ReceiverName(ctx, t)
	f := feature.NewFeatureNamed("Subscribed KafkaChannel goes ready")
	setupEventsHubReceiver(f, receiverName) // Won't receive anything but required for validated KReference check ; )
	setupKafkaChannel(f, testName)
	setupSubscription(f, testName, receiverName)
	assertKafkaChannelReady(f, testName)
	assertSubscriptionReady(f, testName)
	return f
}
