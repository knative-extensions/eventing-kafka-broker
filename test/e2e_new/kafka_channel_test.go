//go:build e2e
// +build e2e

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

package e2e_new

import (
	"fmt"
	"testing"
	"time"

	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/constants"

	"knative.dev/pkg/system"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/k8s"
	"knative.dev/reconciler-test/pkg/knative"
	"knative.dev/reconciler-test/pkg/state"

	"knative.dev/eventing-kafka/test/rekt/features/kafkachannel"
)

const kafkaChannelTestPrefix = "kc-rekt-test-"

func TestKafkaChannelReadiness(t *testing.T) {

	// Run Test In Parallel With Others
	t.Parallel()

	// Create The Test Context / Environment
	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.WithPollTimings(3*time.Second, 120*time.Second),
		environment.Managed(t),
	)

	// Generate Unique Test Names And Add To Context Store
	ctx = state.ContextWith(ctx, &state.KVStore{})
	testName := feature.MakeRandomK8sName(kafkaChannelTestPrefix)
	state.SetOrFail(ctx, t, kafkachannel.TestNameKey, testName)
	state.SetOrFail(ctx, t, kafkachannel.ReceiverNameKey, testName+"-receiver")

	// Define The Features To Test
	testFeatures := []*feature.Feature{
		kafkachannel.UnsubscribedKafkaChannelReadiness(ctx, t),
		kafkachannel.SubscribedKafkaChannelReadiness(ctx, t),
	}

	// Test The Features
	for _, f := range testFeatures {
		env.Test(ctx, t, f)
	}
}

func TestKafkaChannelEvents(t *testing.T) {
	t.Skip()

	// Run Test In Parallel With Others
	t.Parallel()

	// Create The Test Context / Environment
	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.WithPollTimings(4*time.Second, 120*time.Second),
		environment.Managed(t),
	)

	// Generate Unique Test Names And Add To Context Store
	ctx = state.ContextWith(ctx, &state.KVStore{})
	testName := feature.MakeRandomK8sName(kafkaChannelTestPrefix)
	kafkaChannelServiceName := appendKafkaChannelServiceNameSuffix(testName)
	state.SetOrFail(ctx, t, kafkachannel.TestNameKey, testName)
	state.SetOrFail(ctx, t, kafkachannel.SenderNameKey, testName+"-sender")
	state.SetOrFail(ctx, t, kafkachannel.SenderSinkKey, kafkaChannelServiceName)
	state.SetOrFail(ctx, t, kafkachannel.ReceiverNameKey, testName+"-receiver")

	// Determine Time Prior To Test To Replay From
	offsetTime := time.Now().Add(-1 * time.Minute).Format(time.RFC3339)

	// Configure DataPlane, Send Events, Replay Events
	env.Test(ctx, t, kafkachannel.ConfigureDataPlane(ctx, t))
	env.Test(ctx, t, kafkachannel.SendEvents(ctx, t, 10, 1, 10))               // 10 Events with IDs 1-10
	env.Test(ctx, t, kafkachannel.ReplayEvents(ctx, t, offsetTime, 20, 1, 10)) // 20 Events with IDs 1-10
}

// appendKafkaChannelServiceNameSuffix appends the KafkaChannel Service name suffix to the specified string.
func appendKafkaChannelServiceNameSuffix(channelName string) string {
	return fmt.Sprintf("%s-%s", channelName, constants.KafkaChannelServiceNameSuffix)
}
