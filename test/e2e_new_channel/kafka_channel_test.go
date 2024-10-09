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

package e2e_new_channel

import (
	"testing"
	"time"

	"knative.dev/eventing/test/rekt/features/channel"
	"knative.dev/eventing/test/rekt/features/oidc"

	"knative.dev/pkg/system"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/eventshub"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/k8s"
	"knative.dev/reconciler-test/pkg/knative"
	"knative.dev/reconciler-test/pkg/state"

	"knative.dev/eventing-kafka-broker/test/rekt/features"
	"knative.dev/eventing-kafka-broker/test/rekt/features/kafkachannel"
	kafkachannelresource "knative.dev/eventing-kafka-broker/test/rekt/resources/kafkachannel"
	channelresource "knative.dev/eventing/test/rekt/resources/channel"

	"knative.dev/eventing/test/rekt/features/authz"
)

const (
	kafkaChannelTestPrefix = "kc-rekt-test-"
)

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

func TestKafkaChannelDispatcherAuthenticatesWithOIDC(t *testing.T) {
	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.Managed(t),
		eventshub.WithTLS(t),
	)

	env.Test(ctx, t, channel.DispatcherAuthenticatesRequestsWithOIDC())
}

func TestKafkaChannelOIDC(t *testing.T) {
	// Run Test In Parallel With Others
	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.WithPollTimings(2*time.Second, 12*time.Minute),
		environment.Managed(t),
		eventshub.WithTLS(t),
	)

	name := feature.MakeRandomK8sName("kafkaChannel")
	env.Prerequisite(ctx, t, channel.ImplGoesReady(name))

	env.TestSet(ctx, t, oidc.AddressableOIDCConformance(kafkachannelresource.GVR(), "KafkaChannel", name, env.Namespace()))
}

func TestChannelWithBackingKafkaChannelSupportsAuthZ(t *testing.T) {
	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.Managed(t),
		eventshub.WithTLS(t),
	)

	name := feature.MakeRandomK8sName("channel")
	env.Prerequisite(ctx, t, channel.GoesReady(name))

	env.TestSet(ctx, t, authz.AddressableAuthZConformance(channelresource.GVR(), "Channel", name))
}

func TestKafkaChannelSupportsAuthZ(t *testing.T) {
	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.Managed(t),
		eventshub.WithTLS(t),
	)

	name := feature.MakeRandomK8sName("kafkachannel")
	env.Prerequisite(ctx, t, channel.ImplGoesReady(name))

	env.TestSet(ctx, t, authz.AddressableAuthZConformance(kafkachannelresource.GVR(), "KafkaChannel", name))
}

func TestKafkaChannelKedaScaling(t *testing.T) {
	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.WithPollTimings(2*time.Second, 12*time.Minute),
		environment.Managed(t),
	)

	env.Test(ctx, t, features.ChannelScalesToZeroWithKeda())
}
