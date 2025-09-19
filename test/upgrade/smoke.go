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
	"context"
	"sync"

	"github.com/google/uuid"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	"knative.dev/eventing-kafka-broker/test/rekt/features"
	brokerfeatures "knative.dev/eventing/test/rekt/features/broker"
	channelfeatures "knative.dev/eventing/test/rekt/features/channel"
	"knative.dev/eventing/test/rekt/features/knconf"
	brokerresources "knative.dev/eventing/test/rekt/resources/broker"
	"knative.dev/eventing/test/rekt/resources/channel_impl"
	"knative.dev/eventing/test/rekt/resources/subscription"
	eventingupgrade "knative.dev/eventing/test/upgrade"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/system"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/eventshub"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/k8s"
	"knative.dev/reconciler-test/pkg/knative"
	"knative.dev/reconciler-test/pkg/manifest"
)

var (
	brokerConfigMux  = &sync.Mutex{}
	channelConfigMux = &sync.Mutex{}
	opts             = []environment.EnvOpts{
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithObservabilityConfig,
		k8s.WithEventListener,
	}
)

func KafkaChannelFeature(glob environment.GlobalEnvironment) *eventingupgrade.DurableFeature {
	// Prevent race conditions on channel_impl.EnvCfg.ChannelGK when running tests in parallel.
	channelConfigMux.Lock()
	defer channelConfigMux.Unlock()
	channel_impl.EnvCfg.ChannelGK = "KafkaChannel.messaging.knative.dev"
	channel_impl.EnvCfg.ChannelV = "v1beta1"

	createSubscriberFn := func(ref *duckv1.KReference, uri string) manifest.CfgFn {
		return subscription.WithSubscriber(ref, uri, "")
	}

	setupF := feature.NewFeature()
	sink, ch := channelfeatures.ChannelChainSetup(setupF, 1, createSubscriberFn)

	verifyF := func() *feature.Feature {
		f := feature.NewFeatureNamed(setupF.Name)
		channelfeatures.ChannelChainAssert(f, sink, ch)
		return f
	}

	return &eventingupgrade.DurableFeature{SetupF: setupF, VerifyF: verifyF, Global: glob, EnvOpts: opts}
}

func KafkaSinkSourceBinaryEventFeature(glob environment.GlobalEnvironment,
) *eventingupgrade.DurableFeature {
	setupF := feature.NewFeature()
	kafkaSink, receiver := features.KafkaSourceBinaryEventFeatureSetup(setupF)

	verifyF := func() *feature.Feature {
		f := feature.NewFeatureNamed(setupF.Name)
		features.KafkaSourceFeatureAssert(f, kafkaSink, receiver, features.KafkaSourceBinaryEventCustomizeFunc())
		return f
	}

	return &eventingupgrade.DurableFeature{SetupF: setupF, VerifyF: verifyF, Global: glob, EnvOpts: opts}
}

func KafkaSinkSourceStructuredEventFeature(glob environment.GlobalEnvironment,
) *eventingupgrade.DurableFeature {
	setupF := feature.NewFeature()
	kafkaSink, receiver := features.KafkaSourceStructuredEventFeatureSetup(setupF)
	verifyF := func() *feature.Feature {
		f := feature.NewFeatureNamed(setupF.Name)
		features.KafkaSourceFeatureAssert(f, kafkaSink, receiver, features.KafkaSourceStructuredEventCustomizeFunc())
		return f
	}

	return &eventingupgrade.DurableFeature{SetupF: setupF, VerifyF: verifyF, Global: glob, EnvOpts: opts}
}

func BrokerEventTransformationForTrigger(glob environment.GlobalEnvironment,
) *eventingupgrade.DurableFeature {
	// Prevent race conditions on EnvCfg.BrokerClass when running tests in parallel.
	brokerConfigMux.Lock()
	defer brokerConfigMux.Unlock()
	brokerresources.EnvCfg.BrokerClass = kafka.BrokerClass

	setupF := feature.NewFeature()
	cfg := brokerfeatures.BrokerEventTransformationForTriggerSetup(setupF)

	verifyF := func() *feature.Feature {
		f := feature.NewFeatureNamed(setupF.Name)
		brokerfeatures.BrokerEventTransformationForTriggerAssert(f, cfg)
		return f
	}

	return &eventingupgrade.DurableFeature{SetupF: setupF, VerifyF: verifyF, Global: glob, EnvOpts: opts}
}

func NamespacedBrokerEventTransformationForTrigger(glob environment.GlobalEnvironment,
) *eventingupgrade.DurableFeature {
	// Prevent race conditions on EnvCfg.BrokerClass when running tests in parallel.
	brokerConfigMux.Lock()
	defer brokerConfigMux.Unlock()
	brokerresources.EnvCfg.BrokerClass = kafka.NamespacedBrokerClass

	broker := "broker"
	setupF := features.SetupNamespacedBroker(broker)
	// Override name to match the enclosing function name.
	setupF.Name = feature.NewFeature().Name

	verifyF := func() *feature.Feature {
		f := feature.NewFeatureNamed(setupF.Name)
		brokerAcceptsBinaryContentModeAssert(f, broker)
		return f
	}

	return &eventingupgrade.DurableFeature{SetupF: setupF, VerifyF: verifyF, Global: glob, EnvOpts: opts}
}

func brokerAcceptsBinaryContentModeAssert(f *feature.Feature, brokerName string) {
	f.Assert("broker accepts binary content mode", func(ctx context.Context, t feature.T) {
		source := feature.MakeRandomK8sName("source")
		eventshub.Install(source,
			eventshub.StartSenderToResource(brokerresources.GVR(), brokerName),
			eventshub.InputHeader("ce-specversion", "1.0"),
			eventshub.InputHeader("ce-type", "sometype"),
			eventshub.InputHeader("ce-source", "200.request.sender.test.knative.dev"),
			eventshub.InputHeader("ce-id", uuid.New().String()),
			eventshub.InputHeader("content-type", "application/json"),
			eventshub.InputBody("{}"),
			eventshub.InputMethod("POST"),
		)(ctx, t)

		store := eventshub.StoreFromContext(ctx, source)
		events := knconf.Correlate(store.AssertAtLeast(ctx, t, 2, knconf.SentEventMatcher("")))
		for _, e := range events {
			if e.Response.StatusCode < 200 || e.Response.StatusCode > 299 {
				t.Errorf("Expected statuscode 2XX for sequence %d got %d", e.Response.Sequence, e.Response.StatusCode)
			}
		}
	})
}
