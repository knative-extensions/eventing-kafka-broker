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
	"context"
	"strings"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	cetest "github.com/cloudevents/sdk-go/v2/test"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	"knative.dev/eventing-kafka-broker/test/e2e_new/single_partition_config"
	"knative.dev/eventing/test/rekt/resources/broker"
	"knative.dev/eventing/test/rekt/resources/trigger"
	"knative.dev/pkg/system"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/eventshub"
	. "knative.dev/reconciler-test/pkg/eventshub/assert"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/k8s"
	"knative.dev/reconciler-test/pkg/knative"
	"knative.dev/reconciler-test/resources/svc"
)

func TestDeadLetterSinkExtensions(t *testing.T) {
	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.Managed(t),
	)

	env.Test(ctx, t, SubscriberUnreachable())
	env.Test(ctx, t, SubscriberReturnedErrorNoData())
	env.Test(ctx, t, SubscriberReturnedErrorSmallData())
	env.Test(ctx, t, SubscriberReturnedErrorLargeData())
}

func SubscriberUnreachable() *feature.Feature {
	f := feature.NewFeature()

	sourceName := feature.MakeRandomK8sName("source")
	deadLetterSinkName := feature.MakeRandomK8sName("dls")
	triggerName := feature.MakeRandomK8sName("trigger")
	brokerName := feature.MakeRandomK8sName("broker")

	ev := cetest.FullEvent()

	f.Setup("install one partition configuration", single_partition_config.Install)
	f.Setup("install broker", broker.Install(
		brokerName,
		broker.WithBrokerClass(kafka.BrokerClass),
		broker.WithConfig(single_partition_config.ConfigMapName),
	))
	f.Setup("broker is ready", broker.IsReady(brokerName))
	f.Setup("broker is addressable", broker.IsAddressable(brokerName))

	f.Setup("install dead letter sink", eventshub.Install(
		deadLetterSinkName,
		eventshub.StartReceiver,
	))
	f.Setup("install trigger", trigger.Install(
		triggerName,
		brokerName,
		trigger.WithSubscriber(nil, "http://fake.svc.cluster.local"),
		trigger.WithDeadLetterSink(svc.AsKReference(deadLetterSinkName), ""),
	))
	f.Setup("trigger is ready", trigger.IsReady(triggerName))

	f.Requirement("install source", eventshub.Install(
		sourceName,
		eventshub.StartSenderToResource(broker.GVR(), brokerName),
		eventshub.InputEventWithEncoding(ev, cloudevents.EncodingBinary),
		eventshub.AddSequence,
		eventshub.SendMultipleEvents(1, 100*time.Millisecond),
	))

	f.Assert("knativeerrordest added", assertEnhancedWithKnativeErrorExtensions(
		deadLetterSinkName,
		func(ctx context.Context) cetest.EventMatcher {
			return cetest.HasExtension("knativeerrordest", "http://fake.svc.cluster.local")
		},
	))

	return f
}

func SubscriberReturnedErrorNoData() *feature.Feature {
	f := feature.NewFeature()

	sourceName := feature.MakeRandomK8sName("source")
	sinkName := feature.MakeRandomK8sName("sink")
	deadLetterSinkName := feature.MakeRandomK8sName("dls")
	triggerName := feature.MakeRandomK8sName("trigger")
	brokerName := feature.MakeRandomK8sName("broker")

	ev := cetest.FullEvent()

	f.Setup("install one partition configuration", single_partition_config.Install)
	f.Setup("install broker", broker.Install(
		brokerName,
		broker.WithBrokerClass(kafka.BrokerClass),
	))

	f.Setup("broker is ready", broker.IsReady(brokerName))
	f.Setup("broker is addressable", broker.IsAddressable(brokerName))

	f.Setup("install sink", eventshub.Install(
		sinkName,
		eventshub.StartReceiver,
		eventshub.DropFirstN(1),
		eventshub.DropEventsResponseCode(422), // retry error
	))

	f.Setup("install dead letter sink", eventshub.Install(
		deadLetterSinkName,
		eventshub.StartReceiver,
	))

	f.Setup("install trigger", trigger.Install(
		triggerName,
		brokerName,
		trigger.WithSubscriber(svc.AsKReference(sinkName), ""),
		trigger.WithDeadLetterSink(svc.AsKReference(deadLetterSinkName), ""),
	))
	f.Setup("trigger is ready", trigger.IsReady(triggerName))

	f.Requirement("install source", eventshub.Install(
		sourceName,
		eventshub.StartSenderToResource(broker.GVR(), brokerName),
		eventshub.InputEvent(ev),
	))

	f.Assert("knativeerrordest & knativeerrorcode added", assertEnhancedWithKnativeErrorExtensions(
		deadLetterSinkName,
		func(ctx context.Context) cetest.EventMatcher {
			sinkAddress, _ := svc.Address(ctx, sinkName)
			return cetest.HasExtension("knativeerrordest", sinkAddress.String())
		},

		func(ctx context.Context) cetest.EventMatcher {
			return cetest.HasExtension("knativeerrorcode", "422")
		},
	))

	return f
}

func SubscriberReturnedErrorSmallData() *feature.Feature {
	f := feature.NewFeature()

	sourceName := feature.MakeRandomK8sName("source")
	sinkName := feature.MakeRandomK8sName("sink")
	deadLetterSinkName := feature.MakeRandomK8sName("dls")
	triggerName := feature.MakeRandomK8sName("trigger")
	brokerName := feature.MakeRandomK8sName("broker")

	ev := cetest.FullEvent()

	f.Setup("install one partition configuration", single_partition_config.Install)
	f.Setup("install broker", broker.Install(
		brokerName,
		broker.WithBrokerClass(kafka.BrokerClass),
		broker.WithConfig(single_partition_config.ConfigMapName),
	))
	f.Setup("broker is ready", broker.IsReady(brokerName))
	f.Setup("broker is addressable", broker.IsAddressable(brokerName))

	errorData := `{ "message": "catastrophic failure" }`
	f.Setup("install sink", eventshub.Install(
		sinkName,
		eventshub.StartReceiver,
		eventshub.DropFirstN(1),
		eventshub.DropEventsResponseCode(422),
		eventshub.DropEventsResponseBody(errorData),
	))
	f.Setup("install dead letter sink", eventshub.Install(
		deadLetterSinkName,
		eventshub.StartReceiver,
	))
	f.Setup("install trigger", trigger.Install(
		triggerName,
		brokerName,
		trigger.WithSubscriber(svc.AsKReference(sinkName), ""),
		trigger.WithDeadLetterSink(svc.AsKReference(deadLetterSinkName), ""),
	))
	f.Setup("trigger is ready", trigger.IsReady(triggerName))

	f.Requirement("install source", eventshub.Install(
		sourceName,
		eventshub.StartSenderToResource(broker.GVR(), brokerName),
		eventshub.InputEvent(ev),
	))

	f.Assert("knativeerrordest, knativeerrorcode, knativeerrordata added", assertEnhancedWithKnativeErrorExtensions(
		deadLetterSinkName,
		func(ctx context.Context) cetest.EventMatcher {
			sinkAddress, _ := svc.Address(ctx, sinkName)
			return cetest.HasExtension("knativeerrordest", sinkAddress.String())
		},
		func(ctx context.Context) cetest.EventMatcher {
			return cetest.HasExtension("knativeerrorcode", "422")
		},
		func(ctx context.Context) cetest.EventMatcher {
			return cetest.HasExtension("knativeerrordata", errorData)
		},
	))

	return f
}

func SubscriberReturnedErrorLargeData() *feature.Feature {
	f := feature.NewFeature()

	sourceName := feature.MakeRandomK8sName("source")
	sinkName := feature.MakeRandomK8sName("sink")
	deadLetterSinkName := feature.MakeRandomK8sName("dls")
	triggerName := feature.MakeRandomK8sName("trigger")
	brokerName := feature.MakeRandomK8sName("broker")

	ev := cetest.FullEvent()

	f.Setup("install one partition configuration", single_partition_config.Install)
	f.Setup("install broker", broker.Install(
		brokerName,
		broker.WithBrokerClass(kafka.BrokerClass),
		broker.WithConfig(single_partition_config.ConfigMapName),
	))
	f.Setup("broker is ready", broker.IsReady(brokerName))
	f.Setup("broker is addressable", broker.IsAddressable(brokerName))

	errorDataTruncated := strings.Repeat("X", 1024)
	errorData := errorDataTruncated + "YYYYYYY"
	f.Setup("install sink", eventshub.Install(
		sinkName,
		eventshub.StartReceiver,
		eventshub.DropFirstN(1),
		eventshub.DropEventsResponseCode(422),
		eventshub.DropEventsResponseBody(errorData),
	))
	f.Setup("install dead letter sink", eventshub.Install(
		deadLetterSinkName,
		eventshub.StartReceiver,
	))
	f.Setup("install trigger", trigger.Install(
		triggerName,
		brokerName,
		trigger.WithSubscriber(svc.AsKReference(sinkName), ""),
		trigger.WithDeadLetterSink(svc.AsKReference(deadLetterSinkName), ""),
	))
	f.Setup("trigger is ready", trigger.IsReady(triggerName))

	f.Requirement("install source", eventshub.Install(
		sourceName,
		eventshub.StartSenderToResource(broker.GVR(), brokerName),
		eventshub.InputEvent(ev),
	))

	f.Assert("knativeerrordest, knativeerrorcode, truncated knativeerrordata added", assertEnhancedWithKnativeErrorExtensions(
		deadLetterSinkName,
		func(ctx context.Context) cetest.EventMatcher {
			sinkAddress, _ := svc.Address(ctx, sinkName)
			return cetest.HasExtension("knativeerrordest", sinkAddress.String())
		},
		func(ctx context.Context) cetest.EventMatcher {
			return cetest.HasExtension("knativeerrorcode", "422")
		},
		func(ctx context.Context) cetest.EventMatcher {
			return cetest.HasExtension("knativeerrordata", errorDataTruncated)
		},
	))

	return f
}

func assertEnhancedWithKnativeErrorExtensions(sinkName string, matcherfns ...func(ctx context.Context) cetest.EventMatcher) feature.StepFn {
	return func(ctx context.Context, t feature.T) {
		matchers := make([]cetest.EventMatcher, len(matcherfns))
		for i, fn := range matcherfns {
			matchers[i] = fn(ctx)
		}
		_ = eventshub.StoreFromContext(ctx, sinkName).AssertExact(
			t,
			1,
			MatchKind(EventReceived),
			MatchEvent(matchers...),
		)
	}
}
