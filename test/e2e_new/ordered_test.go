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
	"sort"
	"strconv"
	"testing"
	"time"

	"knative.dev/reconciler-test/pkg/environment"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	cetest "github.com/cloudevents/sdk-go/v2/test"
	"github.com/stretchr/testify/require"
	"knative.dev/eventing/test/rekt/resources/broker"
	"knative.dev/eventing/test/rekt/resources/trigger"
	"knative.dev/pkg/system"
	"knative.dev/reconciler-test/pkg/eventshub"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/k8s"
	"knative.dev/reconciler-test/pkg/knative"
	"knative.dev/reconciler-test/resources/svc"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	"knative.dev/eventing-kafka-broker/test/e2e_new/multiple_partition_config"
	"knative.dev/eventing-kafka-broker/test/e2e_new/single_partition_config"

	. "knative.dev/reconciler-test/pkg/eventshub/assert"
)

func TestOrderedDelivery(t *testing.T) {
	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.Managed(t),
	)

	env.Test(ctx, t, SinglePartitionOrderedDelivery())
	env.Test(ctx, t, MultiplePartitionOrderedDelivery())
}

func SinglePartitionOrderedDelivery() *feature.Feature {
	f := feature.NewFeature()

	const responseWaitTime = 100 * time.Millisecond

	sourceName := feature.MakeRandomK8sName("source")
	sinkName := feature.MakeRandomK8sName("sink")
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

	f.Setup("install sink", eventshub.Install(
		sinkName,
		eventshub.StartReceiver,
		eventshub.ResponseWaitTime(responseWaitTime),
	))
	f.Setup("install trigger", trigger.Install(
		triggerName,
		brokerName,
		trigger.WithSubscriber(svc.AsKReference(sinkName), ""),
		trigger.WithAnnotation("kafka.eventing.knative.dev/delivery.order", "ordered"),
	))
	f.Setup("trigger is ready", trigger.IsReady(triggerName))

	f.Setup("install source", eventshub.Install(
		sourceName,
		eventshub.StartSenderToResource(broker.GVR(), brokerName),
		eventshub.InputEventWithEncoding(ev, cloudevents.EncodingBinary),
		eventshub.AddSequence,
		eventshub.SendMultipleEvents(20, 100*time.Millisecond),
	))

	f.Assert("receive events in order", assertDeliveryOrderAndTiming(
		sinkName,
		20,
		responseWaitTime,
		cetest.ContainsExtensions("sequence"),
	))

	return f
}

func MultiplePartitionOrderedDelivery() *feature.Feature {
	f := feature.NewFeature()

	const responseWaitTime = 100 * time.Millisecond

	sinkName := feature.MakeRandomK8sName("sink")
	triggerName := feature.MakeRandomK8sName("trigger")
	brokerName := feature.MakeRandomK8sName("broker")

	evA := cetest.FullEvent()
	evA.SetExtension("partitionkey", "a")
	evB := cetest.FullEvent()
	evB.SetExtension("partitionkey", "b")
	evC := cetest.FullEvent()
	evC.SetExtension("partitionkey", "c")

	f.Setup("install multiple partition configuration", multiple_partition_config.Install)
	f.Setup("install broker", broker.Install(
		brokerName,
		broker.WithBrokerClass(kafka.BrokerClass),
		broker.WithConfig(multiple_partition_config.ConfigMapName),
	))
	f.Setup("broker is ready", broker.IsReady(brokerName))
	f.Setup("broker is addressable", broker.IsAddressable(brokerName))

	f.Setup("install sink", eventshub.Install(
		sinkName,
		eventshub.StartReceiver,
		eventshub.ResponseWaitTime(responseWaitTime),
	))
	f.Setup("install trigger", trigger.Install(
		triggerName,
		brokerName,
		trigger.WithSubscriber(svc.AsKReference(sinkName), ""),
		trigger.WithAnnotation("kafka.eventing.knative.dev/delivery.order", "ordered"),
	))
	f.Setup("trigger is ready", trigger.IsReady(triggerName))

	f.Setup("install source for events keyed 'a'", eventshub.Install(
		feature.MakeRandomK8sName("source-a"),
		eventshub.StartSenderToResource(broker.GVR(), brokerName),
		eventshub.InputEventWithEncoding(evA, cloudevents.EncodingBinary),
		eventshub.AddSequence,
		eventshub.SendMultipleEvents(20, 100*time.Millisecond),
	))
	f.Setup("install source for events keyed 'b'", eventshub.Install(
		feature.MakeRandomK8sName("source-b"),
		eventshub.StartSenderToResource(broker.GVR(), brokerName),
		eventshub.InputEventWithEncoding(evB, cloudevents.EncodingBinary),
		eventshub.AddSequence,
		eventshub.SendMultipleEvents(20, 100*time.Millisecond),
	))
	f.Setup("install source for events keyed 'c'", eventshub.Install(
		feature.MakeRandomK8sName("source-c"),
		eventshub.StartSenderToResource(broker.GVR(), brokerName),
		eventshub.InputEventWithEncoding(evC, cloudevents.EncodingBinary),
		eventshub.AddSequence,
		eventshub.SendMultipleEvents(20, 100*time.Millisecond),
	))

	f.Assert("receive events keyed 'a' in order", assertDeliveryOrderAndTiming(
		sinkName,
		20,
		responseWaitTime,
		cetest.ContainsExtensions("sequence"),
		cetest.HasExtension("partitionkey", "a"),
	))
	f.Assert("receive events keyed 'b' in order", assertDeliveryOrderAndTiming(
		sinkName,
		20,
		responseWaitTime,
		cetest.ContainsExtensions("sequence"),
		cetest.HasExtension("partitionkey", "b"),
	))
	f.Assert("receive events keyed 'c' in order", assertDeliveryOrderAndTiming(
		sinkName,
		20,
		responseWaitTime,
		cetest.ContainsExtensions("sequence"),
		cetest.HasExtension("partitionkey", "c"),
	))

	return f
}

func assertDeliveryOrderAndTiming(sinkName string, expectedNumber int, responseWaitTime time.Duration, matchers ...cetest.EventMatcher) feature.StepFn {
	return func(ctx context.Context, t feature.T) {
		events := eventshub.StoreFromContext(ctx, sinkName).AssertExact(
			t,
			expectedNumber,
			MatchKind(EventReceived),
			MatchEvent(matchers...),
		)

		// Check we received exactly expectedNumber and no more
		require.Len(t, events, expectedNumber)

		// Now we need to check we received these in order
		sort.SliceStable(events, func(i, j int) bool {
			return events[i].Time.Before(events[j].Time)
		})

		// Test sequence
		for i, event := range events {
			expectedSequence := i + 1 // sequence is 1 indexed
			var actualSequenceStr string
			err := event.Event.ExtensionAs("sequence", &actualSequenceStr)
			require.NoError(t, err)
			actualSequence, err := strconv.Atoi(actualSequenceStr)
			require.NoError(t, err)
			require.Equal(t, expectedSequence, actualSequence, "events: %+v", events)
		}

		// Test timings: because events are ordered, and the receiver pauses for 100 ms,
		// their time should be > ~100 ms distant

		// Assuming 10ms is the clock skew (highly improbable on a local cluster)
		const clockSkew = 10 * time.Millisecond

		prev := events[0].Time
		for _, event := range events[1:] {
			require.True(t, prev.Before(event.Time), "EventInfo.Time should be before the previous EventInfo.Time: %s < %s", prev, event.Time)
			require.Greater(t, event.Time.Sub(prev)+clockSkew, responseWaitTime)

			prev = event.Time
		}
	}
}
