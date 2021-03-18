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

	cloudevents "github.com/cloudevents/sdk-go/v2"
	cetest "github.com/cloudevents/sdk-go/v2/test"
	"github.com/stretchr/testify/require"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka"
	"knative.dev/eventing/test/rekt/features"
	"knative.dev/eventing/test/rekt/resources/broker"
	"knative.dev/eventing/test/rekt/resources/svc"
	"knative.dev/pkg/system"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/eventshub"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/k8s"
	"knative.dev/reconciler-test/pkg/knative"

	"knative.dev/eventing-kafka-broker/test/e2e_new/trigger"

	. "knative.dev/reconciler-test/pkg/eventshub/assert"
)

func OrderedDelivery() *feature.Feature {
	f := feature.NewFeature()

	sourceName := feature.MakeRandomK8sName("source")
	sinkName := feature.MakeRandomK8sName("sink")
	triggerName := feature.MakeRandomK8sName("trigger")
	brokerName := feature.MakeRandomK8sName("broker")

	ev := cetest.FullEvent()
	// We need to make sure the event goes always in the same partition, because ordering is per partition
	ev.SetExtension("partitionkey", "abc")

	f.Setup("install broker", broker.Install(brokerName, broker.WithBrokerClass(kafka.BrokerClass)))
	f.Setup("broker is ready", broker.IsReady(brokerName))
	f.Setup("broker is addressable", broker.IsAddressable(brokerName, features.Interval, features.Timeout))

	f.Setup("install sink", eventshub.Install(
		sinkName,
		eventshub.StartReceiver,
		eventshub.ResponseWaitTime(100*time.Millisecond),
	))
	f.Setup("install trigger", trigger.Install(
		triggerName,
		brokerName,
		trigger.WithSubscriber(svc.AsRef(sinkName), ""),
		trigger.WithAnnotation("kafka.eventing.knative.dev/delivery.order", "ordered"),
	))
	f.Setup("trigger is ready", trigger.IsReady(triggerName))

	f.Setup("install source", eventshub.Install(
		sourceName,
		eventshub.StartSenderToResource(broker.Gvr(), brokerName),
		eventshub.InputEventWithEncoding(ev, cloudevents.EncodingBinary),
		eventshub.AddSequence,
		eventshub.SendMultipleEvents(20, 100*time.Millisecond),
	))

	f.Assert("receive events in order", func(ctx context.Context, t feature.T) {
		events := eventshub.StoreFromContext(ctx, sinkName).AssertExact(
			20,
			MatchKind(EventReceived),
			MatchEvent(cetest.ContainsExtensions("sequence")),
		)

		// Check we received exactly 20 and no more
		require.Len(t, events, 20)

		// Now we need to check we received these in order
		sort.SliceStable(events, func(i, j int) bool {
			return events[i].Time.Before(events[j].Time)
		})
		for i, event := range events {
			expectedSequence := i + 1 // sequence is 1 indexed
			var actualSequenceStr string
			err := event.Event.ExtensionAs("sequence", &actualSequenceStr)
			require.NoError(t, err)
			actualSequence, err := strconv.Atoi(actualSequenceStr)
			require.NoError(t, err)
			require.Equal(t, expectedSequence, actualSequence, "events: %+v", events)
		}
	})

	return f
}

func TestOrderedDelivery(t *testing.T) {
	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.Managed(t),
	)

	env.Test(ctx, t, OrderedDelivery())
}
