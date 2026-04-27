//go:build e2e
// +build e2e

/*
 * Copyright 2024 The Knative Authors
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

package e2enew

import (
	"context"
	"math"
	"testing"
	"time"

	cetest "github.com/cloudevents/sdk-go/v2/test"
	"k8s.io/utils/ptr"

	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/eventing/test/rekt/resources/broker"
	"knative.dev/eventing/test/rekt/resources/trigger"
	"knative.dev/pkg/system"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/eventshub"
	. "knative.dev/reconciler-test/pkg/eventshub/assert"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/k8s"
	"knative.dev/reconciler-test/pkg/knative"
	"knative.dev/reconciler-test/pkg/resources/service"
)

// TestDeadLetterSinkHTTP500Retry verifies that events sent to a subscriber
// that consistently returns HTTP 500 are retried according to the delivery
// spec and eventually forwarded to the dead letter sink.
//
// Regression test for https://github.com/knative-extensions/eventing-kafka-broker/issues/3746
func TestDeadLetterSinkHTTP500Retry(t *testing.T) {
	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithObservabilityConfig,
		k8s.WithEventListener,
		environment.Managed(t),
		environment.WithPollTimings(5*time.Second, 20*time.Minute),
	)

	env.Test(ctx, t, HTTP500RetriesWithTriggerDelivery())
	env.Test(ctx, t, HTTP500RetriesWithBrokerDelivery())
}

// HTTP500RetriesWithTriggerDelivery verifies that when a Trigger has a delivery
// spec with exponential backoff retries and a dead letter sink, and the
// subscriber always returns HTTP 500, the event is retried the configured
// number of times and then sent to the dead letter sink.
func HTTP500RetriesWithTriggerDelivery() *feature.Feature {
	f := feature.NewFeature()

	sourceName := feature.MakeRandomK8sName("source")
	sinkName := feature.MakeRandomK8sName("sink")
	deadLetterSinkName := feature.MakeRandomK8sName("dls")
	triggerName := feature.MakeRandomK8sName("trigger")
	brokerName := feature.MakeRandomK8sName("broker")

	retryCount := int32(12)
	backoffPolicy := eventingduck.BackoffPolicyExponential

	ev := cetest.FullEvent()

	f.Setup("install broker", broker.Install(
		brokerName,
		broker.WithEnvConfig()...,
	))

	f.Setup("install sink that always returns HTTP 500", eventshub.Install(
		sinkName,
		eventshub.StartReceiver,
		eventshub.DropFirstN(math.MaxUint32),
		eventshub.DropEventsResponseCode(500),
	))

	f.Setup("install dead letter sink", eventshub.Install(
		deadLetterSinkName,
		eventshub.StartReceiver,
	))

	f.Setup("install trigger with exponential retry and DLS", trigger.Install(
		triggerName,
		trigger.WithBrokerName(brokerName),
		trigger.WithSubscriber(service.AsKReference(sinkName), ""),
		trigger.WithRetry(retryCount, &backoffPolicy, ptr.To("PT0.1S")),
		trigger.WithDeadLetterSink(service.AsKReference(deadLetterSinkName), ""),
	))

	f.Requirement("install source", func(ctx context.Context, t feature.T) {
		broker.IsReady(brokerName)(ctx, t)
		broker.IsAddressable(brokerName)(ctx, t)
		trigger.IsReady(triggerName)(ctx, t)
		eventshub.Install(
			sourceName,
			eventshub.StartSenderToResource(broker.GVR(), brokerName),
			eventshub.InputEvent(ev),
		)(ctx, t)
	})

	f.Assert("subscriber receives 1 original + 12 retries = 13 rejected events",
		OnStore(sinkName).
			MatchRejectedEvent(cetest.HasId(ev.ID())).
			Exact(1+int(retryCount)),
	)

	f.Assert("dead letter sink receives event after retries exhausted",
		OnStore(deadLetterSinkName).
			MatchReceivedEvent(cetest.HasId(ev.ID())).
			Exact(1),
	)

	return f
}

// HTTP500RetriesWithBrokerDelivery verifies that when a Broker has a delivery
// spec with exponential backoff retries and a dead letter sink pointing to
// another Kafka Broker (and the Trigger has no delivery spec), the Trigger
// inherits the Broker's delivery spec. The subscriber always returns HTTP 500,
// and the event should be retried and then forwarded through the DLS Broker.
//
// This is the exact scenario from issue #3746.
func HTTP500RetriesWithBrokerDelivery() *feature.Feature {
	f := feature.NewFeature()

	sourceName := feature.MakeRandomK8sName("source")
	sinkName := feature.MakeRandomK8sName("sink")
	dlsSinkName := feature.MakeRandomK8sName("dlssink")
	triggerName := feature.MakeRandomK8sName("trigger")
	dlsTriggerName := feature.MakeRandomK8sName("dlstrigger")
	brokerName := feature.MakeRandomK8sName("broker")
	dlsBrokerName := feature.MakeRandomK8sName("dlsbroker")

	retryCount := int32(12)
	backoffPolicy := eventingduck.BackoffPolicyExponential

	ev := cetest.FullEvent()

	// Set up the dead letter Kafka Broker with a trigger that routes to an
	// eventshub receiver, so we can observe events that land in the DLS.
	f.Setup("install DLS broker", broker.Install(
		dlsBrokerName,
		broker.WithEnvConfig()...,
	))

	f.Setup("install DLS sink", eventshub.Install(
		dlsSinkName,
		eventshub.StartReceiver,
	))

	f.Setup("install DLS trigger", trigger.Install(
		dlsTriggerName,
		trigger.WithBrokerName(dlsBrokerName),
		trigger.WithSubscriber(service.AsKReference(dlsSinkName), ""),
	))

	// Set up the main Broker with delivery spec pointing to the DLS Broker.
	// The Trigger has no delivery spec and should inherit from the Broker.
	brokerOpts := append(broker.WithEnvConfig(),
		broker.WithRetry(retryCount, &backoffPolicy, ptr.To("PT0.1S")),
		broker.WithDeadLetterSink(broker.AsKReference(dlsBrokerName), ""),
	)

	f.Setup("install broker with exponential retry and Kafka Broker DLS", broker.Install(
		brokerName,
		brokerOpts...,
	))

	f.Setup("install sink that always returns HTTP 500", eventshub.Install(
		sinkName,
		eventshub.StartReceiver,
		eventshub.DropFirstN(math.MaxUint32),
		eventshub.DropEventsResponseCode(500),
	))

	f.Setup("install trigger without delivery spec (inherits from broker)", trigger.Install(
		triggerName,
		trigger.WithBrokerName(brokerName),
		trigger.WithSubscriber(service.AsKReference(sinkName), ""),
	))

	f.Requirement("install source", func(ctx context.Context, t feature.T) {
		broker.IsReady(dlsBrokerName)(ctx, t)
		broker.IsAddressable(dlsBrokerName)(ctx, t)
		trigger.IsReady(dlsTriggerName)(ctx, t)
		broker.IsReady(brokerName)(ctx, t)
		broker.IsAddressable(brokerName)(ctx, t)
		trigger.IsReady(triggerName)(ctx, t)
		eventshub.Install(
			sourceName,
			eventshub.StartSenderToResource(broker.GVR(), brokerName),
			eventshub.InputEvent(ev),
		)(ctx, t)
	})

	f.Assert("subscriber receives 1 original + 12 retries = 13 rejected events",
		OnStore(sinkName).
			MatchRejectedEvent(cetest.HasId(ev.ID())).
			Exact(1+int(retryCount)),
	)

	f.Assert("DLS broker forwards event to DLS sink",
		OnStore(dlsSinkName).
			MatchReceivedEvent(cetest.HasId(ev.ID())).
			Exact(1),
	)

	return f
}
