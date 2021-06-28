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

	cetest "github.com/cloudevents/sdk-go/v2/test"
	"knative.dev/eventing/test/rekt/resources/broker"
	"knative.dev/eventing/test/rekt/resources/trigger"
	"knative.dev/pkg/system"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/eventshub"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/k8s"
	"knative.dev/reconciler-test/pkg/knative"
	"knative.dev/reconciler-test/resources/svc"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka"

	. "knative.dev/reconciler-test/pkg/eventshub/assert"
)

func TestTracingHeaders(t *testing.T) {
	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.Managed(t),
	)

	env.Test(ctx, t, TracingHeadersUsingOrderedDelivery())
	env.Test(ctx, t, TracingHeadersUsingUnorderedDelivery())
	env.Test(ctx, t, TracingHeadersUsingUnorderedDeliveryWithMultipleTriggers())
}

func TracingHeadersUsingOrderedDelivery() *feature.Feature {
	f := feature.NewFeature()

	sourceName := feature.MakeRandomK8sName("source")
	sinkName := feature.MakeRandomK8sName("sink")
	triggerName := feature.MakeRandomK8sName("trigger")
	brokerName := feature.MakeRandomK8sName("broker")

	ev := cetest.FullEvent()
	ev.SetID("full-event-ordered")

	f.Setup("install broker", broker.Install(
		brokerName,
		broker.WithBrokerClass(kafka.BrokerClass),
	))
	f.Setup("broker is ready", broker.IsReady(brokerName))
	f.Setup("broker is addressable", broker.IsAddressable(brokerName))

	f.Setup("install sink", eventshub.Install(
		sinkName,
		eventshub.StartReceiver,
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
		eventshub.InputEvent(ev),
		eventshub.AddTracing,
	))

	f.Assert("received event has traceparent header",
		OnStore(sinkName).
			Match(MatchKind(EventReceived), hasTraceparentHeader).
			Exact(1),
	)

	return f
}

func TracingHeadersUsingUnorderedDelivery() *feature.Feature {
	f := feature.NewFeature()

	sourceName := feature.MakeRandomK8sName("source")
	sinkName := feature.MakeRandomK8sName("sink")
	triggerName := feature.MakeRandomK8sName("trigger")
	brokerName := feature.MakeRandomK8sName("broker")

	ev := cetest.FullEvent()
	ev.SetID("full-event-unordered")

	f.Setup("install broker", broker.Install(
		brokerName,
		broker.WithBrokerClass(kafka.BrokerClass),
	))
	f.Setup("broker is ready", broker.IsReady(brokerName))
	f.Setup("broker is addressable", broker.IsAddressable(brokerName))

	f.Setup("install sink", eventshub.Install(
		sinkName,
		eventshub.StartReceiver,
	))
	f.Setup("install trigger", trigger.Install(
		triggerName,
		brokerName,
		trigger.WithSubscriber(svc.AsKReference(sinkName), ""),
	))
	f.Setup("trigger is ready", trigger.IsReady(triggerName))

	f.Setup("install source", eventshub.Install(
		sourceName,
		eventshub.StartSenderToResource(broker.GVR(), brokerName),
		eventshub.InputEvent(ev),
		eventshub.AddTracing,
	))

	f.Assert("received event has traceparent header",
		OnStore(sinkName).
			Match(MatchKind(EventReceived), hasTraceparentHeader).
			Exact(1),
	)

	return f
}

func TracingHeadersUsingUnorderedDeliveryWithMultipleTriggers() *feature.Feature {
	f := feature.NewFeature()

	sourceName := feature.MakeRandomK8sName("source")
	sinkName := feature.MakeRandomK8sName("sink")
	triggerAName := feature.MakeRandomK8sName("trigger-a")
	triggerBName := feature.MakeRandomK8sName("trigger-b")
	brokerName := feature.MakeRandomK8sName("broker")

	ev := cetest.FullEvent()
	ev.SetID("full-event-unordered")

	f.Setup("install broker", broker.Install(
		brokerName,
		broker.WithBrokerClass(kafka.BrokerClass),
	))
	f.Setup("broker is ready", broker.IsReady(brokerName))
	f.Setup("broker is addressable", broker.IsAddressable(brokerName))

	f.Setup("install sink", eventshub.Install(
		sinkName,
		eventshub.StartReceiver,
	))
	f.Setup("install trigger a", trigger.Install(
		triggerAName,
		brokerName,
		trigger.WithSubscriber(svc.AsKReference(sinkName), ""),
	))
	f.Setup("install trigger b", trigger.Install(
		triggerBName,
		brokerName,
		trigger.WithSubscriber(svc.AsKReference(sinkName), ""),
	))
	f.Setup("trigger is ready", trigger.IsReady(triggerAName))

	f.Setup("install source", eventshub.Install(
		sourceName,
		eventshub.StartSenderToResource(broker.GVR(), brokerName),
		eventshub.InputEvent(ev),
		eventshub.AddTracing,
		eventshub.SendMultipleEvents(5, time.Millisecond),
	))

	f.Assert("received event has traceparent header",
		OnStore(sinkName).
			Match(MatchKind(EventReceived), hasTraceparentHeader).
			Exact(10),
	)

	return f
}

func hasTraceparentHeader(info eventshub.EventInfo) error {
	if _, ok := info.HTTPHeaders["Traceparent"]; !ok {
		return fmt.Errorf("HTTP Headers does not contain the 'Traceparent' header")
	}
	return nil
}
