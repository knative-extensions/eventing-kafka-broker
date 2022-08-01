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
	"fmt"
	"testing"
	"time"

	cetest "github.com/cloudevents/sdk-go/v2/test"
	"github.com/openzipkin/zipkin-go/model"
	tracinghelper "knative.dev/eventing/test/conformance/helpers/tracing"
	"knative.dev/eventing/test/rekt/resources/broker"
	"knative.dev/eventing/test/rekt/resources/trigger"
	"knative.dev/pkg/system"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/eventshub"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/k8s"
	"knative.dev/reconciler-test/pkg/knative"
	"knative.dev/reconciler-test/resources/svc"

	. "knative.dev/reconciler-test/pkg/eventshub/assert"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	"knative.dev/eventing-kafka-broker/test/pkg/tracing"
)

func TestTracingHeaders(t *testing.T) {
	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		tracing.WithZipkin,
		environment.Managed(t),
	)

	env.Test(ctx, t, TracingHeadersUsingOrderedDeliveryWithTraceExported())
	env.Test(ctx, t, TracingHeadersUsingUnorderedDelivery())
	env.Test(ctx, t, TracingHeadersUsingUnorderedDeliveryWithMultipleTriggers())
}

func TracingHeadersUsingOrderedDeliveryWithTraceExported() *feature.Feature {
	f := feature.NewFeature()

	sourceName := feature.MakeRandomK8sName("source")
	sinkName := feature.MakeRandomK8sName("sink")
	triggerName := feature.MakeRandomK8sName("trigger")
	brokerName := feature.MakeRandomK8sName("broker")

	ev := cetest.FullEvent()
	ev.SetID("full-event-ordered")
	ev.SetSource(sourceName)

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

	f.Requirement("install source", eventshub.Install(
		sourceName,
		eventshub.StartSenderToResource(broker.GVR(), brokerName),
		eventshub.InputEvent(ev),
		eventshub.AddTracing,
		// Send at least two events to workaround https://github.com/knative/pkg/issues/2475.
		// There's some time needed for exporting the trace to Zipkin. Sending two events with
		// some delay gives the exporter time to export the trace for the first event. The sender
		// is shutdown immediately after sending the last event so the trace for the last
		// event will probably not be exported.
		eventshub.SendMultipleEvents(2, 3*time.Second),
	))

	f.Assert("received event has traceparent header",
		OnStore(sinkName).
			Match(MatchKind(EventReceived), hasTraceparentHeader).
			AtLeast(1),
	)

	f.Assert("event trace exported", brokerHasMatchingTraceTree(sourceName, sinkName, brokerName, ev.ID()))

	return f
}

func brokerHasMatchingTraceTree(sourceName, sinkName, brokerName, eventID string) func(ctx context.Context, t feature.T) {
	return func(ctx context.Context, t feature.T) {
		testNS := environment.FromContext(ctx).Namespace()
		systemNS := knative.KnativeNamespaceFromContext(ctx)
		expectedTree := tracinghelper.TestSpanTree{
			Note: "1. Sender pod sends event to the Broker Ingress",
			Span: tracinghelper.MatchHTTPSpanNoReply(
				model.Client,
				tracinghelper.WithHTTPURL(
					fmt.Sprintf("kafka-broker-ingress.%s.svc", systemNS),
					fmt.Sprintf("/%s/%s", testNS, brokerName),
				),
				tracinghelper.WithLocalEndpointServiceName(sourceName),
			),
			Children: []tracinghelper.TestSpanTree{
				{
					Note: "2. Kafka Broker Receiver getting the message",
					Span: tracinghelper.MatchHTTPSpanNoReply(
						model.Server,
						tracinghelper.WithLocalEndpointServiceName("kafka-broker-receiver"),
						tracing.WithMessageIDSource(eventID, sourceName),
					),
					Children: []tracinghelper.TestSpanTree{
						{
							Note: "3. Kafka Broker Receiver storing message to Kafka",
							Span: tracinghelper.MatchSpan(
								model.Producer,
								tracinghelper.WithLocalEndpointServiceName("kafka-broker-receiver"),
							),
							Children: []tracinghelper.TestSpanTree{
								{
									Note: "4. Kafka Broker Dispatcher reading message from Kafka",
									Span: tracinghelper.MatchSpan(
										model.Consumer,
										tracinghelper.WithLocalEndpointServiceName("kafka-broker-dispatcher"),
										tracing.WithMessageIDSource(eventID, sourceName),
									),
									Children: []tracinghelper.TestSpanTree{
										{
											Note: "5. Kafka Broker Dispatcher sending message to sink",
											Span: tracinghelper.MatchHTTPSpanNoReply(
												model.Client,
												tracinghelper.WithHTTPURL(
													fmt.Sprintf("%s.%s.svc", sinkName, testNS),
													"/",
												),
												tracinghelper.WithLocalEndpointServiceName("kafka-broker-dispatcher"),
											),
											Children: []tracinghelper.TestSpanTree{
												{
													Note: "6. The target Pod receiving message",
													Span: tracinghelper.MatchHTTPSpanNoReply(
														model.Server,
														tracinghelper.WithHTTPHostAndPath(
															fmt.Sprintf("%s.%s.svc", sinkName, testNS),
															"/",
														),
														tracinghelper.WithLocalEndpointServiceName(sinkName),
													),
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		}
		eventshub.StoreFromContext(ctx, sinkName).AssertAtLeast(t, 1,
			MatchKind(EventReceived),
			tracing.TraceTreeMatches(sourceName, eventID, expectedTree),
		)
	}
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

	f.Requirement("install source", eventshub.Install(
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

	f.Requirement("install source", eventshub.Install(
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
