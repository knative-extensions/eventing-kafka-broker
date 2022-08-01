//go:build e2e
// +build e2e

/*
 * Copyright 2022 The Knative Authors
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
	"context"
	"fmt"
	"testing"
	"time"

	cetest "github.com/cloudevents/sdk-go/v2/test"
	"github.com/openzipkin/zipkin-go/model"
	tracinghelper "knative.dev/eventing/test/conformance/helpers/tracing"
	"knative.dev/eventing/test/rekt/resources/channel_impl"
	"knative.dev/eventing/test/rekt/resources/subscription"
	"knative.dev/eventing/test/rekt/resources/svc"
	"knative.dev/pkg/system"
	_ "knative.dev/pkg/system/testing"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/eventshub"
	. "knative.dev/reconciler-test/pkg/eventshub/assert"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/k8s"
	"knative.dev/reconciler-test/pkg/knative"

	"knative.dev/eventing-kafka-broker/test/pkg/tracing"
)

func TestChannelTracing(t *testing.T) {
	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		tracing.WithZipkin,
		environment.Managed(t),
	)

	env.Test(ctx, t, eventWithTraceExported())
}

func eventWithTraceExported() *feature.Feature {
	f := feature.NewFeature()

	sourceName := feature.MakeRandomK8sName("source")
	sinkName := feature.MakeRandomK8sName("sink")
	channelName := feature.MakeRandomK8sName("channel")
	subName := feature.MakeRandomK8sName("subscription")

	f.Setup("install sink", eventshub.Install(
		sinkName,
		eventshub.StartReceiver,
	))
	f.Setup("install channel", channel_impl.Install(channelName))
	f.Setup("install subscription", subscription.Install(subName,
		subscription.WithChannel(channel_impl.AsRef(channelName)),
		subscription.WithSubscriber(svc.AsRef(sinkName), ""),
	))

	f.Setup("subscription is ready", subscription.IsReady(subName))
	f.Setup("channel is ready", channel_impl.IsReady(channelName))

	ev := cetest.FullEvent()
	ev.SetID("full-event-kafka-channel")
	ev.SetSource(sourceName)

	f.Requirement("install source", eventshub.Install(
		sourceName,
		eventshub.StartSenderToResource(channel_impl.GVR(), channelName),
		eventshub.InputEvent(ev),
		eventshub.AddTracing,
		// Send at least two events to workaround https://github.com/knative/pkg/issues/2475.
		// There's some time needed for exporting the trace to Zipkin. Sending two events with
		// some delay gives the exporter time to export the trace for the first event. The sender
		// is shutdown immediately after sending the last event so the trace for the last
		// event will probably not be exported.
		eventshub.SendMultipleEvents(2, 3*time.Second),
	))

	f.Assert("event trace exported", channelHasMatchingTraceTree(sourceName, sinkName, channelName, ev.ID()))

	return f
}

func channelHasMatchingTraceTree(sourceName, sinkName, channelName, eventID string) func(ctx context.Context, t feature.T) {
	return func(ctx context.Context, t feature.T) {
		testNS := environment.FromContext(ctx).Namespace()
		expectedTree := tracinghelper.TestSpanTree{
			Note: "1. Sender pod sends event to the Kafka Channel",
			Span: tracinghelper.MatchHTTPSpanNoReply(
				model.Client,
				tracinghelper.WithHTTPURL(
					fmt.Sprintf("%s-kn-channel.%s.svc", channelName, testNS),
					"",
				),
				tracinghelper.WithLocalEndpointServiceName(sourceName),
			),
			Children: []tracinghelper.TestSpanTree{
				{
					Note: "2. Kafka Channel Receiver getting the message",
					Span: tracinghelper.MatchHTTPSpanNoReply(
						model.Server,
						tracinghelper.WithLocalEndpointServiceName("kafka-channel-receiver"),
						tracing.WithMessageIDSource(eventID, sourceName),
					),
					Children: []tracinghelper.TestSpanTree{
						{
							Note: "3. Kafka Channel Receiver storing message to Kafka",
							Span: tracinghelper.MatchSpan(
								model.Producer,
								tracinghelper.WithLocalEndpointServiceName("kafka-channel-receiver"),
							),
							Children: []tracinghelper.TestSpanTree{
								{
									Note: "4. Kafka Channel Dispatcher reading message from Kafka",
									Span: tracinghelper.MatchSpan(
										model.Consumer,
										tracinghelper.WithLocalEndpointServiceName("kafka-channel-dispatcher"),
										tracing.WithMessageIDSource(eventID, sourceName),
									),
									Children: []tracinghelper.TestSpanTree{
										{
											Note: "5. Kafka Channel Dispatcher sending message to sink",
											Span: tracinghelper.MatchHTTPSpanNoReply(
												model.Client,
												tracinghelper.WithHTTPURL(
													fmt.Sprintf("%s.%s.svc", sinkName, testNS),
													"/",
												),
												tracinghelper.WithLocalEndpointServiceName("kafka-channel-dispatcher"),
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
