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
	"testing"

	"knative.dev/reconciler-test/pkg/environment"

	cetest "github.com/cloudevents/sdk-go/v2/test"
	"knative.dev/eventing/test/rekt/resources/broker"
	"knative.dev/eventing/test/rekt/resources/trigger"
	"knative.dev/pkg/system"
	"knative.dev/reconciler-test/pkg/eventshub"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/k8s"
	"knative.dev/reconciler-test/pkg/knative"
	"knative.dev/reconciler-test/resources/svc"

	. "knative.dev/reconciler-test/pkg/eventshub/assert"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
)

func TestDeadLetterSink(t *testing.T) {
	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.Managed(t),
	)

	env.Test(ctx, t, SendsEventWithRetries())
	env.Test(ctx, t, SendsEventNoRetries())
}

func SendsEventWithRetries() *feature.Feature {
	f := feature.NewFeature()

	sourceName := feature.MakeRandomK8sName("source")
	sinkName := feature.MakeRandomK8sName("sink")
	deadLetterSinkName := feature.MakeRandomK8sName("dls")
	triggerName := feature.MakeRandomK8sName("trigger")
	brokerName := feature.MakeRandomK8sName("broker")

	ev := cetest.FullEvent()

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
		eventshub.DropEventsResponseCode(429), // retry error
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

	f.Setup("install source", eventshub.Install(
		sourceName,
		eventshub.StartSenderToResource(broker.GVR(), brokerName),
		eventshub.InputEvent(ev),
	))

	f.Assert("receive event on sink",
		OnStore(sinkName).
			MatchEvent(cetest.HasId(ev.ID())).
			Exact(1),
	)

	f.Assert("receive event on dead letter sink",
		OnStore(deadLetterSinkName).
			MatchEvent(cetest.HasId(ev.ID())).
			Exact(1),
	)

	return f
}

func SendsEventNoRetries() *feature.Feature {
	f := feature.NewFeature()

	sourceName := feature.MakeRandomK8sName("source")
	sinkName := feature.MakeRandomK8sName("sink")
	deadLetterSinkName := feature.MakeRandomK8sName("dls")
	triggerName := feature.MakeRandomK8sName("trigger")
	brokerName := feature.MakeRandomK8sName("broker")

	ev := cetest.FullEvent()

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

	f.Setup("install source", eventshub.Install(
		sourceName,
		eventshub.StartSenderToResource(broker.GVR(), brokerName),
		eventshub.InputEvent(ev),
	))

	f.Assert("receive event on sink",
		OnStore(sinkName).
			MatchEvent(cetest.HasId(ev.ID())).
			Exact(1),
	)

	f.Assert("receive event on dead letter sink",
		OnStore(deadLetterSinkName).
			Exact(0),
	)

	return f
}
