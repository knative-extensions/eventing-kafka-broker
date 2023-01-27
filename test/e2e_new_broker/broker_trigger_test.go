//go:build e2e
// +build e2e

/*
 * Copyright 2023 The Knative Authors
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

package e2e_broker

import (
	"context"
	"testing"
	"time"

	testpkg "knative.dev/eventing-kafka-broker/test/pkg"
	"knative.dev/pkg/system"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/eventshub"
	"knative.dev/reconciler-test/pkg/eventshub/assert"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/k8s"
	"knative.dev/reconciler-test/pkg/knative"
	"knative.dev/reconciler-test/resources/svc"

	"github.com/cloudevents/sdk-go/v2/test"
	"github.com/google/uuid"
	brokerconfigmap "knative.dev/eventing-kafka-broker/test/rekt/resources/configmap/broker"
	"knative.dev/eventing/test/rekt/resources/broker"
	"knative.dev/eventing/test/rekt/resources/trigger"
)

func TestBrokerTrigger(t *testing.T) {

	const (
		eventSource     = "source1"
		extension1      = "ext1"
		valueExtension1 = "value1"
	)

	// Run Test In Parallel With Others
	t.Parallel()

	//Create The Test Context / Environment
	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.WithPollTimings(3*time.Second, 120*time.Second),
		environment.Managed(t),
	)

	brokerName := feature.MakeRandomK8sName("broker")
	brokerConfig := feature.MakeRandomK8sName("brokercfg")
	senderNameMatchingEvent := feature.MakeRandomK8sName("sender-matching")
	senderNameNonMatchingEvent := feature.MakeRandomK8sName("sender-non-matching")
	triggerName := feature.MakeRandomK8sName("trigger")
	sink := feature.MakeRandomK8sName("sink")

	nonMatchingEventId := uuid.New().String()
	nonMatchingEvent := test.MinEvent()
	nonMatchingEvent.SetID(nonMatchingEventId)
	nonMatchingEvent.SetSource(eventSource)
	nonMatchingEvent.SetExtension(extension1, valueExtension1+"a")

	eventId := uuid.New().String()
	eventToSend := test.MinEvent()
	eventToSend.SetID(eventId)
	eventToSend.SetSource(eventSource)
	eventToSend.SetExtension(extension1, valueExtension1)

	f := feature.NewFeatureNamed("Trigger filters events")

	f.Setup("Create broker config", brokerconfigmap.Install(
		brokerConfig,
		brokerconfigmap.WithBootstrapServer(testpkg.BootstrapServersPlaintext),
		brokerconfigmap.WithNumPartitions(testpkg.NumPartitions),
		brokerconfigmap.WithReplicationFactor(testpkg.ReplicationFactor),
	))

	f.Setup("Install broker", broker.Install(brokerName,
		append(
			broker.WithEnvConfig(),
			broker.WithConfig(brokerConfig))...,
	))
	f.Assert("Broker ready", broker.IsReady(brokerName))

	f.Setup("Install sink", eventshub.Install(sink,
		eventshub.StartReceiver))

	f.Setup("Create trigger", trigger.Install(triggerName, brokerName,
		trigger.WithSubscriber(svc.AsKReference(sink), ""),
		trigger.WithFilter(map[string]string{
			"source":   eventSource,
			extension1: valueExtension1,
			"type":     "",
		}),
	))
	f.Assert("Trigger ready", trigger.IsReady(triggerName))

	f.Requirement("Send matching event", eventshub.Install(senderNameMatchingEvent,
		eventshub.InputEvent(eventToSend),
		eventshub.StartSenderToResource(broker.GVR(), brokerName)))
	f.Assert("Matching event received", assert.OnStore(sink).MatchEvent(test.HasId(eventId)).Exact(1))

	f.Requirement("Send non-matching event", eventshub.Install(senderNameNonMatchingEvent,
		eventshub.InputEvent(nonMatchingEvent),
		eventshub.StartSenderToResource(broker.GVR(), brokerName)))
	f.Assert("Non matching event is not received", func(ctx context.Context, t feature.T) {
		time.Sleep(20 * time.Second)
		assert.OnStore(sink).MatchEvent(test.HasId(nonMatchingEventId)).Not()(ctx, t)
	})

	env.Test(ctx, t, f)
}
