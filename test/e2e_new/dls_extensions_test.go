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
	"testing"
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
	//env.Test(ctx, t, SubscriberReturnedErrorSmallData())
	//env.Test(ctx, t, SubscriberReturnedErrorLargeData())
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

	f.Setup("install source", eventshub.Install(
		sourceName,
		eventshub.StartSenderToResource(broker.GVR(), brokerName),
		eventshub.InputEventWithEncoding(ev, cloudevents.EncodingBinary),
		eventshub.AddSequence,
		eventshub.SendMultipleEvents(1, 0),
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
		broker.WithConfig(single_partition_config.ConfigMapName),
	))
	f.Setup("broker is ready", broker.IsReady(brokerName))
	f.Setup("broker is addressable", broker.IsAddressable(brokerName))

	f.Setup("install sink", eventshub.Install(
		sinkName,
		eventshub.StartReceiver,
		eventshub.DropEventsResponseCode(422),
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
		eventshub.InputEventWithEncoding(ev, cloudevents.EncodingBinary),
		eventshub.AddSequence,
		eventshub.SendMultipleEvents(1, 0),
	))

	address, err := svc.Address(context.Background(), deadLetterSinkName)
	f.Assert("deadLetterSinkName is addressable", func(ctx context.Context, t feature.T) {
		if err != nil || address == nil {
			t.Fail()
		}
	})

	f.Assert("knativeerrordest & knativeerrorcode added", assertEnhancedWithKnativeErrorExtensions(
		deadLetterSinkName,
		func(ctx context.Context) cetest.EventMatcher {
			dlsAddress, err := svc.Address(ctx, deadLetterSinkName)
			if err != nil || dlsAddress == nil {
				fmt.Println("dead letter sink is not addressable", err)
			}
			fmt.Println("dead letter sink address: ", dlsAddress.Path)
			return cetest.HasExtension("knativeerrordest", dlsAddress.Path)
		},
		func(ctx context.Context) cetest.EventMatcher {
			return cetest.HasExtension("knativeerrorcode", "422")
		},
	))

	return f
}

//func SubscriberReturnedErrorSmallData() *feature.Feature {
//	f := feature.NewFeature()
//
//	sourceName := feature.MakeRandomK8sName("source")
//	sinkName := feature.MakeRandomK8sName("sink")
//	deadLetterSinkName := feature.MakeRandomK8sName("dls")
//	triggerName := feature.MakeRandomK8sName("trigger")
//	brokerName := feature.MakeRandomK8sName("broker")
//
//	ev := cetest.FullEvent()
//
//	f.Setup("install one partition configuration", single_partition_config.Install)
//	f.Setup("install broker", broker.Install(
//		brokerName,
//		broker.WithBrokerClass(kafka.BrokerClass),
//		broker.WithConfig(single_partition_config.ConfigMapName),
//	))
//	f.Setup("broker is ready", broker.IsReady(brokerName))
//	f.Setup("broker is addressable", broker.IsAddressable(brokerName))
//
//	f.Setup("install sink", eventshub.Install(
//		sinkName,
//		eventshub.StartReceiver,
//		eventshub.DropEventsResponseCode(422),
//		// todo add small data
//	))
//	f.Setup("install dead letter sink", eventshub.Install(
//		deadLetterSinkName,
//		eventshub.StartReceiver,
//	))
//	f.Setup("install trigger", trigger.Install(
//		triggerName,
//		brokerName,
//		trigger.WithSubscriber(svc.AsKReference(sinkName), ""),
//		trigger.WithDeadLetterSink(svc.AsKReference(deadLetterSinkName), ""),
//	))
//	f.Setup("trigger is ready", trigger.IsReady(triggerName))
//
//	f.Setup("install source", eventshub.Install(
//		sourceName,
//		eventshub.StartSenderToResource(broker.GVR(), brokerName),
//		eventshub.InputEventWithEncoding(ev, cloudevents.EncodingBinary),
//		eventshub.AddSequence,
//		eventshub.SendMultipleEvents(1, 0),
//	))
//
//	address, err := svc.Address(context.Background(), deadLetterSinkName)
//	f.Assert("deadLetterSinkName is addressable", func(ctx context.Context, t feature.T) {
//		if err != nil || address == nil {
//			t.Fail()
//		}
//	})
//
//	f.Assert("knativeerrordest & knativeerrorcode added", assertEnhancedWithKnativeErrorExtensions(
//		deadLetterSinkName,
//		func(ctx context.Context) cetest.EventMatcher {
//			dlsAddress, err := svc.Address(ctx, deadLetterSinkName)
//			if err != nil || dlsAddress == nil {
//				fmt.Println("dead letter sink is not addressable", err)
//			}
//			fmt.Println("dead letter sink address: ", dlsAddress.Path)
//			return cetest.HasExtension("knativeerrordest", dlsAddress.Path)
//		},
//		func(ctx context.Context) cetest.EventMatcher {
//			return cetest.HasExtension("knativeerrorcode", "422")
//		},
//		func(ctx context.Context) cetest.EventMatcher {
//			return cetest.HasExtension("knativeerrordata", "") // todo verify data
//		},
//	))
//
//	return f
//}

func assertEnhancedWithKnativeErrorExtensions(sinkName string, matcherfns ...func(ctx context.Context) cetest.EventMatcher) feature.StepFn {
	return func(ctx context.Context, t feature.T) {
		matchers := make([]cetest.EventMatcher, len(matcherfns))
		for i, fn := range matcherfns {
			matchers[i] = fn(ctx)
		}
		store := eventshub.StoreFromContext(ctx, sinkName)
		for _, ei := range store.Collected() {
			fmt.Println("actual event extensions:")
			for k, v := range ei.Event.Extensions() {
				fmt.Println(k, ": ", v)
			}
		}
		_ = store.AssertExact(
			t,
			1,
			MatchKind(EventReceived),
			MatchEvent(matchers...),
		)
	}
}
