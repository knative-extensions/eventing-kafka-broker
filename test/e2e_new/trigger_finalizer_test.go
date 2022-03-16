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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"knative.dev/eventing/test/rekt/resources/broker"
	"knative.dev/reconciler-test/pkg/environment"

	triggerfeatures "knative.dev/eventing/test/rekt/features/trigger"
	"knative.dev/eventing/test/rekt/resources/trigger"
	"knative.dev/pkg/system"
	"knative.dev/reconciler-test/pkg/eventshub"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/k8s"
	"knative.dev/reconciler-test/pkg/knative"
	"knative.dev/reconciler-test/resources/svc"

	triggerreconciler "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/trigger"
)

func TestTriggerNoFinalizer(t *testing.T) {
	RunMultiple(t, func(t *testing.T) {
		ctx, env := global.Environment(
			knative.WithKnativeNamespace(system.Namespace()),
			knative.WithLoggingConfig,
			knative.WithTracingConfig,
			k8s.WithEventListener,
			environment.Managed(t),
		)

		t.Logf("Namespace is %s", env.Namespace())

		env.Test(ctx, t, triggerNoFinalizerOnBrokerNotFound())
		env.Test(ctx, t, unknownBrokerClass("Unknown"))
		env.Test(ctx, t, unknownBrokerClass("MTChannelBasedBroker"))
	})
}

func triggerNoFinalizerOnBrokerNotFound() *feature.Feature {

	f := feature.NewFeature()

	brokerName := feature.MakeRandomK8sName("broker")
	triggerName := feature.MakeRandomK8sName("trigger")
	sinkName := feature.MakeRandomK8sName("sink")

	f.Setup("install sink", eventshub.Install(sinkName, eventshub.StartReceiver))
	f.Setup("install trigger", trigger.Install(triggerName, brokerName,
		trigger.WithSubscriber(svc.AsKReference(sinkName), ""),
	))
	f.Setup("set trigger name", triggerfeatures.SetTriggerName(triggerName))

	f.Assert("eventually trigger has no finalizer", hasNoKafkaBrokerFinalizer())

	return f
}

func unknownBrokerClass(brokerClass string) *feature.Feature {
	f := feature.NewFeatureNamed("Unknown Broker Class - " + brokerClass)

	brokerName := feature.MakeRandomK8sName("broker")
	triggerName := feature.MakeRandomK8sName("trigger")
	sink := feature.MakeRandomK8sName("sink")

	f.Setup("Install Broker", broker.Install(brokerName, broker.WithBrokerClass(brokerClass)))
	f.Setup("Install events hub", eventshub.Install(sink, eventshub.StartReceiver))
	f.Setup("Install Trigger", trigger.Install(triggerName, brokerName,
		trigger.WithSubscriber(svc.AsKReference(sink), ""),
	))
	f.Setup("set trigger name", triggerfeatures.SetTriggerName(triggerName))

	f.Assert("eventually trigger has no finalizer", hasNoKafkaBrokerFinalizer())

	return f
}

func hasNoKafkaBrokerFinalizer() func(ctx context.Context, t feature.T) {
	return func(ctx context.Context, t feature.T) {
		time.Sleep(time.Second * 20) // "eventually"
		tr := triggerfeatures.GetTrigger(ctx, t)
		for _, f := range tr.Finalizers {
			require.NotEqual(t, f, triggerreconciler.FinalizerName, "%+v", tr)
		}
	}
}
