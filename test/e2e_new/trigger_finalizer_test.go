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

	"github.com/stretchr/testify/assert"
	triggerreconciler "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/trigger"
	"knative.dev/eventing-kafka-broker/test/e2e_new/trigger"
	triggerfeatures "knative.dev/eventing/test/rekt/features/trigger"
	"knative.dev/pkg/system"
	"knative.dev/reconciler-test/pkg/eventshub"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/k8s"
	"knative.dev/reconciler-test/pkg/knative"
	"knative.dev/reconciler-test/resources/svc"
)

func TestTriggerNoFinalizerOnBrokerNotFound(t *testing.T) {

	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
	)

	t.Logf("Namespace is %s", env.Namespace())

	env.Test(ctx, t, triggerNoFinalizerOnBrokerNotFound())
}

func triggerNoFinalizerOnBrokerNotFound() *feature.Feature {

	f := feature.NewFeature()

	const responseWaitTime = 100 * time.Millisecond

	triggerName := feature.MakeRandomK8sName("trigger")
	sinkName := feature.MakeRandomK8sName("sink")

	f.Setup("install sink", eventshub.Install(
		sinkName,
		eventshub.StartReceiver,
		eventshub.ResponseWaitTime(responseWaitTime),
	))

	f.Setup("install trigger", trigger.Install(
		triggerName,
		"broker",
		trigger.WithSubscriber(svc.AsKReference(sinkName), ""),
	))

	f.Setup("set trigger name", triggerfeatures.SetTriggerName(triggerName))

	f.Assert("eventually trigger has no finalizer", func(ctx context.Context, t feature.T) {
		time.Sleep(time.Second * 20) // "eventually"
		for _, f := range triggerfeatures.GetTrigger(ctx, t).Finalizers {
			assert.NotEqual(t, f, triggerreconciler.FinalizerName)
		}
	})

	return f
}
