//go:build e2e
// +build e2e

/*
 * Copyright 2025 The Knative Authors
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
	"testing"

	"knative.dev/pkg/system"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/k8s"
	"knative.dev/reconciler-test/pkg/knative"

	"knative.dev/eventing-kafka-broker/test/rekt/features"
)

// TestTriggerCrossNamespaceBrokerRef tests that a Trigger in one namespace
// can reference a Kafka Broker in another namespace using brokerRef.
func TestTriggerCrossNamespaceBrokerRef(t *testing.T) {
	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithObservabilityConfig,
		k8s.WithEventListener,
		environment.WithPollTimings(PollInterval, PollTimeout),
		environment.Managed(t),
	)

	env.Test(ctx, t, features.TriggerCrossNamespaceBrokerRef())
}

// TestTriggerCrossNamespaceBrokerRefMultipleTriggers tests that multiple Triggers
// in different namespaces can reference the same Kafka Broker.
func TestTriggerCrossNamespaceBrokerRefMultipleTriggers(t *testing.T) {
	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithObservabilityConfig,
		k8s.WithEventListener,
		environment.WithPollTimings(PollInterval, PollTimeout),
		environment.Managed(t),
	)

	env.Test(ctx, t, features.TriggerCrossNamespaceBrokerRefMultipleTriggers())
}

// TestTriggerCrossNamespaceBrokerRefWithFilter tests that Triggers with filters
// in other namespaces correctly filter events from the cross-namespace Broker.
func TestTriggerCrossNamespaceBrokerRefWithFilter(t *testing.T) {
	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithObservabilityConfig,
		k8s.WithEventListener,
		environment.WithPollTimings(PollInterval, PollTimeout),
		environment.Managed(t),
	)

	env.Test(ctx, t, features.TriggerCrossNamespaceBrokerRefWithFilter())
}
