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
	"time"

	"knative.dev/pkg/system"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/k8s"
	"knative.dev/reconciler-test/pkg/knative"

	"knative.dev/eventing-kafka-broker/test/rekt/features"
)

const (
	PollInterval = 3 * time.Second
	PollTimeout  = 4 * time.Minute
)

func TestBrokerDeletedRecreated(t *testing.T) {
	// this test is observed to flake more when it is parallel
	// t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.WithPollTimings(PollInterval, PollTimeout),
		environment.Managed(t),
	)

	env.Test(ctx, t, features.BrokerDeletedRecreated())
}

func TestBrokerConfigMapDeletedFirst(t *testing.T) {
	// this test is observed to flake more when it is parallel
	// t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.WithPollTimings(PollInterval, PollTimeout),
		environment.Managed(t),
	)

	env.Test(ctx, t, features.BrokerConfigMapDeletedFirst())
}

func TestBrokerConfigMapDoesNotExist(t *testing.T) {
	// this test is observed to flake more when it is parallel
	// t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.WithPollTimings(PollInterval, PollTimeout),
		environment.Managed(t),
	)

	env.Test(ctx, t, features.BrokerConfigMapDoesNotExist())
}

func TestBrokerAuthSecretDoesNotExist(t *testing.T) {
	// this test is observed to flake more when it is parallel
	// t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.WithPollTimings(PollInterval, PollTimeout),
		environment.Managed(t),
	)

	env.Test(ctx, t, features.BrokerAuthSecretDoesNotExist())
}

func TestBrokerExternalTopicDoesNotExist(t *testing.T) {
	// this test is observed to flake more when it is parallel
	// t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.WithPollTimings(PollInterval, PollTimeout),
		environment.Managed(t),
	)

	env.Test(ctx, t, features.BrokerExternalTopicDoesNotExist())
}

func TestBrokerExternalTopicAuthSecretDoesNotExist(t *testing.T) {
	// this test is observed to flake more when it is parallel
	// t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.WithPollTimings(PollInterval, PollTimeout),
		environment.Managed(t),
	)

	env.Test(ctx, t, features.BrokerExternalTopicAuthSecretDoesNotExist())
}

func TestBrokerAuthSecretForInternalTopicDoesNotExist(t *testing.T) {
	// this test is observed to flake more when it is parallel
	// t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.WithPollTimings(PollInterval, PollTimeout),
		environment.Managed(t),
	)

	env.Test(ctx, t, features.BrokerAuthSecretForInternalTopicDoesNotExist())
}

func TestTriggerLatestOffset(t *testing.T) {
	// this test is observed to flake more when it is parallel
	// t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.WithPollTimings(PollInterval, PollTimeout),
		environment.Managed(t),
	)

	env.Test(ctx, t, features.TriggerLatestOffset())
}

func TestBrokerCannotReachKafkaCluster(t *testing.T) {
	// this test is observed to flake more when it is parallel
	// t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.WithPollTimings(PollInterval, PollTimeout),
		environment.Managed(t),
	)

	env.Test(ctx, t, features.BrokerCannotReachKafkaCluster())
}

func TestNamespacedBrokerResourcesPropagation(t *testing.T) {
	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.WithPollTimings(PollInterval, PollTimeout),
		environment.Managed(t),
	)

	env.Test(ctx, t, features.NamespacedBrokerResourcesPropagation())
}

func TestNamespacedBrokerNamespaceDeletion(t *testing.T) {

	name := "broker"
	namespace := feature.MakeRandomK8sName("test-namespaced-broker")

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.WithPollTimings(PollInterval, PollTimeout),
		environment.WithTestLogger(t),
		environment.InNamespace(namespace),
	)

	env.Test(ctx, t, features.SetupNamespace(namespace))
	env.Test(ctx, t, features.SetupNamespacedBroker(name))
	env.Test(ctx, t, features.CleanupNamespace(namespace))
}
