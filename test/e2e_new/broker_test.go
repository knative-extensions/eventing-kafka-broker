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

	"knative.dev/eventing/test/experimental/features/eventtype_autocreate"
	"knative.dev/pkg/system"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/eventshub"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/k8s"
	"knative.dev/reconciler-test/pkg/knative"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	"knative.dev/eventing-kafka-broker/test/e2e_new/single_partition_config"
	"knative.dev/eventing-kafka-broker/test/rekt/features"
	"knative.dev/eventing/test/rekt/features/authz"
	brokereventingfeatures "knative.dev/eventing/test/rekt/features/broker"
	"knative.dev/eventing/test/rekt/features/oidc"
	brokerresources "knative.dev/eventing/test/rekt/resources/broker"
)

const (
	PollInterval = 3 * time.Second
	PollTimeout  = 6 * time.Minute
)

func TestBrokerDeletedRecreated(t *testing.T) {
	// this test is observed to flake more when it is parallel
	// t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithObservabilityConfig,
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
		knative.WithObservabilityConfig,
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
		knative.WithObservabilityConfig,
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
		knative.WithObservabilityConfig,
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
		knative.WithObservabilityConfig,
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
		knative.WithObservabilityConfig,
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
		knative.WithObservabilityConfig,
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
		knative.WithObservabilityConfig,
		k8s.WithEventListener,
		environment.WithPollTimings(PollInterval, PollTimeout),
		environment.Managed(t),
	)

	env.Test(ctx, t, features.TriggerLatestOffset())
}

func TestBrokerWithBogusConfig(t *testing.T) {
	// this test is observed to flake more when it is parallel
	// t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithObservabilityConfig,
		k8s.WithEventListener,
		environment.WithPollTimings(PollInterval, PollTimeout),
		environment.Managed(t),
	)

	env.Test(ctx, t, features.BrokerWithBogusConfig())
}

func TestBrokerCannotReachKafkaCluster(t *testing.T) {
	// this test is observed to flake more when it is parallel
	// t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithObservabilityConfig,
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
		knative.WithObservabilityConfig,
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
		knative.WithObservabilityConfig,
		k8s.WithEventListener,
		environment.WithPollTimings(PollInterval, PollTimeout),
		environment.WithTestLogger(t),
		environment.InNamespace(namespace),
	)

	env.Test(ctx, t, features.SetupNamespace(namespace))
	env.Test(ctx, t, features.SetupNamespacedBroker(name))
	env.Test(ctx, t, features.CleanupNamespace(namespace))
}

func TestBrokerEventTypeAutoCreate(t *testing.T) {
	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithObservabilityConfig,
		k8s.WithEventListener,
		environment.Managed(t),
	)

	brokerName := feature.MakeRandomK8sName("broker")

	env.Prerequisite(ctx, t, InstallBroker(brokerName))
	env.Test(ctx, t, eventtype_autocreate.AutoCreateEventTypesOnBroker(brokerName))
}

func TestTriggerEventTypeAutoCreate(t *testing.T) {
	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithObservabilityConfig,
		k8s.WithEventListener,
		environment.Managed(t),
	)

	brokerName := feature.MakeRandomK8sName("broker")

	env.Prerequisite(ctx, t, InstallBroker(brokerName))
	env.Test(ctx, t, eventtype_autocreate.AutoCreateEventTypesOnTrigger(brokerName))
}

func InstallBroker(brokerName string) *feature.Feature {
	install, cmName := single_partition_config.MakeInstall()

	f := feature.NewFeature()

	f.Setup("install one partition configuration", install)
	f.Setup("install kafka broker", brokerresources.Install(
		brokerName,
		brokerresources.WithBrokerClass(kafka.BrokerClass),
		brokerresources.WithConfig(cmName),
	))
	f.Requirement("kafka broker is ready", brokerresources.IsReady(brokerName))
	f.Requirement("kafka broker is addressable", brokerresources.IsAddressable(brokerName))

	return f
}

func TestBrokerSupportsOIDC(t *testing.T) {
	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithObservabilityConfig,
		k8s.WithEventListener,
		environment.WithPollTimings(4*time.Second, 12*time.Minute),
		environment.Managed(t),
		eventshub.WithTLS(t),
	)

	name := feature.MakeRandomK8sName("broker")
	env.Prerequisite(ctx, t, brokereventingfeatures.GoesReady(name, brokerresources.WithEnvConfig()...))

	env.TestSet(ctx, t, oidc.AddressableOIDCConformance(brokerresources.GVR(), "Broker", name, env.Namespace()))
}

func TestBrokerSendsEventsWithOIDCSupport(t *testing.T) {
	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithObservabilityConfig,
		k8s.WithEventListener,
		environment.Managed(t),
		eventshub.WithTLS(t),
	)

	env.TestSet(ctx, t, brokereventingfeatures.BrokerSendEventWithOIDC())
}

func TestBrokerSupportsAuthZ(t *testing.T) {
	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithObservabilityConfig,
		k8s.WithEventListener,
		environment.Managed(t),
		eventshub.WithTLS(t),
	)

	name := feature.MakeRandomK8sName("broker")
	env.Prerequisite(ctx, t, brokereventingfeatures.GoesReady(name, brokerresources.WithEnvConfig()...))

	env.TestSet(ctx, t, authz.AddressableAuthZConformance(brokerresources.GVR(), "Broker", name))
}

func TestBrokerDispatcherKedaScaling(t *testing.T) {
	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithObservabilityConfig,
		k8s.WithEventListener,
		environment.WithPollTimings(5*time.Second, 4*time.Minute),
		environment.Managed(t),
	)

	env.Test(ctx, t, features.TriggerScalesToZeroWithKeda())
}

func TestBrokerDispatcherKedaScalingSASL(t *testing.T) {
	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithObservabilityConfig,
		k8s.WithEventListener,
		environment.WithPollTimings(5*time.Second, 4*time.Minute),
		environment.Managed(t),
	)

	env.Test(ctx, t, features.TriggerSASLScalesToZeroWithKeda())
}

func TestBrokerDispatcherKedaScalingSSL(t *testing.T) {
	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithObservabilityConfig,
		k8s.WithEventListener,
		environment.WithPollTimings(5*time.Second, 4*time.Minute),
		environment.Managed(t),
	)

	env.Test(ctx, t, features.TriggerSSLScalesToZeroWithKeda())
}
