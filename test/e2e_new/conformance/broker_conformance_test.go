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

package conformance

import (
	"testing"

	// Uncomment the following line to load the gcp plugin (only required to authenticate against GKE clusters).
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"knative.dev/reconciler-test/pkg/manifest"

	"knative.dev/pkg/system"
	_ "knative.dev/pkg/system/testing"

	testpkg "knative.dev/eventing-kafka-broker/test/pkg"
	brokerconfigmap "knative.dev/eventing-kafka-broker/test/rekt/resources/configmap/broker"
	"knative.dev/eventing/pkg/apis/eventing"
	"knative.dev/eventing/test/rekt/features/broker"
	b "knative.dev/eventing/test/rekt/resources/broker"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/k8s"
	"knative.dev/reconciler-test/pkg/knative"
)

func TestBrokerConformance(t *testing.T) {
	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.Managed(t),
	)

	configName := feature.MakeRandomK8sName("kafka-broker-config")

	env.Prerequisite(ctx, t, BrokerCreateConfigMap(configName))

	opts := append(b.WithEnvConfig(), b.WithConfig(configName))

	brokerName := feature.MakeRandomK8sName("broker")

	env.Prerequisite(ctx, t, broker.GoesReady(brokerName, opts...))

	env.TestSet(ctx, t, broker.ControlPlaneConformance(brokerName, opts...))
	env.TestSet(ctx, t, broker.DataPlaneConformance(brokerName))
}

func BrokerCreateConfigMap(configName string) *feature.Feature {
	f := feature.NewFeature()

	opts := []manifest.CfgFn{
		brokerconfigmap.WithBootstrapServer(testpkg.BootstrapServersPlaintext),
		brokerconfigmap.WithNumPartitions(2),
		brokerconfigmap.WithReplicationFactor(3),
	}
	if b.EnvCfg.BrokerClass == eventing.MTChannelBrokerClassValue {
		opts = []manifest.CfgFn{brokerconfigmap.WithKafkaChannelMTBroker()}
	}

	f.Setup("create broker config", brokerconfigmap.Install(configName, opts...))

	return f
}
