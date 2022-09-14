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

	"knative.dev/eventing-kafka-broker/test/e2e_new/features"
	"knative.dev/reconciler-test/pkg/feature"

	// Uncomment the following line to load the gcp plugin (only required to authenticate against GKE clusters).
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"

	"knative.dev/pkg/system"
	_ "knative.dev/pkg/system/testing"

	brokerfeatures "knative.dev/eventing/test/rekt/features/broker"
	"knative.dev/eventing/test/rekt/resources/broker"
	"knative.dev/reconciler-test/pkg/environment"
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

	env.Prerequisite(ctx, t, features.BrokerConfigmapCreated(configName))

	// Install and wait for a Ready Broker.
	env.Prerequisite(ctx, t,
		brokerfeatures.GoesReady(
			"default",
			broker.WithBrokerClass(broker.EnvCfg.BrokerClass),
			broker.WithConfig(configName),
		),
	)

	env.TestSet(ctx, t, brokerfeatures.ControlPlaneConformance("default", broker.WithConfig(configName)))
	env.TestSet(ctx, t, brokerfeatures.DataPlaneConformance("default"))
}
