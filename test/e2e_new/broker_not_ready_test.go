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

	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka"
	brokerconfigmap "knative.dev/eventing-kafka-broker/test/e2e_new/resources/configmap/broker"
	"knative.dev/eventing/test/rekt/resources/broker"
	"knative.dev/pkg/system"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/k8s"
	"knative.dev/reconciler-test/pkg/knative"
)

func TestBrokerNotReadyAfterBeingReady(t *testing.T) {

	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.Managed(t),
	)

	env.Test(ctx, t, BrokerNotReadyAfterBeingReady())
}

func BrokerNotReadyAfterBeingReady() *feature.Feature {
	f := feature.NewFeature()

	brokerName := feature.MakeRandomK8sName("broker")
	configName := feature.MakeRandomK8sName("config")

	f.Setup("create broker config", brokerconfigmap.Install(
		configName,
		brokerconfigmap.WithBootstrapServer("my-cluster-kafka-bootstrap.kafka:9092"),
		brokerconfigmap.WithNumPartitions(1),
		brokerconfigmap.WithReplicationFactor(1),
	))

	f.Setup("install broker", broker.Install(
		brokerName,
		broker.WithBrokerClass(kafka.BrokerClass),
		broker.WithConfig(configName),
	))

	f.Setup("broker is ready", broker.IsReady(brokerName))

	f.Requirement("update broker config", brokerconfigmap.Install(
		configName,
		// Invalid bootstrap server
		brokerconfigmap.WithBootstrapServer("my-cluster-kafka-bootstrap.non-kafka:9092"),
		brokerconfigmap.WithNumPartitions(1),
		brokerconfigmap.WithReplicationFactor(1),
	))

	f.Assert("broker is not ready", k8s.IsNotReady(broker.GVR(), brokerName))

	return f
}
