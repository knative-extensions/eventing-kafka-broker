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

package experimental

import (
	"testing"

	"knative.dev/pkg/system"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/k8s"
	"knative.dev/reconciler-test/pkg/knative"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka"
	"knative.dev/eventing-kafka-broker/test/e2e_new/single_partition_config"
	"knative.dev/eventing-kafka-broker/test/experimental/features_config"

	newfilters "knative.dev/eventing/test/experimental/features/new_trigger_filters"
	"knative.dev/eventing/test/rekt/resources/broker"
)

func TestMTChannelBrokerNewTriggerFilters(t *testing.T) {
	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.Managed(t),
	)
	brokerName := feature.MakeRandomK8sName("broker")

	env.Prerequisite(ctx, t, InstallKafkaBrokerWithExperimentalFeatures(brokerName))
	env.TestSet(ctx, t, newfilters.FiltersFeatureSet(brokerName))
}

func InstallKafkaBrokerWithExperimentalFeatures(name string) *feature.Feature {
	f := feature.NewFeatureNamed("Kafka broker")
	f.Setup("enable new trigger filters experimental feature", features_config.Install)
	f.Setup("install one partition configuration", single_partition_config.Install)
	f.Setup("install broker", broker.Install(
		name,
		broker.WithBrokerClass(kafka.BrokerClass),
		broker.WithConfig(single_partition_config.ConfigMapName),
	))
	f.Setup("broker is ready", broker.IsReady(name))
	f.Setup("broker is addressable", broker.IsAddressable(name))

	return f
}
