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

package features

import (
	"fmt"

	"knative.dev/eventing/test/rekt/resources/broker"
	"knative.dev/reconciler-test/pkg/feature"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	brokerconfigmap "knative.dev/eventing-kafka-broker/test/e2e_new/resources/configmap/broker"
	testpkg "knative.dev/eventing-kafka-broker/test/pkg"
)

func SetupNamespacedBroker(name string) *feature.Feature {
	f := feature.NewFeatureNamed("setup namespaced broker")

	f.Setup("Create broker config", brokerconfigmap.Install(
		"kafka-broker-config",
		brokerconfigmap.WithBootstrapServer(testpkg.BootstrapServersPlaintext),
		brokerconfigmap.WithNumPartitions(1),
		brokerconfigmap.WithReplicationFactor(1),
	))
	f.Setup(fmt.Sprintf("install broker %q", name), broker.Install(
		name,
		broker.WithBrokerClass(kafka.NamespacedBrokerClass),
		broker.WithConfig("kafka-broker-config"),
	))
	f.Setup("Broker is ready", broker.IsReady(name))
	f.Setup("Broker is addressable", broker.IsAddressable(name))

	return f
}
