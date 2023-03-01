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

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"knative.dev/eventing/test/rekt/resources/broker"
	"knative.dev/reconciler-test/pkg/feature"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	testpkg "knative.dev/eventing-kafka-broker/test/pkg"
	"knative.dev/eventing-kafka-broker/test/rekt/features/featuressteps"
	"knative.dev/eventing-kafka-broker/test/rekt/resources/configmap"
	brokerconfigmap "knative.dev/eventing-kafka-broker/test/rekt/resources/configmap/broker"
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

func NamespacedBrokerResourcesPropagation() *feature.Feature {
	f := feature.NewFeatureNamed("Namespaced Broker resource propagation")

	cmName := "config-namespaced-broker-resources"
	additionalCMName := "x-unknown-config"

	additionalResource := unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]interface{}{
				"name":      additionalCMName,
				"namespace": "{{.Namespace}}",
			},
			"data": map[string]string{
				"config":           "x-unknown-config",
				"dataFromTemplate": "{{.Namespace}}",
			},
		},
	}

	br := feature.MakeRandomK8sName("br-")

	f.Setup("Add additional resources to propagation ConfigMap",
		featuressteps.AddAdditionalResourcesToPropagationConfigMap(cmName, additionalResource))
	f.Setup("Create broker config", brokerconfigmap.Install(
		"kafka-broker-config",
		brokerconfigmap.WithBootstrapServer(testpkg.BootstrapServersPlaintext),
		brokerconfigmap.WithNumPartitions(1),
		brokerconfigmap.WithReplicationFactor(1),
	))

	f.Requirement(fmt.Sprintf("install broker %q", br), broker.Install(
		br,
		broker.WithBrokerClass(kafka.NamespacedBrokerClass),
		broker.WithConfig("kafka-broker-config"),
	))
	f.Requirement("Broker is ready", broker.IsReady(br))
	f.Requirement("Broker is addressable", broker.IsAddressable(br))

	f.Assert(fmt.Sprintf("%s ConfigMap is present in test namespace", additionalCMName),
		configmap.ExistsInTestNamespace(additionalCMName))

	return f
}
