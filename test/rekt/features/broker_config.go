/*
 * Copyright 2023 The Knative Authors
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
	testpkg "knative.dev/eventing-kafka-broker/test/pkg"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/feature"

	apisconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/config"
	brokerconfigmap "knative.dev/eventing-kafka-broker/test/rekt/resources/configmap/broker"
	"knative.dev/eventing-kafka-broker/test/rekt/resources/kafkatopic"
	"knative.dev/eventing/test/rekt/resources/broker"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func BrokerWithCustomReplicationFactorAndNumPartitions(env environment.Environment) *feature.Feature {
	const (
		numPartitions     = 20
		replicationFactor = 1
	)

	f := feature.NewFeatureNamed("Broker KafkaTopic config from Broker configmap")

	brokerName := feature.MakeRandomK8sName("broker")
	brokerConfig := feature.MakeRandomK8sName("brokercfg")

	f.Setup("Create broker config", brokerconfigmap.Install(brokerConfig,
		brokerconfigmap.WithNumPartitions(numPartitions),
		brokerconfigmap.WithReplicationFactor(replicationFactor),
		brokerconfigmap.WithBootstrapServer(testpkg.BootstrapServersPlaintext)))

	f.Setup("Install broker", broker.Install(brokerName, append(
		broker.WithEnvConfig(),
		broker.WithConfig(brokerConfig))...,
	))
	f.Setup("Broker ready", broker.IsReady(brokerName))

	topic, err := apisconfig.DefaultFeaturesConfig().ExecuteBrokersTopicTemplate(metav1.ObjectMeta{Name: brokerName, Namespace: env.Namespace()})
	if err != nil {
		panic("failed to create broker topic name")
	}
	f.Requirement("Topic is ready", kafkatopic.IsReady(topic))

	f.Assert("Replication factor", kafkatopic.HasReplicationFactor(topic, replicationFactor))
	f.Assert("Number of partitions", kafkatopic.HasNumPartitions(topic, numPartitions))
	return f
}
