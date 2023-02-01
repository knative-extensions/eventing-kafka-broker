//go:build e2e
// +build e2e

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

package e2e_new

import (
	"testing"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	testpkg "knative.dev/eventing-kafka-broker/test/pkg"
	"knative.dev/pkg/system"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/k8s"
	"knative.dev/reconciler-test/pkg/knative"

	brokerreconciler "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/broker"
	brokerconfigmap "knative.dev/eventing-kafka-broker/test/rekt/resources/configmap/broker"
	"knative.dev/eventing-kafka-broker/test/rekt/resources/kafkatopic"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1"
	"knative.dev/eventing/test/rekt/resources/broker"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestBrokerWithConfig tests a brokers values for ReplicationFactor and number
// of partitions if set in the broker configmap.
func TestBrokerWithConfig(t *testing.T) {

	const (
		numPartitions     = 20
		replicationFactor = 1
	)

	// Run Test In Parallel With Others
	t.Parallel()

	//Create The Test Context / Environment
	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
	)

	brokerName := feature.MakeRandomK8sName("broker")
	brokerConfig := feature.MakeRandomK8sName("brokercfg")

	f := feature.NewFeatureNamed("Broker KafkaTopic config from Broker configmap")

	f.Setup("Create broker config", brokerconfigmap.Install(brokerConfig,
		brokerconfigmap.WithNumPartitions(numPartitions),
		brokerconfigmap.WithReplicationFactor(replicationFactor),
		brokerconfigmap.WithBootstrapServer(testpkg.BootstrapServersPlaintext)))

	f.Setup("Install broker", broker.Install(brokerName, append(
		broker.WithEnvConfig(),
		broker.WithConfig(brokerConfig))...,
	))
	f.Setup("Broker ready", broker.IsReady(brokerName))

	topic := kafka.BrokerTopic(brokerreconciler.TopicPrefix, &eventing.Broker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      brokerName,
			Namespace: env.Namespace(),
		},
	})
	f.Setup("Topic is ready", kafkatopic.IsReady(topic))

	f.Assert("Replication factor", kafkatopic.HasReplicationFactor(topic, replicationFactor))
	f.Assert("Number of partitions", kafkatopic.HasNumPartitions(topic, numPartitions))

	env.Test(ctx, t, f)
}
