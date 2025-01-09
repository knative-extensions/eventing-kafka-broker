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
	"context"
	"math"
	"strconv"

	cetest "github.com/cloudevents/sdk-go/v2/test"
	"github.com/google/uuid"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	kafkabroker "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/broker"
	"knative.dev/eventing-kafka-broker/test/e2e_new/bogus_config"
	"knative.dev/eventing-kafka-broker/test/rekt/resources/kafkatopic"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/eventshub/assert"

	"knative.dev/pkg/system"

	"knative.dev/eventing/test/rekt/resources/broker"
	"knative.dev/eventing/test/rekt/resources/trigger"
	"knative.dev/reconciler-test/pkg/eventshub"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/resources/service"

	testpkg "knative.dev/eventing-kafka-broker/test/pkg"
	"knative.dev/eventing-kafka-broker/test/rekt/features/featuressteps"
	"knative.dev/eventing-kafka-broker/test/rekt/resources/configmap"
	brokerconfigmap "knative.dev/eventing-kafka-broker/test/rekt/resources/configmap/broker"
)

func compose(steps ...feature.StepFn) feature.StepFn {
	return func(ctx context.Context, t feature.T) {
		for _, s := range steps {
			s(ctx, t)
		}
	}
}

// BrokerDeletedRecreated tests that when a broker and trigger is deleted and re-created, the original sink will eventually stop receiving events
func BrokerDeletedRecreated() *feature.Feature {
	f := feature.NewFeatureNamed("broker deleted and recreated")

	brokerName := feature.MakeRandomK8sName("broker")
	triggerName := feature.MakeRandomK8sName("trigger")

	sink1 := feature.MakeRandomK8sName("asink")
	sink2 := feature.MakeRandomK8sName("bsink")

	event := cetest.FullEvent()
	event.SetID(uuid.New().String())

	eventMatchers := []cetest.EventMatcher{
		cetest.HasId(event.ID()),
		cetest.HasSource(event.Source()),
		cetest.HasType(event.Type()),
		cetest.HasSubject(event.Subject()),
	}

	backoffPolicy := eventingduck.BackoffPolicyLinear

	f.Setup("test broker", compose(
		eventshub.Install(sink1, eventshub.StartReceiver),
		broker.Install(brokerName, broker.WithEnvConfig()...),
		broker.IsReady(brokerName),
		trigger.Install(
			triggerName,
			trigger.WithBrokerName(brokerName),
			trigger.WithRetry(3, &backoffPolicy, ptr.To("PT1S")),
			trigger.WithSubscriber(service.AsKReference(sink1), ""),
		),
		trigger.IsReady(triggerName),
		eventshub.Install(
			feature.MakeRandomK8sName("source"),
			eventshub.StartSenderToResource(broker.GVR(), brokerName),
			eventshub.AddSequence,
			eventshub.InputEvent(event),
			// We want to send 1 event/s until the timeout
			func(ctx context.Context, envs map[string]string) error {
				_, timeout := environment.PollTimingsFromContext(ctx)
				envs["PERIOD"] = "1" // in seconds
				envs["MAX_MESSAGES"] = strconv.Itoa(int(math.Ceil(timeout.Seconds())))
				return nil
			},
		),
		assert.OnStore(sink1).MatchEvent(eventMatchers...).AtLeast(1),
	))

	f.Requirement("delete broker", featuressteps.DeleteBroker(brokerName))
	f.Assert("test broker after deletion", compose(
		eventshub.Install(sink2, eventshub.StartReceiver),
		broker.Install(brokerName, broker.WithEnvConfig()...),
		broker.IsReady(brokerName),
		trigger.Install(
			triggerName,
			trigger.WithBrokerName(brokerName),
			trigger.WithRetry(3, &backoffPolicy, ptr.To("PT1S")),
			trigger.WithSubscriber(service.AsKReference(sink2), ""),
		),
		trigger.IsReady(triggerName),
		// We need to check both that
		// 1. sink1 eventually stops receiving new events
		// 2. sink2 eventually starts receiving all events
		// therefore, we check that eventually, the last few events sent (16 for no particular reason) are all received by the sink2 only
		// and contain an uninterrupted (without any missing sequence numbers) source sequence as sent by the source with eventshub.AddSequence
		EventSequenceOnStores(sink1, sink2).
			MatchingReceived(eventMatchers...).  // ... when ...
			OrderedBySourceSequence().           // ..., and taken the ...
			LastN(16).                           // ... events, the sequence...
			ContainsOnlyEventsObservedBy(sink2). // ...and...
			IsAnUninterruptedSourceSequence().
			Eventually(),
	))

	return f
}

func BrokerConfigMapDeletedFirst() *feature.Feature {
	f := feature.NewFeatureNamed("delete broker ConfigMap first")

	brokerName := feature.MakeRandomK8sName("broker")
	triggerName := feature.MakeRandomK8sName("trigger")
	cmName := feature.MakeRandomK8sName("cm-deleted-first")
	sink := feature.MakeRandomK8sName("sink")

	f.Setup("install sink", eventshub.Install(sink, eventshub.StartReceiver))
	f.Setup("install config", configmap.Copy(
		types.NamespacedName{Namespace: system.Namespace(), Name: "kafka-broker-config"},
		cmName,
	))
	f.Setup("install broker", broker.Install(brokerName, append(broker.WithEnvConfig(), broker.WithConfig(cmName))...))
	f.Setup("install trigger", trigger.Install(triggerName, trigger.WithBrokerName(brokerName),
		trigger.WithSubscriber(service.AsKReference(sink), "")),
	)

	f.Requirement("delete Broker ConfigMap", featuressteps.DeleteConfigMap(cmName))

	f.Assert("delete broker", featuressteps.DeleteBroker(brokerName))

	return f
}

// BrokerConfigMapDoesNotExist tests that a broker can be deleted without the ConfigMap existing.
func BrokerConfigMapDoesNotExist() *feature.Feature {
	f := feature.NewFeatureNamed("delete broker ConfigMap first")

	brokerName := feature.MakeRandomK8sName("broker")

	f.Setup("install broker", broker.Install(brokerName, append(broker.WithEnvConfig(), broker.WithConfig("doesNotExist"))...))

	f.Assert("delete broker", featuressteps.DeleteBroker(brokerName))

	return f
}

// BrokerAuthSecretDoesNotExist tests that a broker can be deleted without the Secret existing.
func BrokerAuthSecretDoesNotExist() *feature.Feature {
	f := feature.NewFeatureNamed("delete broker with non existing Secret")

	brokerName := feature.MakeRandomK8sName("broker")
	configName := feature.MakeRandomK8sName("config")
	topicName := feature.MakeRandomK8sName("topic") // A k8s name is also a valid topic name.

	f.Setup("install kafka topic", kafkatopic.Install(topicName))
	f.Setup("create broker config", brokerconfigmap.Install(
		configName,
		brokerconfigmap.WithBootstrapServer(testpkg.BootstrapServersPlaintext),
		brokerconfigmap.WithNumPartitions(1),
		brokerconfigmap.WithReplicationFactor(1),
		brokerconfigmap.WithAuthSecret("does-not-exist"),
	))

	f.Setup("install broker", broker.Install(
		brokerName,
		append(
			broker.WithEnvConfig(),
			broker.WithConfig(configName),
			broker.WithAnnotations(
				map[string]interface{}{
					kafkabroker.ExternalTopicAnnotation: topicName,
				}))...))

	f.Assert("delete broker", featuressteps.DeleteBroker(brokerName))

	return f
}

// BrokerExternalTopicDoesNotExist tests that a broker can be deleted without the Topic existing.
func BrokerExternalTopicDoesNotExist() *feature.Feature {
	f := feature.NewFeatureNamed("delete broker with non existing Topic")

	brokerName := feature.MakeRandomK8sName("broker")
	topicName := feature.MakeRandomK8sName("topic-does-not-exist") // A k8s name is also a valid topic name.

	f.Setup("install broker", broker.Install(
		brokerName,
		append(
			broker.WithEnvConfig(),
			broker.WithAnnotations(
				map[string]interface{}{
					kafkabroker.ExternalTopicAnnotation: topicName,
				}))...))

	f.Assert("delete broker", featuressteps.DeleteBroker(brokerName))

	return f
}

// BrokerExternalTopicAuthSecretDoesNotExist tests that a broker can be deleted without the Secret and Topic existing.
func BrokerExternalTopicAuthSecretDoesNotExist() *feature.Feature {
	f := feature.NewFeatureNamed("delete broker with non existing Secret or Topic")

	brokerName := feature.MakeRandomK8sName("broker")
	configName := feature.MakeRandomK8sName("config")
	topicName := feature.MakeRandomK8sName("topic-does-not-exist") // A k8s name is also a valid topic name.

	f.Setup("create broker config", brokerconfigmap.Install(
		configName,
		brokerconfigmap.WithBootstrapServer(testpkg.BootstrapServersPlaintext),
		brokerconfigmap.WithAuthSecret("does-not-exist"),
	))

	f.Setup("install broker", broker.Install(
		brokerName,
		append(
			broker.WithEnvConfig(),
			broker.WithConfig(configName),
			broker.WithAnnotations(
				map[string]interface{}{
					kafkabroker.ExternalTopicAnnotation: topicName,
				}))...))

	f.Assert("delete broker", featuressteps.DeleteBroker(brokerName))

	return f
}

// BrokerAuthSecretForInternalTopicDoesNotExist tests that a broker can be deleted without the Secret.
func BrokerAuthSecretForInternalTopicDoesNotExist() *feature.Feature {
	f := feature.NewFeatureNamed("delete broker with non existing Secret or Topic")

	brokerName := feature.MakeRandomK8sName("broker")
	configName := feature.MakeRandomK8sName("config")

	f.Setup("create broker config", brokerconfigmap.Install(
		configName,
		brokerconfigmap.WithBootstrapServer(testpkg.BootstrapServersSsl),
		brokerconfigmap.WithAuthSecret("does-not-exist"),
		brokerconfigmap.WithNumPartitions(1),
		brokerconfigmap.WithReplicationFactor(1),
	))

	f.Setup("install broker", broker.Install(
		brokerName,
		append(
			broker.WithEnvConfig(),
			broker.WithConfig(configName))...))

	f.Assert("delete broker", featuressteps.DeleteBroker(brokerName))

	return f
}

// BrokerWithBogusConfig tests that a broker can be deleted even when it has a bogus config attached.
func BrokerWithBogusConfig() *feature.Feature {
	f := feature.NewFeatureNamed("delete broker with bogus config")

	brokerName := feature.MakeRandomK8sName("broker")
	secretName := feature.MakeRandomK8sName("sasl-secret")

	f.Setup("install bogus configuration", bogus_config.Install)

	f.Requirement("Create SASL secret", featuressteps.CopySecretInTestNamespace(system.Namespace(), SASLSecretName, secretName))

	f.Setup("install broker", broker.Install(
		brokerName,
		broker.WithConfig(bogus_config.ConfigMapName),
	))
	f.Assert("delete broker", featuressteps.DeleteBroker(brokerName))

	return f
}

// BrokerCannotReachKafkaCluster tests that a broker can be deleted even when KafkaCluster is unreachable.
func BrokerCannotReachKafkaCluster() *feature.Feature {
	f := feature.NewFeatureNamed("delete broker with unreachable Kafka cluster")

	brokerName := feature.MakeRandomK8sName("broker")
	configName := feature.MakeRandomK8sName("config")

	f.Setup("create broker config", brokerconfigmap.Install(
		configName,
		brokerconfigmap.WithBootstrapServer("cluster-does-not-exist:9092"),
		brokerconfigmap.WithNumPartitions(1),
		brokerconfigmap.WithReplicationFactor(1),
	))

	f.Setup("install broker", broker.Install(brokerName, append(broker.WithEnvConfig(), broker.WithConfig(configName))...))

	f.Assert("delete broker", featuressteps.DeleteBroker(brokerName))

	return f
}
