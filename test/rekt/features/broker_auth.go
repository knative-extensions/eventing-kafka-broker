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
	"context"
	"time"

	"github.com/cloudevents/sdk-go/v2/test"

	"github.com/google/uuid"
	testpkg "knative.dev/eventing-kafka-broker/test/pkg"
	"knative.dev/eventing-kafka-broker/test/rekt/resources/kafkaauthsecret"

	"knative.dev/eventing/test/rekt/resources/broker"
	"knative.dev/eventing/test/rekt/resources/trigger"

	"knative.dev/reconciler-test/pkg/eventshub"
	"knative.dev/reconciler-test/pkg/eventshub/assert"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/manifest"

	"knative.dev/reconciler-test/resources/svc"

	brokerconfigmap "knative.dev/eventing-kafka-broker/test/rekt/resources/configmap/broker"
)

func SetupBrokerAuthPlaintext() *feature.Feature {
	return SetupBrokerAuth(testpkg.BootstrapServersPlaintext,
		kafkaauthsecret.WithPlaintextData())
}

func SetupBrokerAuthSsl(ctx context.Context) *feature.Feature {
	return SetupBrokerAuth(testpkg.BootstrapServersSsl,
		kafkaauthsecret.WithSslData(ctx))
}

func SetupBrokerNoAuthSsl(ctx context.Context) *feature.Feature {
	return SetupBrokerAuth(testpkg.BootstrapServersTlsNoAuth,
		kafkaauthsecret.WithTlsNoAuthData(ctx))
}

func SetupBrokerAuthSaslPlaintextScram512(ctx context.Context) *feature.Feature {
	return SetupBrokerAuth(testpkg.BootstrapServersSaslPlaintext,
		kafkaauthsecret.WithSaslPlaintextScram512Data(ctx))
}

func SetupBrokerAuthSslSaslScram512(ctx context.Context) *feature.Feature {
	return SetupBrokerAuth(testpkg.BootstrapServersSslSaslScram,
		kafkaauthsecret.WithSslSaslScram512Data(ctx))
}

func SetupBrokerAuthRestrictedSslSaslScram512(ctx context.Context) *feature.Feature {
	return SetupBrokerAuth(testpkg.BootstrapServersSslSaslScram,
		kafkaauthsecret.WithRestrictedSslSaslScram512Data(ctx))
}

func SetupBrokerAuth(bootstrapServer string, authSecretOptions ...manifest.CfgFn) *feature.Feature {
	f := feature.NewFeatureNamed("Broker with Kafka Auth Secret")

	brokerName := feature.MakeRandomK8sName("broker")
	brokerConfigName := feature.MakeRandomK8sName("brokercfg")
	authSecretName := feature.MakeRandomK8sName("kafkaauth")
	sinkName := feature.MakeRandomK8sName("sink")
	triggerName := feature.MakeRandomK8sName("trigger")
	senderName := feature.MakeRandomK8sName("sender")

	eventId := uuid.New().String()
	eventToSend := test.MinEvent()
	eventToSend.SetID(eventId)

	f.Setup("Create auth secret", kafkaauthsecret.Install(authSecretName, authSecretOptions...))

	f.Setup("Create broker config", brokerconfigmap.Install(brokerConfigName,
		brokerconfigmap.WithNumPartitions(3),
		brokerconfigmap.WithReplicationFactor(3),
		brokerconfigmap.WithBootstrapServer(bootstrapServer),
		brokerconfigmap.WithAuthSecret(authSecretName)))

	f.Setup("Install broker", broker.Install(brokerName, append(
		broker.WithEnvConfig(),
		broker.WithConfig(brokerConfigName))...,
	))
	f.Setup("Broker ready", broker.IsReady(brokerName))

	f.Setup("Install sink", eventshub.Install(sinkName,
		eventshub.StartReceiver))

	f.Setup("Create trigger", trigger.Install(triggerName, brokerName,
		trigger.WithSubscriber(svc.AsKReference(sinkName), "")))
	f.Setup("Trigger ready", trigger.IsReady(triggerName))

	f.Requirement("Send matching event", eventshub.Install(senderName,
		eventshub.InputEvent(eventToSend),
		eventshub.StartSenderToResource(broker.GVR(), brokerName)))

	f.Assert("Event received", assert.OnStore(sinkName).MatchEvent(test.HasId(eventId)).Exact(1))

	return f
}

func BrokerNotReadyWithoutAuthSecret() *feature.Feature {
	f := feature.NewFeatureNamed("Broker not ready without Kafka Auth Secret")

	brokerName := feature.MakeRandomK8sName("broker")
	brokerConfigName := feature.MakeRandomK8sName("brokercfg")
	authSecretName := feature.MakeRandomK8sName("kafkaauth")

	f.Setup("Create broker config", brokerconfigmap.Install(brokerConfigName,
		brokerconfigmap.WithNumPartitions(3),
		brokerconfigmap.WithReplicationFactor(1),
		brokerconfigmap.WithBootstrapServer(testpkg.BootstrapServersPlaintext),
		brokerconfigmap.WithAuthSecret(authSecretName)))

	f.Setup("Install broker", broker.Install(brokerName, append(
		broker.WithEnvConfig(),
		broker.WithConfig(brokerConfigName))...,
	))

	f.Setup("Broker is not ready without Kafka Auth Secret", func(ctx context.Context, t feature.T) {
		time.Sleep(10 * time.Second)
		broker.IsNotReady(brokerName)(ctx, t)
	})

	f.Requirement("Create auth secret", kafkaauthsecret.Install(authSecretName,
		kafkaauthsecret.WithPlaintextData()))
	f.Assert("Broker becomes ready with Kafka Auth Secret", broker.IsReady(brokerName))

	return f
}
