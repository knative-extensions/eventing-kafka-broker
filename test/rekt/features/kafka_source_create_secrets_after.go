/*
 * Copyright 2022 The Knative Authors
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
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/system"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/k8s"
	"knative.dev/reconciler-test/pkg/resources/service"

	"knative.dev/eventing-kafka-broker/test/rekt/resources/kafkasource"
	"knative.dev/eventing-kafka-broker/test/rekt/resources/kafkatopic"

	testingpkg "knative.dev/eventing-kafka-broker/test/pkg"
	"knative.dev/eventing-kafka-broker/test/rekt/features/featuressteps"
)

func CreateSecretsAfterKafkaSource() *feature.Feature {
	f := feature.NewFeatureNamed("Create secrets after KafkaSource")

	topicName := feature.MakeRandomK8sName("topic") // A k8s name is also a valid topic name.
	name := feature.MakeRandomK8sName("source")
	sink := feature.MakeRandomK8sName("sink")
	saslSecretName := feature.MakeRandomK8sName("sasl-secret")
	tlsSecretName := feature.MakeRandomK8sName("tls-secret")

	f.Setup("install kafka topic", kafkatopic.Install(topicName))
	f.Setup("install a service", service.Install(sink,
		service.WithSelectors(map[string]string{"app": "rekt"})))
	f.Setup("install a KafkaSource", kafkasource.Install(name,
		kafkasource.WithSink(&duckv1.KReference{Kind: "Service", Name: sink, APIVersion: "v1"}, ""),
		kafkasource.WithBootstrapServers(testingpkg.BootstrapServersSslSaslScramArr),
		kafkasource.WithTopics([]string{topicName}),
		kafkasource.WithSASLEnabled(),
		kafkasource.WithSASLUser(saslSecretName, "user"),
		kafkasource.WithSASLPassword(saslSecretName, "password"),
		kafkasource.WithSASLType(saslSecretName, "saslType"),
		kafkasource.WithTLSEnabled(),
		kafkasource.WithTLSCACert(tlsSecretName, "ca.crt"),
	))
	f.Setup("KafkaSource is not ready", k8s.IsNotReady(kafkasource.GVR(), name))

	f.Requirement("Create TLS secret", featuressteps.CopySecretInTestNamespace(system.Namespace(), TLSSecretName, tlsSecretName))
	f.Requirement("Create SASL secret", featuressteps.CopySecretInTestNamespace(system.Namespace(), SASLSecretName, saslSecretName))

	f.Assert("KafkaSource is ready", kafkasource.IsReady(name))

	return f
}
