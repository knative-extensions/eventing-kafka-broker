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

	cetest "github.com/cloudevents/sdk-go/v2/test"
	"github.com/google/uuid"
	"k8s.io/apimachinery/pkg/types"
	testpkg "knative.dev/eventing-kafka-broker/test/pkg"
	"knative.dev/eventing-kafka-broker/test/rekt/resources/kafkasink"
	"knative.dev/eventing-kafka-broker/test/rekt/resources/kafkatopic"
	"knative.dev/eventing/test/rekt/features/featureflags"
	"knative.dev/eventing/test/rekt/resources/addressable"
	"knative.dev/pkg/system"
	"knative.dev/reconciler-test/pkg/eventshub"
	"knative.dev/reconciler-test/pkg/eventshub/assert"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/resources/certificate"
)

func RotateSinkTLSCertificates(ctx context.Context) *feature.Feature {

	sink := feature.MakeRandomK8sName("sink")
	source := feature.MakeRandomK8sName("source")
	ingressCertificateName := "kafka-sink-ingress-server-tls"

	f := feature.NewFeatureNamed("Rotate Kafka Sink TLS certificate")

	f.Prerequisite("transport encryption is strict", featureflags.TransportEncryptionStrict())
	f.Prerequisite("should not run when Istio is enabled", featureflags.IstioDisabled())

	topic := feature.MakeRandomK8sName("topic")

	f.Setup("install kafka topic", kafkatopic.Install(topic))
	f.Setup("topic is ready", kafkatopic.IsReady(topic))

	f.Setup("Install kafkasink", kafkasink.Install(sink, topic, testpkg.BootstrapServersPlaintextArr,
		kafkasink.WithNumPartitions(10),
		kafkasink.WithReplicationFactor(1)))
	f.Setup("KafkaSink is ready", kafkasink.IsReady(sink))

	f.Setup("Rotate ingress certificate", certificate.Rotate(certificate.RotateCertificate{
		Certificate: types.NamespacedName{
			Namespace: system.Namespace(),
			Name:      ingressCertificateName,
		},
	}))

	f.Setup("Sink has HTTPS address", kafkasink.ValidateAddress(sink, addressable.AssertHTTPSAddress))
	event := cetest.FullEvent()
	event.SetID(uuid.New().String())

	f.Requirement("install source", eventshub.Install(source,
		eventshub.StartSenderToResourceTLS(kafkasink.GVR(), sink, nil),
		eventshub.InputEvent(event),
		// Send multiple events so that we take into account that the certificate rotation might
		// be detected by the server after some time.
		eventshub.AddSequence,
		eventshub.SendMultipleEvents(100, 3*time.Second),
	))
	f.Assert("Event sent", assert.OnStore(source).
		MatchSentEvent(cetest.HasId(event.ID())).
		AtLeast(1),
	)

	f.Assert("Source match updated peer certificate", assert.OnStore(source).
		MatchPeerCertificatesReceived(assert.MatchPeerCertificatesFromSecret(system.Namespace(), ingressCertificateName, "tls.crt")).
		AtLeast(1),
	)

	return f
}
