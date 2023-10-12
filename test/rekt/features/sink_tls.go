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
	cetest "github.com/cloudevents/sdk-go/v2/test"
	"github.com/google/uuid"
	testpkg "knative.dev/eventing-kafka-broker/test/pkg"
	"knative.dev/eventing-kafka-broker/test/rekt/resources/kafkasink"
	"knative.dev/eventing-kafka-broker/test/rekt/resources/kafkatopic"
	"knative.dev/eventing/test/rekt/features/featureflags"
	"knative.dev/eventing/test/rekt/resources/addressable"
	"knative.dev/reconciler-test/pkg/eventshub"
	"knative.dev/reconciler-test/pkg/eventshub/assert"
	"knative.dev/reconciler-test/pkg/feature"
	"time"
)

func RotateSinkTLSCertificates(ctx context.Context) *feature.Feature {

	sink := feature.MakeRandomK8sName("sink")
	source := feature.MakeRandomK8sName("source")
	//ingressCertificateName := "kafka-sink-ingress-server-tls"

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

	//f.Setup("Rotate ingress certificate", certificate.Rotate(certificate.RotateCertificate{
	//	Certificate: types.NamespacedName{
	//		Namespace: system.Namespace(),
	//		Name:      ingressCertificateName,
	//	},
	//}))

	//f.Setup("install sink", eventshub.Install(sink, eventshub.StartReceiverTLS))
	//f.Setup("Install broker", broker.Install(brokerName, append(
	//	broker.WithEnvConfig())...,
	//))
	//f.Setup("Broker is ready", broker.IsReady(brokerName))
	//f.Setup("install trigger", func(ctx context.Context, t feature.T) {
	//	d := service.AsDestinationRef(sink)
	//	d.CACerts = eventshub.GetCaCerts(ctx)
	//	trigger.Install(triggerName, brokerName, trigger.WithSubscriberFromDestination(d))(ctx, t)
	//})
	//f.Setup("trigger is ready", trigger.IsReady(triggerName))
	f.Setup("Sink has HTTPS address", kafkasink.ValidateAddress(sink, addressable.AssertHTTPSAddress))
	//
	event := cetest.FullEvent()
	event.SetID(uuid.New().String())

	//-----BEGIN CERTIFICATE-----
	//MIIBcDCCARagAwIBAgIQP9lZt2NjVVNOI9yWIkGQ0zAKBggqhkjOPQQDAjAYMRYw
	//FAYDVQQDEw1zZWxmc2lnbmVkLWNhMB4XDTIzMTAxMjE4MjY1MloXDTI0MDExMDE4
	//MjY1MlowGDEWMBQGA1UEAxMNc2VsZnNpZ25lZC1jYTBZMBMGByqGSM49AgEGCCqG
	//SM49AwEHA0IABJotArvLvDk8YWiH00avqrYuxRN+8rXp6yiAifdgaG4mr+Tn+FdP
	//1y12GJT5zeZpNfPyH8NqpV+oovFgJF7yfaKjQjBAMA4GA1UdDwEB/wQEAwICpDAP
	//BgNVHRMBAf8EBTADAQH/MB0GA1UdDgQWBBRT06amxyrqWc000SEflcDscthirjAK
	//BggqhkjOPQQDAgNIADBFAiAHCDPxU4WnIvTxs7hwrV6gmfaTr0iJqkGvlxLMpl2S
	//CgIhAMSvZKOSZFvKGI31np1skHH6BSdkLQr+RM9pfg4azseD
	//-----END CERTIFICATE-----
	caCert := `-----BEGIN CERTIFICATE-----
MIIBcDCCARagAwIBAgIQP9lZt2NjVVNOI9yWIkGQ0zAKBggqhkjOPQQDAjAYMRYw
FAYDVQQDEw1zZWxmc2lnbmVkLWNhMB4XDTIzMTAxMjE4MjY1MloXDTI0MDExMDE4
MjY1MlowGDEWMBQGA1UEAxMNc2VsZnNpZ25lZC1jYTBZMBMGByqGSM49AgEGCCqG
SM49AwEHA0IABJotArvLvDk8YWiH00avqrYuxRN+8rXp6yiAifdgaG4mr+Tn+FdP
1y12GJT5zeZpNfPyH8NqpV+oovFgJF7yfaKjQjBAMA4GA1UdDwEB/wQEAwICpDAP
BgNVHRMBAf8EBTADAQH/MB0GA1UdDgQWBBRT06amxyrqWc000SEflcDscthirjAK
BggqhkjOPQQDAgNIADBFAiAHCDPxU4WnIvTxs7hwrV6gmfaTr0iJqkGvlxLMpl2S
CgIhAMSvZKOSZFvKGI31np1skHH6BSdkLQr+RM9pfg4azseD
-----END CERTIFICATE-----`

	f.Requirement("install source", eventshub.Install(source,
		eventshub.StartSenderToResourceTLS(kafkasink.GVR(), sink, &caCert),
		eventshub.InputEvent(event),
		// Send multiple events so that we take into account that the certificate rotation might
		// be detected by the server after some time.
		eventshub.AddSequence,
		eventshub.SendMultipleEvents(100, 3*time.Second),
	))
	//
	f.Assert("Event sent", assert.OnStore(source).
		MatchSentEvent(cetest.HasId(event.ID())).
		AtLeast(1),
	)
	f.Assert("Event received", assert.OnStore(sink).
		MatchReceivedEvent(cetest.HasId(event.ID())).
		AtLeast(1),
	)
	//f.Assert("Source match updated peer certificate", assert.OnStore(source).
	//	MatchPeerCertificatesReceived(assert.MatchPeerCertificatesFromSecret(system.Namespace(), ingressSecretName, "tls.crt")).
	//	AtLeast(1),
	//)

	return f
}
