//go:build e2e
// +build e2e

/*
 * Copyright 2020 The Knative Authors
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

package e2e

import (
	"testing"

	"k8s.io/utils/pointer"

	eventingv1alpha1 "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/eventing/v1alpha1"
	. "knative.dev/eventing-kafka-broker/test/pkg/testing"
)

func TestKafkaSinkV1Alpha1DefaultContentMode(t *testing.T) {
	RunTestKafkaSink(t, eventingv1alpha1.ModeStructured, nil, func(kss *eventingv1alpha1.KafkaSinkSpec) error {
		kss.ContentMode = pointer.StringPtr("")
		return nil
	})
}

func TestKafkaSinkV1Alpha1StructuredContentMode(t *testing.T) {
	RunTestKafkaSink(t, eventingv1alpha1.ModeStructured, nil)
}

func TestKafkaSinkV1Alpha1BinaryContentMode(t *testing.T) {
	RunTestKafkaSink(t, eventingv1alpha1.ModeBinary, nil)
}

func TestKafkaSinkV1Alpha1AuthPlaintext(t *testing.T) {
	RunTestKafkaSink(t, eventingv1alpha1.ModeStructured, Plaintext, withBootstrap(BootstrapServersPlaintextArr), withSecret)
}

func TestKafkaSinkV1Alpha1AuthSsl(t *testing.T) {
	RunTestKafkaSink(t, eventingv1alpha1.ModeStructured, Ssl, withBootstrap(BootstrapServersSslArr), withSecret)
}

func TestKafkaSinkV1Alpha1AuthSaslPlaintextScram512(t *testing.T) {
	RunTestKafkaSink(t, eventingv1alpha1.ModeStructured, SaslPlaintextScram512, withBootstrap(BootstrapServersSaslPlaintextArr), withSecret)
}

func TestKafkaSinkV1Alpha1AuthSslSaslScram512(t *testing.T) {
	RunTestKafkaSink(t, eventingv1alpha1.ModeStructured, SslSaslScram512, withBootstrap(BootstrapServersSslSaslScramArr), withSecret)
}

func withSecret(kss *eventingv1alpha1.KafkaSinkSpec) error {
	kss.Auth = &eventingv1alpha1.Auth{
		Secret: &eventingv1alpha1.Secret{
			Ref: &eventingv1alpha1.SecretReference{
				Name: sinkSecretName,
			},
		},
	}
	return nil
}

func withBootstrap(bs []string) func(kss *eventingv1alpha1.KafkaSinkSpec) error {
	return func(kss *eventingv1alpha1.KafkaSinkSpec) error {
		kss.BootstrapServers = bs
		return nil
	}
}
