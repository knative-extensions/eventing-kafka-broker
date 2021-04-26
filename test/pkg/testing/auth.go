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

package testing

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	testlib "knative.dev/eventing/test/lib"
)

type SecretProvider func(t *testing.T, client *testlib.Client) map[string][]byte

func Plaintext(t *testing.T, _ *testlib.Client) map[string][]byte {
	return map[string][]byte{
		"protocol": []byte("PLAINTEXT"),
	}
}

func Ssl(t *testing.T, client *testlib.Client) map[string][]byte {
	caSecret, err := client.Kube.CoreV1().Secrets(KafkaClusterNamespace).Get(context.Background(), CaSecretName, metav1.GetOptions{})
	assert.Nil(t, err)

	tlsUserSecret, err := client.Kube.CoreV1().Secrets(KafkaClusterNamespace).Get(context.Background(), TlsUserSecretName, metav1.GetOptions{})
	assert.Nil(t, err)

	return map[string][]byte{
		"protocol": []byte("SSL"),
		"ca.crt":   caSecret.Data["ca.crt"],
		"user.crt": tlsUserSecret.Data["user.crt"],
		"user.key": tlsUserSecret.Data["user.key"],
	}
}

func SaslPlaintextScram512(t *testing.T, client *testlib.Client) map[string][]byte {

	saslUserSecret, err := client.Kube.CoreV1().Secrets(KafkaClusterNamespace).Get(context.Background(), SaslUserSecretName, metav1.GetOptions{})
	assert.Nil(t, err)

	return map[string][]byte{
		"protocol":       []byte("SASL_PLAINTEXT"),
		"sasl.mechanism": []byte("SCRAM-SHA-512"),
		"user":           []byte(SaslUserSecretName),
		"password":       saslUserSecret.Data["password"],
	}
}

func SslSaslScram512(t *testing.T, client *testlib.Client) map[string][]byte {
	caSecret, err := client.Kube.CoreV1().Secrets(KafkaClusterNamespace).Get(context.Background(), CaSecretName, metav1.GetOptions{})
	assert.Nil(t, err)

	saslUserSecret, err := client.Kube.CoreV1().Secrets(KafkaClusterNamespace).Get(context.Background(), SaslUserSecretName, metav1.GetOptions{})
	assert.Nil(t, err)

	return map[string][]byte{
		"protocol":       []byte("SASL_SSL"),
		"sasl.mechanism": []byte("SCRAM-SHA-512"),
		"ca.crt":         caSecret.Data["ca.crt"],
		"user":           []byte(SaslUserSecretName),
		"password":       saslUserSecret.Data["password"],
	}
}

type ConfigProvider func(secretName string, client *testlib.Client) map[string]string
