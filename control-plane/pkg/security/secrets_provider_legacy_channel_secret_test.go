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

package security

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
	"knative.dev/pkg/system"
)

func Test_getProtocolFromLegacyChannelSecret(t *testing.T) {
	// Define The TestCase Struct
	type TestCase struct {
		name                     string
		data                     map[string][]byte
		expectedProtocolStr      string
		expectedProtocolContract contract.Protocol
	}

	// Create The TestCases
	testCases := []TestCase{
		{
			name:                     "Plaintext",
			data:                     map[string][]byte{},
			expectedProtocolStr:      ProtocolPlaintext,
			expectedProtocolContract: contract.Protocol_PLAINTEXT,
		},
		{
			name: "SSL/TLS via CA cert",
			data: map[string][]byte{
				CaCertificateKey: []byte("test-cacert"),
			},
			expectedProtocolStr:      ProtocolSSL,
			expectedProtocolContract: contract.Protocol_SSL,
		},
		{
			name: "SSL/TLS via tls.enabled",
			data: map[string][]byte{
				SSLLegacyEnabled: []byte("true"),
			},
			expectedProtocolStr:      ProtocolSSL,
			expectedProtocolContract: contract.Protocol_SSL,
		},
		{
			name: "SASL",
			data: map[string][]byte{
				SaslUsernameKey: []byte("test-sasluser"),
				SaslPasswordKey: []byte("test-password"),
			},
			expectedProtocolStr:      ProtocolSASLPlaintext,
			expectedProtocolContract: contract.Protocol_SASL_PLAINTEXT,
		},
		{
			name: "SASL via legacy channel secret",
			data: map[string][]byte{
				SaslUserKey:     []byte("test-sasluser"),
				SaslPasswordKey: []byte("test-password"),
			},
			expectedProtocolStr:      ProtocolSASLPlaintext,
			expectedProtocolContract: contract.Protocol_SASL_PLAINTEXT,
		},

		{
			name: "SASL and TLS/SSL via ca cert",
			data: map[string][]byte{
				CaCertificateKey: []byte("test-cacert"),

				SaslUsernameKey: []byte("test-sasluser"),
				SaslPasswordKey: []byte("test-password"),
			},
			expectedProtocolStr:      ProtocolSASLSSL,
			expectedProtocolContract: contract.Protocol_SASL_SSL,
		},
		{
			name: "SASL and TLS/SSL via tls.enabled",
			data: map[string][]byte{
				SSLLegacyEnabled: []byte("true"),

				SaslUsernameKey: []byte("test-sasluser"),
				SaslPasswordKey: []byte("test-password"),
			},
			expectedProtocolStr:      ProtocolSASLSSL,
			expectedProtocolContract: contract.Protocol_SASL_SSL,
		},
		{
			name: "SASL and TLS/SSL",
			data: map[string][]byte{
				CaCertificateKey: []byte("test-cacert"),
				UserCertificate:  []byte("test-usercert"),
				UserKey:          []byte("test-userkey"),
				SSLLegacyEnabled: []byte("true"),
				SaslUserKey:      []byte("test-sasluser"),
				SaslPasswordKey:  []byte("test-password"),
			},
			expectedProtocolStr:      ProtocolSASLSSL,
			expectedProtocolContract: contract.Protocol_SASL_SSL,
		},
		{
			name: "SASL and TLS/SSL (with invalid TLS)",
			data: map[string][]byte{
				SSLLegacyEnabled: []byte("invalid-bool"),
				SaslUserKey:      []byte("test-sasluser"),
				SaslPasswordKey:  []byte("test-password"),
			},
			expectedProtocolStr:      ProtocolSASLPlaintext,
			expectedProtocolContract: contract.Protocol_SASL_PLAINTEXT,
		},
	}

	// Run The TestCases
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			secret := &corev1.Secret{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Secret",
					APIVersion: corev1.SchemeGroupVersion.String(),
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "my-secret",
					Namespace: system.Namespace(),
				},
				Data: testCase.data,
			}

			gotProtocolStr, gotRrotocolContract := getProtocolFromLegacyChannelSecret(secret)
			if gotProtocolStr != testCase.expectedProtocolStr {
				t.Errorf("getProtocolFromLegacyChannelSecret() gotProtocolStr = %v, want %v", gotProtocolStr, testCase.expectedProtocolStr)
			}
			if gotRrotocolContract != testCase.expectedProtocolContract {
				t.Errorf("getProtocolFromLegacyChannelSecret() gotRrotocolContract = %v, want %v", gotRrotocolContract, testCase.expectedProtocolContract)
			}
		})
	}
}
