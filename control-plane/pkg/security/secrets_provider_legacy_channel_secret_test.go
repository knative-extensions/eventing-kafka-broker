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

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/pkg/system"
)

func TestGetAuthConfigFromSecret(t *testing.T) {
	// Define The TestCase Struct
	type TestCase struct {
		name      string
		data      map[string][]byte
		expectNil bool
	}

	// Asserts that, if there is a key in the data, that its mapped value is the same as the given value
	assertKey := func(t *testing.T, data map[string][]byte, key string, value string) {
		mapValue, ok := data[key]
		if !ok {
			return
		}
		assert.Equal(t, string(mapValue), value)
	}

	// Create The TestCases
	testCases := []TestCase{
		{
			name: "Valid secret",
			data: map[string][]byte{
				SaslUsernameKey: []byte("OldAuthUsername"),
				SaslPasswordKey: []byte("OldAuthPassword"),
				SaslType:        []byte("OldAuthSaslType"),
			},
		},
		{
			name: "Valid secret, backwards-compatibility, TLS and SASL",
			data: map[string][]byte{
				CaCertificateKey: []byte("test-cacert"),
				UserCertificate:  []byte("test-usercert"),
				UserKey:          []byte("test-userkey"),
				SSLLegacyEnabled: []byte("true"),
				SaslUserKey:      []byte("test-sasluser"),
				SaslPasswordKey:  []byte("test-password"),
			},
		},
		{
			name: "Valid secret, backwards-compatibility, TLS/SASL without CA cert",
			data: map[string][]byte{
				SSLLegacyEnabled: []byte("true"),
				SaslUserKey:      []byte("test-sasluser"),
				SaslPasswordKey:  []byte("test-password"),
			},
		},
		{
			name: "Valid secret, backwards-compatibility, SASL, Invalid TLS",
			data: map[string][]byte{
				SSLLegacyEnabled: []byte("invalid-bool"),
				SaslUserKey:      []byte("test-sasluser"),
				SaslPasswordKey:  []byte("test-password"),
			},
		},
		{
			name:      "Valid secret, backwards-compatibility, nil data",
			data:      nil,
			expectNil: true,
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
			kafkaAuth := getAuthConfigFromSecret(secret)
			if testCase.expectNil {
				assert.Nil(t, kafkaAuth)
			} else {
				assert.NotNil(t, kafkaAuth)
				assert.NotNil(t, kafkaAuth.SASL)
				assertKey(t, testCase.data, SaslUsernameKey, kafkaAuth.SASL.User)
				assertKey(t, testCase.data, SaslType, kafkaAuth.SASL.SaslType)
				assertKey(t, testCase.data, SaslUserKey, kafkaAuth.SASL.User)
				assertKey(t, testCase.data, SaslPasswordKey, kafkaAuth.SASL.Password)
				if kafkaAuth.TLS != nil {
					assertKey(t, testCase.data, CaCertificateKey, kafkaAuth.TLS.Cacert)
					assertKey(t, testCase.data, UserCertificate, kafkaAuth.TLS.Usercert)
					assertKey(t, testCase.data, UserKey, kafkaAuth.TLS.Userkey)
				}
			}
		})
	}
}
