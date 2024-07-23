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
	"sort"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	bindings "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/bindings/v1beta1"
	secretinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/secret/fake"
	reconcilertesting "knative.dev/pkg/reconciler/testing"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
)

func TestResolveAuthContextFromNetSpec(t *testing.T) {
	tests := []struct {
		name                   string
		secrets                []*corev1.Secret
		namespace              string
		netSpec                bindings.KafkaNetSpec
		wantNetSpecAuthContext *NetSpecAuthContext
		wantErr                bool
	}{
		{
			name: "SASL_SSL - SCRAM-SHA-512",
			secrets: []*corev1.Secret{
				{
					ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "user"},
					StringData: map[string]string{"key": "key"},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "password"},
					StringData: map[string]string{"key": "key"},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "type"},
					StringData: map[string]string{"key": "SCRAM-SHA-512"},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "cert"},
					StringData: map[string]string{"key": "key"},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "key"},
					StringData: map[string]string{"key": "key"},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "cacert"},
					StringData: map[string]string{"key": "key"},
				},
			},
			namespace: "ns",
			netSpec: bindings.KafkaNetSpec{
				SASL: bindings.KafkaSASLSpec{
					Enable: true,
					User: bindings.SecretValueFromSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "user"},
							Key:                  "key",
						},
					},
					Password: bindings.SecretValueFromSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "password"},
							Key:                  "key",
						},
					},
					Type: bindings.SecretValueFromSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "type"},
							Key:                  "key",
						},
					},
				},
				TLS: bindings.KafkaTLSSpec{
					Enable: true,
					Cert: bindings.SecretValueFromSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "cert"},
							Key:                  "key",
						},
					},
					Key: bindings.SecretValueFromSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "key"},
							Key:                  "key",
						},
					},
					CACert: bindings.SecretValueFromSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "cacert"},
							Key:                  "key",
						},
					},
				},
			},
			wantNetSpecAuthContext: &NetSpecAuthContext{
				VirtualSecret: &corev1.Secret{StringData: map[string]string{
					ProtocolKey:      ProtocolSASLSSL,
					CaCertificateKey: "key",
					UserCertificate:  "key",
					UserKey:          "key",
					SaslMechanismKey: SaslScramSha512,
					SaslUserKey:      "key",
					SaslPasswordKey:  "key",
				}, ObjectMeta: metav1.ObjectMeta{
					Name:      "cacertcertkeypasswordtypeuser",
					Namespace: "nsnsnsnsnsns",
				},
				},
				MultiSecretReference: &contract.MultiSecretReference{
					Protocol: contract.Protocol_SASL_SSL,
					References: []*contract.SecretReference{
						{
							Reference: &contract.Reference{Namespace: "ns", Name: "cert"},
							KeyFieldReferences: []*contract.KeyFieldReference{
								{SecretKey: "key", Field: contract.SecretField_USER_CRT},
							},
						},
						{
							Reference: &contract.Reference{Namespace: "ns", Name: "key"},
							KeyFieldReferences: []*contract.KeyFieldReference{
								{SecretKey: "key", Field: contract.SecretField_USER_KEY},
							},
						},
						{
							Reference: &contract.Reference{Namespace: "ns", Name: "cacert"},
							KeyFieldReferences: []*contract.KeyFieldReference{
								{SecretKey: "key", Field: contract.SecretField_CA_CRT},
							},
						},
						{
							Reference: &contract.Reference{Namespace: "ns", Name: "type"},
							KeyFieldReferences: []*contract.KeyFieldReference{
								{SecretKey: "key", Field: contract.SecretField_SASL_MECHANISM},
							},
						},
						{
							Reference: &contract.Reference{Namespace: "ns", Name: "user"},
							KeyFieldReferences: []*contract.KeyFieldReference{
								{SecretKey: "key", Field: contract.SecretField_USER},
							},
						},
						{
							Reference: &contract.Reference{Namespace: "ns", Name: "password"},
							KeyFieldReferences: []*contract.KeyFieldReference{
								{SecretKey: "key", Field: contract.SecretField_PASSWORD},
							},
						},
					},
				},
			},
		},
		{
			name: "SASL_SSL - SCRAM-SHA-256",
			secrets: []*corev1.Secret{
				{
					ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "user"},
					StringData: map[string]string{"key": "key"},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "password"},
					StringData: map[string]string{"key": "key"},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "type"},
					StringData: map[string]string{"key": "SCRAM-SHA-256"},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "cert"},
					StringData: map[string]string{"key": "key"},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "key"},
					StringData: map[string]string{"key": "key"},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "cacert"},
					StringData: map[string]string{"key": "key"},
				},
			},
			namespace: "ns",
			netSpec: bindings.KafkaNetSpec{
				SASL: bindings.KafkaSASLSpec{
					Enable: true,
					User: bindings.SecretValueFromSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "user"},
							Key:                  "key",
						},
					},
					Password: bindings.SecretValueFromSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "password"},
							Key:                  "key",
						},
					},
					Type: bindings.SecretValueFromSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "type"},
							Key:                  "key",
						},
					},
				},
				TLS: bindings.KafkaTLSSpec{
					Enable: true,
					Cert: bindings.SecretValueFromSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "cert"},
							Key:                  "key",
						},
					},
					Key: bindings.SecretValueFromSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "key"},
							Key:                  "key",
						},
					},
					CACert: bindings.SecretValueFromSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "cacert"},
							Key:                  "key",
						},
					},
				},
			},
			wantNetSpecAuthContext: &NetSpecAuthContext{
				VirtualSecret: &corev1.Secret{StringData: map[string]string{
					ProtocolKey:      ProtocolSASLSSL,
					CaCertificateKey: "key",
					UserCertificate:  "key",
					UserKey:          "key",
					SaslMechanismKey: SaslScramSha256,
					SaslUserKey:      "key",
					SaslPasswordKey:  "key",
				}, ObjectMeta: metav1.ObjectMeta{
					Name:      "cacertcertkeypasswordtypeuser",
					Namespace: "nsnsnsnsnsns",
				},
				},
				MultiSecretReference: &contract.MultiSecretReference{
					Protocol: contract.Protocol_SASL_SSL,
					References: []*contract.SecretReference{
						{
							Reference: &contract.Reference{Namespace: "ns", Name: "cert"},
							KeyFieldReferences: []*contract.KeyFieldReference{
								{SecretKey: "key", Field: contract.SecretField_USER_CRT},
							},
						},
						{
							Reference: &contract.Reference{Namespace: "ns", Name: "key"},
							KeyFieldReferences: []*contract.KeyFieldReference{
								{SecretKey: "key", Field: contract.SecretField_USER_KEY},
							},
						},
						{
							Reference: &contract.Reference{Namespace: "ns", Name: "cacert"},
							KeyFieldReferences: []*contract.KeyFieldReference{
								{SecretKey: "key", Field: contract.SecretField_CA_CRT},
							},
						},
						{
							Reference: &contract.Reference{Namespace: "ns", Name: "type"},
							KeyFieldReferences: []*contract.KeyFieldReference{
								{SecretKey: "key", Field: contract.SecretField_SASL_MECHANISM},
							},
						},
						{
							Reference: &contract.Reference{Namespace: "ns", Name: "user"},
							KeyFieldReferences: []*contract.KeyFieldReference{
								{SecretKey: "key", Field: contract.SecretField_USER},
							},
						},
						{
							Reference: &contract.Reference{Namespace: "ns", Name: "password"},
							KeyFieldReferences: []*contract.KeyFieldReference{
								{SecretKey: "key", Field: contract.SecretField_PASSWORD},
							},
						},
					},
				},
			},
		},
		{
			name: "SSL",
			secrets: []*corev1.Secret{
				{
					ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "cert"},
					StringData: map[string]string{"key": "key"},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "key"},
					StringData: map[string]string{"key": "key"},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "cacert"},
					StringData: map[string]string{"key": "key"},
				},
			},
			namespace: "ns",
			netSpec: bindings.KafkaNetSpec{
				TLS: bindings.KafkaTLSSpec{
					Enable: true,
					Cert: bindings.SecretValueFromSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "cert"},
							Key:                  "key",
						},
					},
					Key: bindings.SecretValueFromSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "key"},
							Key:                  "key",
						},
					},
					CACert: bindings.SecretValueFromSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "cacert"},
							Key:                  "key",
						},
					},
				},
			},
			wantNetSpecAuthContext: &NetSpecAuthContext{
				VirtualSecret: &corev1.Secret{StringData: map[string]string{
					ProtocolKey:      ProtocolSSL,
					CaCertificateKey: "key",
					UserCertificate:  "key",
					UserKey:          "key",
				}, ObjectMeta: metav1.ObjectMeta{
					Name:      "cacertcertkey",
					Namespace: "nsnsns",
				}},
				MultiSecretReference: &contract.MultiSecretReference{
					Protocol: contract.Protocol_SSL,
					References: []*contract.SecretReference{
						{
							Reference: &contract.Reference{Namespace: "ns", Name: "cert"},
							KeyFieldReferences: []*contract.KeyFieldReference{
								{SecretKey: "key", Field: contract.SecretField_USER_CRT},
							},
						},
						{
							Reference: &contract.Reference{Namespace: "ns", Name: "key"},
							KeyFieldReferences: []*contract.KeyFieldReference{
								{SecretKey: "key", Field: contract.SecretField_USER_KEY},
							},
						},
						{
							Reference: &contract.Reference{Namespace: "ns", Name: "cacert"},
							KeyFieldReferences: []*contract.KeyFieldReference{
								{SecretKey: "key", Field: contract.SecretField_CA_CRT},
							},
						},
					},
				},
			},
		},
		{
			name: "SSL - no CA",
			secrets: []*corev1.Secret{
				{
					ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "cert"},
					StringData: map[string]string{"key": "key"},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "key"},
					StringData: map[string]string{"key": "key"},
				},
			},
			namespace: "ns",
			netSpec: bindings.KafkaNetSpec{
				TLS: bindings.KafkaTLSSpec{
					Enable: true,
					Cert: bindings.SecretValueFromSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "cert"},
							Key:                  "key",
						},
					},
					Key: bindings.SecretValueFromSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "key"},
							Key:                  "key",
						},
					},
				},
			},
			wantNetSpecAuthContext: &NetSpecAuthContext{
				VirtualSecret: &corev1.Secret{StringData: map[string]string{
					ProtocolKey:     ProtocolSSL,
					UserCertificate: "key",
					UserKey:         "key",
				}, ObjectMeta: metav1.ObjectMeta{
					Name:      "certkey",
					Namespace: "nsns",
				}},
				MultiSecretReference: &contract.MultiSecretReference{
					Protocol: contract.Protocol_SSL,
					References: []*contract.SecretReference{
						{
							Reference: &contract.Reference{Namespace: "ns", Name: "cert"},
							KeyFieldReferences: []*contract.KeyFieldReference{
								{SecretKey: "key", Field: contract.SecretField_USER_CRT},
							},
						},
						{
							Reference: &contract.Reference{Namespace: "ns", Name: "key"},
							KeyFieldReferences: []*contract.KeyFieldReference{
								{SecretKey: "key", Field: contract.SecretField_USER_KEY},
							},
						},
					},
				},
			},
		},
		{
			name: "SASL_PLAINTEXT - SCRAM-SHA-256",
			secrets: []*corev1.Secret{
				{
					ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "user"},
					StringData: map[string]string{"key": "key"},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "password"},
					StringData: map[string]string{"key": "key"},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "type"},
					StringData: map[string]string{"key": "SCRAM-SHA-256"},
				},
			},
			namespace: "ns",
			netSpec: bindings.KafkaNetSpec{
				SASL: bindings.KafkaSASLSpec{
					Enable: true,
					User: bindings.SecretValueFromSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "user"},
							Key:                  "key",
						},
					},
					Password: bindings.SecretValueFromSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "password"},
							Key:                  "key",
						},
					},
					Type: bindings.SecretValueFromSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "type"},
							Key:                  "key",
						},
					},
				},
			},
			wantNetSpecAuthContext: &NetSpecAuthContext{
				VirtualSecret: &corev1.Secret{StringData: map[string]string{
					ProtocolKey:      ProtocolSASLPlaintext,
					SaslMechanismKey: SaslScramSha256,
					SaslUserKey:      "key",
					SaslPasswordKey:  "key",
				}, ObjectMeta: metav1.ObjectMeta{
					Name:      "passwordtypeuser",
					Namespace: "nsnsns",
				}},
				MultiSecretReference: &contract.MultiSecretReference{
					Protocol: contract.Protocol_SASL_PLAINTEXT,
					References: []*contract.SecretReference{
						{
							Reference: &contract.Reference{Namespace: "ns", Name: "type"},
							KeyFieldReferences: []*contract.KeyFieldReference{
								{SecretKey: "key", Field: contract.SecretField_SASL_MECHANISM},
							},
						},
						{
							Reference: &contract.Reference{Namespace: "ns", Name: "user"},
							KeyFieldReferences: []*contract.KeyFieldReference{
								{SecretKey: "key", Field: contract.SecretField_USER},
							},
						},
						{
							Reference: &contract.Reference{Namespace: "ns", Name: "password"},
							KeyFieldReferences: []*contract.KeyFieldReference{
								{SecretKey: "key", Field: contract.SecretField_PASSWORD},
							},
						},
					},
				},
			},
		},
		{
			name: "SASL_PLAINTEXT - SCRAM-SHA-512",
			secrets: []*corev1.Secret{
				{
					ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "user"},
					StringData: map[string]string{"key": "key"},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "password"},
					StringData: map[string]string{"key": "key"},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "type"},
					StringData: map[string]string{"key": "SCRAM-SHA-512"},
				},
			},
			namespace: "ns",
			netSpec: bindings.KafkaNetSpec{
				SASL: bindings.KafkaSASLSpec{
					Enable: true,
					User: bindings.SecretValueFromSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "user"},
							Key:                  "key",
						},
					},
					Password: bindings.SecretValueFromSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "password"},
							Key:                  "key",
						},
					},
					Type: bindings.SecretValueFromSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "type"},
							Key:                  "key",
						},
					},
				},
			},
			wantNetSpecAuthContext: &NetSpecAuthContext{
				VirtualSecret: &corev1.Secret{StringData: map[string]string{
					ProtocolKey:      ProtocolSASLPlaintext,
					SaslMechanismKey: SaslScramSha512,
					SaslUserKey:      "key",
					SaslPasswordKey:  "key",
				}, ObjectMeta: metav1.ObjectMeta{
					Name:      "passwordtypeuser",
					Namespace: "nsnsns",
				}},
				MultiSecretReference: &contract.MultiSecretReference{
					Protocol: contract.Protocol_SASL_PLAINTEXT,
					References: []*contract.SecretReference{
						{
							Reference: &contract.Reference{Namespace: "ns", Name: "type"},
							KeyFieldReferences: []*contract.KeyFieldReference{
								{SecretKey: "key", Field: contract.SecretField_SASL_MECHANISM},
							},
						},
						{
							Reference: &contract.Reference{Namespace: "ns", Name: "user"},
							KeyFieldReferences: []*contract.KeyFieldReference{
								{SecretKey: "key", Field: contract.SecretField_USER},
							},
						},
						{
							Reference: &contract.Reference{Namespace: "ns", Name: "password"},
							KeyFieldReferences: []*contract.KeyFieldReference{
								{SecretKey: "key", Field: contract.SecretField_PASSWORD},
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, _ := reconcilertesting.SetupFakeContext(t)
			lister := secretinformer.Get(ctx)
			for _, s := range tt.secrets {
				cp := copySecretDataToData(s)
				_ = lister.Informer().GetStore().Add(cp)
			}

			got, err := ResolveAuthContextFromNetSpec(lister.Lister(), tt.namespace, tt.netSpec)
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tt.wantErr)
			}
			wantSecret := copySecretDataToData(tt.wantNetSpecAuthContext.VirtualSecret)
			wantSecret.StringData = nil
			tt.wantNetSpecAuthContext.VirtualSecret = wantSecret

			if diff := cmp.Diff(tt.wantNetSpecAuthContext.VirtualSecret, got.VirtualSecret); diff != "" {
				t.Error("(-want, +got)", diff)
			}
			wantMS := tt.wantNetSpecAuthContext.MultiSecretReference
			sort.Slice(wantMS.References, func(i, j int) bool {
				return strings.Compare(wantMS.References[i].Reference.Name, wantMS.References[j].Reference.Name) < 0
			})
			gotMS := got.MultiSecretReference
			sort.Slice(gotMS.References, func(i, j int) bool {
				return strings.Compare(gotMS.References[i].Reference.Name, gotMS.References[j].Reference.Name) < 0
			})
			if diff := cmp.Diff(wantMS, gotMS, protocmp.Transform()); diff != "" {
				t.Error("(-want, +got)", diff)
			}
		})
	}
}

func copySecretDataToData(s *corev1.Secret) *corev1.Secret {
	cp := s.DeepCopy()
	if cp.Data == nil {
		cp.Data = map[string][]byte{}
	}
	for k, v := range cp.StringData {
		cp.Data[k] = []byte(v)
	}
	return cp
}
