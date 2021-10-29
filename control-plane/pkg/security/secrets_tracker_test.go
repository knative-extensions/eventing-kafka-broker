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
	"io"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	bindings "knative.dev/eventing-kafka/pkg/apis/bindings/v1beta1"
	sources "knative.dev/eventing-kafka/pkg/apis/sources/v1beta1"
	"knative.dev/pkg/tracker"
)

func TestTrackNetSpecSecrets(t *testing.T) {
	tests := []struct {
		name                        string
		secretsTracker              *mockTracker
		netSpec                     bindings.KafkaNetSpec
		expectedTrackReferenceCalls int
		parent                      metav1.Object
		wantErr                     bool
	}{
		{
			name:           "track all",
			secretsTracker: &mockTracker{},
			parent: &sources.KafkaSource{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "s",
					Namespace: "ns",
				},
			},
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
			expectedTrackReferenceCalls: 6,
			wantErr:                     false,
		},
		{
			name:           "track error",
			secretsTracker: &mockTracker{trackReferenceErr: io.EOF},
			parent: &sources.KafkaSource{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "s",
					Namespace: "ns",
				},
			},
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
			expectedTrackReferenceCalls: 1,
			wantErr:                     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := TrackNetSpecSecrets(tt.secretsTracker, tt.netSpec, tt.parent); (err != nil) != tt.wantErr {
				t.Errorf("TrackNetSpecSecrets() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.secretsTracker.trackReferenceCalls != tt.expectedTrackReferenceCalls {
				t.Errorf("Expected %d calls to TrackReference, got %d", tt.expectedTrackReferenceCalls, tt.secretsTracker.trackReferenceCalls)
			}
		})
	}
}

type mockTracker struct {
	trackReferenceCalls int
	trackReferenceErr   error
}

func (m mockTracker) Track(corev1.ObjectReference, interface{}) error {
	panic("implement me")
}

func (m *mockTracker) TrackReference(tracker.Reference, interface{}) error {
	m.trackReferenceCalls++
	return m.trackReferenceErr
}

func (m mockTracker) OnChanged(interface{}) {
	panic("implement me")
}

func (m mockTracker) GetObservers(interface{}) []types.NamespacedName {
	panic("implement me")
}

func (m mockTracker) OnDeletedObserver(interface{}) {
	panic("implement me")
}
