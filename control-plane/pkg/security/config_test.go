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

package security

import (
	"context"
	"io"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	corev1listers "k8s.io/client-go/listers/core/v1"
	kubeclient "knative.dev/pkg/client/injection/kube/client/fake"
	secretinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/secret"
	reconcilertesting "knative.dev/pkg/reconciler/testing"

	_ "knative.dev/pkg/client/injection/kube/informers/core/v1/secret/fake"
)

type SecretProviderFuncMock struct {
	secret *corev1.Secret
	err    error

	wantName      string
	wantNamespace string
	t             *testing.T
}

func (sp *SecretProviderFuncMock) F(ctx context.Context, namespace, name string) (*corev1.Secret, error) {
	assert.NotNil(sp.t, ctx)
	assert.Equal(sp.t, sp.wantNamespace, namespace)
	assert.Equal(sp.t, sp.wantName, name)
	return sp.secret, sp.err
}

func TestSecret(t *testing.T) {

	tests := []struct {
		name               string
		ctx                context.Context
		config             SecretLocator
		secretProviderFunc SecretProviderFunc
		wantSecret         *corev1.Secret
		wantErr            bool
	}{
		{
			name: "happy case - use configmap namespace",
			ctx:  context.Background(),
			config: &MTConfigMapSecretLocator{
				UseNamespaceInConfigmap: false,
				ConfigMap: &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "my-ns",
						Name:      "my-name",
					},
					Data: map[string]string{
						AuthSecretNameKey:      "my-name",
						AuthSecretNamespaceKey: "NOT_USED",
					},
				},
			},
			secretProviderFunc: (&SecretProviderFuncMock{
				secret: &corev1.Secret{
					Data: map[string][]byte{
						ProtocolKey: []byte(ProtocolPlaintext),
					},
				},
				err:           nil,
				wantName:      "my-name",
				wantNamespace: "my-ns",
				t:             t,
			}).F,
			wantSecret: &corev1.Secret{
				Data: map[string][]byte{
					ProtocolKey: []byte(ProtocolPlaintext),
				},
			},
			wantErr: false,
		},
		{
			name: "happy case - use namespace in configmap",
			ctx:  context.Background(),
			config: &MTConfigMapSecretLocator{
				UseNamespaceInConfigmap: true,
				ConfigMap: &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "my-ns",
						Name:      "my-name",
					},
					Data: map[string]string{
						AuthSecretNameKey: "my-name",
					},
				},
			},
			secretProviderFunc: (&SecretProviderFuncMock{
				secret: &corev1.Secret{
					Data: map[string][]byte{
						ProtocolKey: []byte(ProtocolPlaintext),
					},
				},
				err:           nil,
				wantName:      "my-name",
				wantNamespace: "my-ns",
				t:             t,
			}).F,
			wantSecret: &corev1.Secret{
				Data: map[string][]byte{
					ProtocolKey: []byte(ProtocolPlaintext),
				},
			},
			wantErr: false,
		},
		{
			name:       "no secret in MTConfigMapSecretLocator config",
			ctx:        context.Background(),
			config:     &MTConfigMapSecretLocator{ConfigMap: nil},
			wantSecret: nil,
		},
		{
			name: "no secret in MTConfigMapSecretLocator",
			ctx:  context.Background(),
			config: &MTConfigMapSecretLocator{
				UseNamespaceInConfigmap: false,
				ConfigMap: &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "my-ns",
						Name:      "my-name",
					},
					// No ref to secret here
					Data: map[string]string{},
				},
			},
			wantSecret: nil,
		},
		{
			name: "secret provider error",
			ctx:  context.Background(),
			config: &MTConfigMapSecretLocator{
				UseNamespaceInConfigmap: false,
				ConfigMap: &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "my-ns",
						Name:      "my-name",
					},
					Data: map[string]string{
						AuthSecretNameKey:      "my-name",
						AuthSecretNamespaceKey: "my-ns",
					},
				},
			},
			secretProviderFunc: (&SecretProviderFuncMock{
				secret: &corev1.Secret{
					Data: map[string][]byte{
						ProtocolKey: []byte(ProtocolPlaintext),
					},
				},
				err:           io.EOF,
				wantName:      "my-name",
				wantNamespace: "my-ns",
				t:             t,
			}).F,
			wantSecret: nil,
			wantErr:    true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			secret, err := Secret(tt.ctx, tt.config, tt.secretProviderFunc)
			if (err != nil) != tt.wantErr {
				t.Errorf("Secret() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if diff := cmp.Diff(tt.wantSecret, secret); diff != "" {
				t.Errorf("Secret() secret = %v, want %v diff: %s", secret, tt.wantSecret, diff)
			}
		})
	}
}

func TestDefaultSecretProviderFunc(t *testing.T) {

	ctx, _ := reconcilertesting.SetupFakeContext(t)

	tests := []struct {
		name   string
		lister corev1listers.SecretLister
		kc     kubernetes.Interface
		want   bool

		secretNamespace string
		secretName      string
		wantSecret      *corev1.Secret
		wantErr         bool
	}{
		{
			name:   "not found",
			lister: secretinformer.Get(ctx).Lister(),
			kc:     kubeclient.Get(ctx),
			want:   true,

			secretNamespace: "my-namespace",
			secretName:      "my-name",
			wantSecret:      nil,
			wantErr:         true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DefaultSecretProviderFunc(tt.lister, tt.kc)
			if (got != nil) != tt.want {
				t.Errorf("DefaultSecretProviderFunc() = %p, want %v", got, tt.want)
			}
			if got != nil {
				if s, err := got(ctx, tt.secretNamespace, tt.secretNamespace); (err != nil) != tt.wantErr {
					t.Errorf("got() got error %v want %v", err, tt.wantErr)
				} else if diff := cmp.Diff(tt.wantSecret, s); diff != "" {
					t.Errorf("diff %v", diff)
				}
			}
		})
	}
}
