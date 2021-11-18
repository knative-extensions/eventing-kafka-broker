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

package broker

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	fakekubeclientset "k8s.io/client-go/kubernetes/fake"
	_ "knative.dev/eventing/pkg/client/injection/informers/eventing/v1/broker/fake"
	_ "knative.dev/eventing/pkg/client/injection/informers/eventing/v1/trigger/fake"
	_ "knative.dev/pkg/client/injection/ducks/duck/v1/addressable/fake"
	fakekubeclient "knative.dev/pkg/client/injection/kube/client/fake"
	_ "knative.dev/pkg/client/injection/kube/informers/core/v1/configmap/fake"
	_ "knative.dev/pkg/client/injection/kube/informers/core/v1/pod/fake"
	_ "knative.dev/pkg/client/injection/kube/informers/core/v1/secret/fake"
	_ "knative.dev/pkg/client/injection/kube/informers/core/v1/service/fake"
	"knative.dev/pkg/configmap"
	dynamicclient "knative.dev/pkg/injection/clients/dynamicclient/fake"
	reconcilertesting "knative.dev/pkg/reconciler/testing"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
)

func TestNewController(t *testing.T) {
	ctx, _ := reconcilertesting.SetupFakeContext(t)

	env := &config.Env{
		SystemNamespace:      "cm",
		GeneralConfigMapName: "cm",
		IngressPodPort:       "8080",
	}

	ctx, _ = fakekubeclient.With(
		ctx,
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      env.GeneralConfigMapName,
				Namespace: env.SystemNamespace,
			},
		},
	)
	dynamicScheme := runtime.NewScheme()
	_ = fakekubeclientset.AddToScheme(dynamicScheme)

	dynamicclient.With(ctx, dynamicScheme)

	controller := NewController(
		ctx,
		configmap.NewStaticWatcher(&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name: "cm",
			},
		}),
		env,
		"",
	)
	if controller == nil {
		t.Error("failed to create controller: <nil>")
	}
}

func TestValidateDefaultBackoffDelayMs(t *testing.T) {

	tests := []struct {
		name    string
		env     config.Env
		wantErr bool
	}{
		{
			name: "0 want error",
			env: config.Env{
				DefaultBackoffDelayMs: 0,
			},
			wantErr: true,
		},
		{
			name: "non 0 no error",
			env: config.Env{
				DefaultBackoffDelayMs: 1,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidateDefaultBackoffDelayMs(tt.env); (err != nil) != tt.wantErr {
				t.Errorf("ValidateDefaultBackoffDelayMs() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
