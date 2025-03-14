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

package source

import (
	"testing"

	"knative.dev/eventing/pkg/apis/feature"

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
	"knative.dev/pkg/configmap"
	dynamicclient "knative.dev/pkg/injection/clients/dynamicclient/fake"
	reconcilertesting "knative.dev/pkg/reconciler/testing"

	_ "knative.dev/eventing-kafka-broker/control-plane/pkg/client/injection/informers/sources/v1/kafkasource/fake"

	_ "knative.dev/eventing-kafka-broker/control-plane/pkg/client/injection/informers/internalskafkaeventing/v1alpha1/consumergroup/fake"

	_ "knative.dev/pkg/client/injection/kube/informers/core/v1/serviceaccount/fake"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	kedaclient "knative.dev/eventing-kafka-broker/third_party/pkg/client/injection/client/fake"
)

func TestNewController(t *testing.T) {
	ctx, _ := reconcilertesting.SetupFakeContext(t)
	ctx, _ = kedaclient.With(ctx)

	configs := &config.Env{
		SystemNamespace:      "cm",
		GeneralConfigMapName: "cm",
	}

	ctx, _ = fakekubeclient.With(
		ctx,
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      configs.GeneralConfigMapName,
				Namespace: configs.SystemNamespace,
			},
		},
	)
	dynamicScheme := runtime.NewScheme()
	_ = fakekubeclientset.AddToScheme(dynamicScheme)

	dynamicclient.With(ctx, dynamicScheme)

	controller := NewController(ctx, configmap.NewStaticWatcher(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: "config-kafka-features",
		},
	}, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: feature.FlagsConfigName,
		},
	}))
	if controller == nil {
		t.Error("failed to create controller: <nil>")
	}
}
