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

package sink

import (
	"testing"

	"knative.dev/pkg/configmap"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/stretchr/testify/assert"
	reconcilertesting "knative.dev/pkg/reconciler/testing"

	_ "knative.dev/pkg/client/injection/kube/client/fake"
	_ "knative.dev/pkg/client/injection/kube/informers/core/v1/configmap/fake"
	_ "knative.dev/pkg/client/injection/kube/informers/core/v1/pod/fake"
	secretinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/secret/fake"

	"k8s.io/apimachinery/pkg/types"
	_ "knative.dev/eventing-kafka-broker/control-plane/pkg/client/injection/informers/eventing/v1alpha1/kafkasink/fake"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/pkg/system"

	eventingtlstesting "knative.dev/eventing/pkg/eventingtls/eventingtlstesting"
)

func TestNewController(t *testing.T) {

	ctx, _ := reconcilertesting.SetupFakeContext(t)

	secret := types.NamespacedName{
		Namespace: system.Namespace(),
		Name:      sinkIngressTLSSecretName,
	}

	_ = secretinformer.Get(ctx).Informer().GetStore().Add(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: secret.Namespace,
			Name:      secret.Name,
		},
		Data: map[string][]byte{
			"ca.crt": eventingtlstesting.CA,
		},
		Type: corev1.SecretTypeTLS,
	})

	controller := NewController(ctx, configmap.NewStaticWatcher(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: "config-features",
		},
	},
	), &config.Env{
		IngressPodPort: "8080",
	})

	assert.NotNil(t, controller, "controller is nil")
}
