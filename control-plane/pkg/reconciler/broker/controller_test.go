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

	"github.com/Shopify/sarama"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	_ "knative.dev/eventing/pkg/client/injection/informers/eventing/v1beta1/broker/fake"
	_ "knative.dev/eventing/pkg/client/injection/informers/eventing/v1beta1/trigger/fake"
	_ "knative.dev/pkg/client/injection/ducks/duck/v1/addressable/fake"
	_ "knative.dev/pkg/client/injection/kube/informers/core/v1/pod/fake"
	"knative.dev/pkg/configmap"
	reconcilertesting "knative.dev/pkg/reconciler/testing"
)

func TestNewController(t *testing.T) {
	ctx, _ := reconcilertesting.SetupFakeContext(t)

	NewClusterAdmin = func(addrs []string, conf *sarama.Config) (sarama.ClusterAdmin, error) {
		return nil, nil
	}

	controller := NewController(
		ctx,
		configmap.NewStaticWatcher(&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name: "cm",
			},
		}),
		&Configs{
			EnvConfigs: EnvConfigs{
				GeneralConfigMapName: "cm",
			},
		})
	if controller == nil {
		t.Error("failed to create controller: <nil>")
	}
}
