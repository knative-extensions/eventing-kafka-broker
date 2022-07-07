/*
 * Copyright 2022 The Knative Authors
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

package consumer

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"knative.dev/pkg/configmap"
	reconcilertesting "knative.dev/pkg/reconciler/testing"

	fakekubeclientset "k8s.io/client-go/kubernetes/fake"
	dynamicclient "knative.dev/pkg/injection/clients/dynamicclient/fake"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"

	_ "knative.dev/pkg/client/injection/ducks/duck/v1/addressable/fake"

	_ "knative.dev/pkg/client/injection/kube/informers/apps/v1/statefulset/fake"
	_ "knative.dev/pkg/client/injection/kube/informers/core/v1/node/fake"
	_ "knative.dev/pkg/client/injection/kube/informers/core/v1/pod/fake"
	_ "knative.dev/pkg/client/injection/kube/informers/core/v1/secret/fake"

	_ "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/informers/eventing/v1alpha1/consumer/fake"
	_ "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/informers/eventing/v1alpha1/consumergroup/fake"
)

func TestNewController(t *testing.T) {
	ctx, _ := reconcilertesting.SetupFakeContext(t)

	dynamicScheme := runtime.NewScheme()
	_ = fakekubeclientset.AddToScheme(dynamicScheme)

	dynamicclient.With(ctx, dynamicScheme)

	t.Setenv("CONSUMER_DATA_PLANE_CONFIG_FORMAT", "json")

	controller := NewController(ctx, configmap.NewStaticWatcher(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: "config-kafka-features",
		},
	}))
	if controller == nil {
		t.Error("failed to create controller: <nil>")
	}
}

func TestFormatSerDeFromString(t *testing.T) {
	tt := []struct {
		format string
		serde  contract.FormatSerDe
	}{
		{
			format: "protobuf",
			serde:  contract.FormatSerDe{Format: contract.Protobuf},
		},
		{
			format: "json",
			serde:  contract.FormatSerDe{Format: contract.Json},
		},
	}

	for _, tc := range tt {
		t.Run(tc.format, func(t *testing.T) {

			got := formatSerDeFromString(tc.format)

			require.Equal(t, tc.serde, got, "%v %v", tc.serde, got)
		})
	}

}
