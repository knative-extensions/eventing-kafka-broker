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

package v2

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	_ "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/informers/eventing/v1alpha1/consumergroup/fake"
	_ "knative.dev/eventing/pkg/client/injection/informers/eventing/v1/broker/fake"
	_ "knative.dev/eventing/pkg/client/injection/informers/eventing/v1/trigger/fake"
	_ "knative.dev/pkg/client/injection/kube/informers/core/v1/configmap/fake"
	_ "knative.dev/pkg/client/injection/kube/informers/core/v1/pod/fake"
	_ "knative.dev/pkg/client/injection/kube/informers/core/v1/secret/fake"
	_ "knative.dev/pkg/client/injection/kube/informers/core/v1/serviceaccount/filtered/fake"
	_ "knative.dev/pkg/client/injection/kube/informers/factory/filtered/fake"
	"knative.dev/pkg/configmap"
	reconcilertesting "knative.dev/pkg/reconciler/testing"

	filteredFactory "knative.dev/pkg/client/injection/kube/informers/factory/filtered"

	"knative.dev/eventing/pkg/auth"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
)

func TestNewController(t *testing.T) {
	ctx, _ := reconcilertesting.SetupFakeContext(t, setupInformerSelector)

	controller := NewController(ctx, configmap.NewStaticWatcher(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: "config-features",
		},
	}), &config.Env{})
	if controller == nil {
		t.Error("failed to create controller: <nil>")
	}
}

func setupInformerSelector(ctx context.Context) context.Context {
	return filteredFactory.WithSelectors(ctx, auth.OIDCLabelSelector)
}
