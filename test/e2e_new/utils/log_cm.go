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

package utils

import (
	"context"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/knative"
)

// LogContractConfigMap logs the broker config map
func LogContractConfigMap(ctx context.Context, t feature.T) {
	LogConfigMap("kafka-broker-brokers-triggers", knative.KnativeNamespaceFromContext(ctx))(ctx, t)
}

// LogConfigMap logs the provided config map
func LogConfigMap(name string, namespace string) feature.StepFn {
	return func(ctx context.Context, t feature.T) {
		cm, err := kubeclient.Get(ctx).CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
		require.NoError(t, err)

		t.Logf("Config map %s/%s", cm.Name, cm.Namespace)
		for k, v := range cm.Data {
			t.Logf("%s: %s", k, v)
		}
		for k, v := range cm.BinaryData {
			t.Logf("%s: %s", k, string(v))
		}
	}
}
