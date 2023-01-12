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

package configmap

import (
	"context"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/knative"
)

// DeleteContract deletes the broker config map
func DeleteContract(ctx context.Context, t feature.T) {
	Delete("kafka-broker-brokers-triggers", knative.KnativeNamespaceFromContext(ctx))(ctx, t)
}

// Delete the provided config map
func Delete(name string, namespace string) feature.StepFn {
	return func(ctx context.Context, t feature.T) {
		err := kubeclient.Get(ctx).CoreV1().ConfigMaps(namespace).Delete(ctx, name, metav1.DeleteOptions{})
		require.NoError(t, err)
	}
}
