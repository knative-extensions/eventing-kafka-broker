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
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/feature"
)

func Copy(from types.NamespacedName, toName string) feature.StepFn {
	return func(ctx context.Context, t feature.T) {
		cm, err := kubeclient.Get(ctx).CoreV1().
			ConfigMaps(from.Namespace).
			Get(ctx, from.Name, metav1.GetOptions{})
		require.Nil(t, err)

		ns := environment.FromContext(ctx).Namespace()
		toCm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns,
				Name:      toName,
			},
			Data:       cm.Data,
			BinaryData: cm.BinaryData,
		}

		_, err = kubeclient.Get(ctx).CoreV1().
			ConfigMaps(toCm.GetNamespace()).
			Create(ctx, toCm, metav1.CreateOptions{})
		if apierrors.IsAlreadyExists(err) {
			return
		}
		require.Nil(t, err)
	}
}
