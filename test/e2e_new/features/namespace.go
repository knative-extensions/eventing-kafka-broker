/*
 * Copyright 2023 The Knative Authors
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

package features

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/reconciler-test/pkg/feature"
)

func SetupNamespace(name string) *feature.Feature {
	f := feature.NewFeatureNamed("setup namespace")

	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
	}

	f.Setup(fmt.Sprintf("install namespace %s", name), func(ctx context.Context, t feature.T) {
		_, err := kubeclient.Get(ctx).CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			t.Fatal(err)
		}

	})

	return f
}

func CleanupNamespace(name string) *feature.Feature {
	f := feature.NewFeatureNamed("delete namespace")

	f.Setup(fmt.Sprintf("delete namespace %s", name), func(ctx context.Context, t feature.T) {
		pp := metav1.DeletePropagationForeground
		err := kubeclient.Get(ctx).CoreV1().Namespaces().Delete(ctx, name, metav1.DeleteOptions{
			PropagationPolicy: &pp,
		})
		if err != nil && !apierrors.IsNotFound(err) {
			t.Fatal(err)
		}

	})

	f.Assert(fmt.Sprintf("wait for namespace %s to be deleted", name), func(ctx context.Context, t feature.T) {
		err := wait.PollImmediate(100*time.Millisecond, time.Minute, func() (done bool, err error) {
			_, err = kubeclient.Get(ctx).CoreV1().Namespaces().Get(ctx, name, metav1.GetOptions{})
			if err != nil && apierrors.IsNotFound(err) {
				return true, nil
			}
			return false, err
		})
		if err != nil {
			t.Errorf("failed while waiting for namespace %s to be deleted %v", name, err)
		}
	})

	return f
}
