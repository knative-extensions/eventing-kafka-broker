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
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/k8s"
	"knative.dev/reconciler-test/pkg/knative"
)

// ExistsContract check that the broker contract config map exists
func ExistsContract(ctx context.Context, t feature.T) {
	Exists("kafka-broker-brokers-triggers", knative.KnativeNamespaceFromContext(ctx))(ctx, t)
}

// Exists check that the provided config map exists
func Exists(name string, namespace string) feature.StepFn {
	return func(ctx context.Context, t feature.T) {
		interval, timeout := k8s.PollTimings(ctx, []time.Duration{})
		err := wait.PollUntilContextTimeout(ctx, interval, timeout, true, func(ctx2 context.Context) (bool, error) {
			client := kubeclient.Get(ctx)
			_, err := client.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				if apierrors.IsNotFound(err) {
					// keep polling
					return false, nil
				}
				return false, err
			}
			return true, nil
		})
		if err != nil {
			t.Fatal("Error while checking if the config map %s/%s exists: %v", name, namespace, err)
		}
	}
}

// DoesNotExist check that the provided config map does not exist
func DoesNotExist(name string, namespace string) feature.StepFn {
	return func(ctx context.Context, t feature.T) {
		client := kubeclient.Get(ctx)
		_, err := client.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
		if err == nil {
			t.Fatal("Config map %s/%s exists but it should have not", name, namespace)
		} else if !apierrors.IsNotFound(err) {
			t.Fatal("Error while checking if the config map %s/%s does not exist: %v", name, namespace, err)
		}
	}
}

func ExistsInTestNamespace(name string) feature.StepFn {
	return func(ctx context.Context, t feature.T) {
		ns := environment.FromContext(ctx).Namespace()
		Exists(name, ns)(ctx, t)
	}
}
