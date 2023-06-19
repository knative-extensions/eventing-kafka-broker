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

package statefulset

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	kubeclient "knative.dev/pkg/client/injection/kube/client/fake"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/knative"
)

func WaitForReplicas(name string, r int32) feature.StepFn {
	return func(ctx context.Context, t feature.T) {
		interval, timeout := environment.PollTimingsFromContext(ctx)

		var lastErr error
		err := wait.PollImmediate(interval, 2*timeout, func() (bool, error) {
			ss, err := kubeclient.Get(ctx).AppsV1().
				StatefulSets(knative.KnativeNamespaceFromContext(ctx)).
				Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				lastErr = err
				return false, nil
			}

			if *ss.Spec.Replicas == r {
				return true, nil
			}

			lastErr = fmt.Errorf("expected %v replicas, got %v", r, *ss.Spec.Replicas)
			return false, nil
		})
		if err != nil {
			t.Errorf("Failed while waiting for statefulset %s to have replicas %v: %v", name, r, lastErr)
		}
	}
}
