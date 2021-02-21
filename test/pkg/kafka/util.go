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

package kafka

import (
	"context"
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	testlib "knative.dev/eventing/test/lib"
)

func verifyJobSucceeded(
	ctx context.Context,
	client kubernetes.Interface,
	tracker *testlib.Tracker,
	namespacedName types.NamespacedName,
	job *batchv1.Job) error {

	job, err := client.BatchV1().Jobs(namespacedName.Namespace).Create(ctx, job, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("failed to create job: %w", err)
	}

	gvr, _ := meta.UnsafeGuessKindToResource(job.GroupVersionKind())
	tracker.Add(gvr.Group, gvr.Version, gvr.Resource, namespacedName.Namespace, namespacedName.Name)

	return wait.PollImmediate(interval, timeout, func() (bool, error) {
		job, err := client.BatchV1().Jobs(namespacedName.Namespace).Get(ctx, namespacedName.Name, metav1.GetOptions{})
		if err != nil {
			return false, fmt.Errorf("failed to get job: %w", err)
		}
		if job.Status.Succeeded >= 1 {
			return true, nil
		}
		return false, nil
	})
}
