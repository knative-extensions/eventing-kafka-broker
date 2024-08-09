/*
 * Copyright 2024 The Knative Authors
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

	batchv1 "k8s.io/api/batch/v1"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/k8s"
)

func JobSucceeded(name string) *feature.Feature {
	f := feature.NewFeatureNamed("Job " + name + " succeeded")

	f.Assert("Job success", func(ctx context.Context, t feature.T) {
		isSucceeded := func(job *batchv1.Job) bool {
			return k8s.IsJobComplete(job) && job.Status.Succeeded > 0
		}
		if err := k8s.WaitForJobCondition(ctx, t, name, isSucceeded); err != nil {
			t.Error("Job did not turn into done state", err)
		}
	})

	return f
}
