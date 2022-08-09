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

package featuressteps

import (
	"context"

	"knative.dev/eventing/pkg/utils"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/feature"
)

func CopySecretInTestNamespace(namespace, name string) feature.StepFn {
	return func(ctx context.Context, t feature.T) {
		tgtNs := environment.FromContext(ctx).Namespace()
		_, err := utils.CopySecret(
			kubeclient.Get(ctx).CoreV1(),
			namespace,
			name,
			tgtNs,
			"default",
		)
		if err != nil {
			t.Fatalf("failed to copy secret %s from %s to %s: %v", name, namespace, tgtNs, err)
		}
	}
}
