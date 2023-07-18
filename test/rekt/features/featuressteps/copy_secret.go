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

	"k8s.io/client-go/kubernetes/scheme"
	ref "k8s.io/client-go/tools/reference"
	"knative.dev/eventing/pkg/utils"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/logging"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/feature"
)

func CopySecretInTestNamespace(namespace, sourceSecretName, targetSecretName string) feature.StepFn {
	return func(ctx context.Context, t feature.T) {
		env := environment.FromContext(ctx)
		tgtNs := environment.FromContext(ctx).Namespace()
		secret, err := utils.CopySecretWithName(
			kubeclient.Get(ctx).CoreV1(),
			namespace,
			sourceSecretName,
			tgtNs,
			targetSecretName,
			"default",
		)
		if err != nil {
			t.Fatalf("Failed to copy secret %s from %s to %s: %v", sourceSecretName, namespace, tgtNs, err)
		}
		reference, err := ref.GetReference(scheme.Scheme, secret)
		if err != nil {
			logging.FromContext(ctx).Fatalf("Could not construct reference to: '%#v' due to: '%v'", secret, err)
		}
		env.Reference(*reference)
	}
}
