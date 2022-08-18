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

package tracing

import (
	"context"

	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/test/zipkin"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/knative"
)

func WithZipkin(ctx context.Context, env environment.Environment) (context.Context, error) {
	err := zipkin.SetupZipkinTracingFromConfigTracing(ctx,
		kubeclient.Get(ctx),
		logging.FromContext(ctx).Infof,
		knative.KnativeNamespaceFromContext(ctx))
	zipkin.ZipkinTracingEnabled = true
	return ctx, err
}
