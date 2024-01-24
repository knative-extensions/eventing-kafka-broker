//go:build e2e
// +build e2e

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

package e2e_new

import (
	"testing"

	"knative.dev/eventing-kafka-broker/test/rekt/features"
	"knative.dev/pkg/system"
	"knative.dev/reconciler-test/pkg/k8s"
	"knative.dev/reconciler-test/pkg/knative"
)

// TestBrokerWithConfig tests a brokers values for ReplicationFactor and number
// of partitions if set in the broker configmap.
func TestBrokerWithConfig(t *testing.T) {
	t.Skip("Skipping due to https://github.com/knative-extensions/eventing-kafka-broker/issues/3592")

	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
	)

	env.Test(ctx, t, features.BrokerWithCustomReplicationFactorAndNumPartitions(env))
}
