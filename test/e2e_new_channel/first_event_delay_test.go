//go:build e2e

/*
 * Copyright 2026 The Knative Authors
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

package e2enewchannel

import (
	"fmt"
	"testing"
	"time"

	"knative.dev/pkg/system"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/k8s"
	"knative.dev/reconciler-test/pkg/knative"

	"knative.dev/eventing-kafka-broker/test/rekt/features/kafkachannel"
)

func TestKafkaChannelFirstEventDelay(t *testing.T) {
	t.Parallel()

	const (
		iterations    = 10
		numPartitions = "32"
	)

	for i := 0; i < iterations; i++ {
		i := i
		t.Run(fmt.Sprintf("iteration-%d", i), func(t *testing.T) {
			start := time.Now()

			ctx, env := global.Environment(
				knative.WithKnativeNamespace(system.Namespace()),
				knative.WithLoggingConfig,
				knative.WithObservabilityConfig,
				k8s.WithEventListener,
				environment.WithPollTimings(2*time.Second, 3*time.Minute),
				environment.Managed(t),
			)

			channelName := feature.MakeRandomK8sName("fed-ch")

			f := kafkachannel.FirstEventDelay(channelName, numPartitions)
			env.Test(ctx, t, f)

			elapsed := time.Since(start)
			t.Logf("iteration %d completed in %v", i, elapsed)
		})
	}
}
