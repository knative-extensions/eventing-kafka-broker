//go:build e2e
// +build e2e

/*
 * Copyright 2020 The Knative Authors
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

package e2enew

import (
	"testing"

	"knative.dev/eventing-kafka-broker/test/rekt/features"
	"knative.dev/pkg/system"
	"knative.dev/reconciler-test/pkg/k8s"
	"knative.dev/reconciler-test/pkg/knative"
)

/*
+---------+     +--------+     +---------+   +---------------------------+
|  Broker +---->+ Trigger+---->+KafkaSink+-->+kafka consumer (test image)|
+----+----+     +--------+     +----+----+   +---------------------------+

	|                              ^
	|          +--------+          |
	+--------->+ Trigger+----------+
	           +--------+
*/
func TestBrokerTriggersSink(t *testing.T) {
	t.Skip("Skipping due to https://github.com/knative-extensions/eventing-kafka-broker/issues/2951")

	// Run Test In Parallel With Others
	t.Parallel()

	//Create The Test Context / Environment
	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithObservabilityConfig,
		k8s.WithEventListener,
	)

	env.Test(ctx, t, features.BrokerWithTriggersAndKafkaSink(env))
}
