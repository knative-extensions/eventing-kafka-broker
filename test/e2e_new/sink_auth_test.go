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
	"time"

	"knative.dev/eventing-kafka-broker/test/rekt/resources/kafkasink"
	"knative.dev/eventing-kafka-broker/test/rekt/resources/kafkatopic"

	testpkg "knative.dev/eventing-kafka-broker/test/pkg"
	"knative.dev/eventing/test/rekt/features/oidc"
	"knative.dev/pkg/system"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/eventshub"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/k8s"
	"knative.dev/reconciler-test/pkg/knative"
)

func TestKafkaSinkSupportsOIDC(t *testing.T) {
	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.WithPollTimings(4*time.Second, 12*time.Minute),
		environment.Managed(t),
		eventshub.WithTLS(t),
	)

	topic := feature.MakeRandomK8sName("topic")
	sink := feature.MakeRandomK8sName("kafkasink")
	env.Prerequisite(ctx, t, kafkatopic.GoesReady(topic))
	env.Prerequisite(ctx, t, kafkasink.GoesReady(sink, topic, testpkg.BootstrapServersPlaintextArr))

	env.TestSet(ctx, t, oidc.AddressableOIDCConformance(kafkasink.GVR(), "KafkaSink", sink, env.Namespace()))
}
