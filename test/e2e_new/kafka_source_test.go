//go:build e2e
// +build e2e

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

package e2e_new

import (
	"testing"
	"time"

	"knative.dev/pkg/system"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/eventshub"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/k8s"
	"knative.dev/reconciler-test/pkg/knative"

	"knative.dev/eventing-kafka-broker/test/rekt/features"
)

func TestKafkaSourceCreateSecretsAfterKafkaSource(t *testing.T) {

	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.WithPollTimings(PollInterval, PollTimeout),
		environment.Managed(t),
	)

	env.Test(ctx, t, features.CreateSecretsAfterKafkaSource())
}

func TestKafkaSourceDeletedFromContractConfigMaps(t *testing.T) {

	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.WithPollTimings(PollInterval, PollTimeout),
		environment.Managed(t),
	)
	t.Cleanup(env.Finish)

	env.Test(ctx, t, features.SetupKafkaSources("permanent-kafka-source-", 21))
	env.Test(ctx, t, features.SetupAndCleanupKafkaSources("x-kafka-source-", 42))
	env.Test(ctx, t, features.KafkaSourcesAreNotPresentInContractConfigMaps("x-kafka-source-"))
}

func TestKafkaSourceScale(t *testing.T) {

	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.WithPollTimings(PollInterval, PollTimeout),
		environment.Managed(t),
	)
	t.Cleanup(env.Finish)

	env.Test(ctx, t, features.ScaleKafkaSource())
}

func TestKafkaSourceInitialOffsetEarliest(t *testing.T) {
	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.WithPollTimings(PollInterval, PollTimeout),
		environment.Managed(t),
	)

	topic := feature.MakeRandomK8sName("kafka-topic-earliest")

	env.Test(ctx, t, features.SetupKafkaTopicWithEvents(2, topic))
	env.Test(ctx, t, features.KafkaSourceInitialOffsetEarliest(2, topic))
}

func TestKafkaSourceBinaryEvent(t *testing.T) {
	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.WithPollTimings(PollInterval, PollTimeout),
		environment.Managed(t),
	)

	env.Test(ctx, t, features.KafkaSourceBinaryEvent())
}

func TestKafkaSourceBinaryEventWithExtensions(t *testing.T) {
	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.WithPollTimings(PollInterval, PollTimeout),
		environment.Managed(t),
	)

	env.Test(ctx, t, features.KafkaSourceBinaryEventWithExtensions())
}

func TestKafkaSourceStructuredEvent(t *testing.T) {
	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.WithPollTimings(PollInterval, PollTimeout),
		environment.Managed(t),
	)

	env.Test(ctx, t, features.KafkaSourceStructuredEvent())
}

func TestKafkaSourceWithExtensions(t *testing.T) {
	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.WithPollTimings(PollInterval, PollTimeout),
		environment.Managed(t),
	)

	env.Test(ctx, t, features.KafkaSourceWithExtensions())
}

func TestKafkaSourceTLS(t *testing.T) {

	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.WithPollTimings(PollInterval, PollTimeout),
		environment.Managed(t),
	)

	kafkaSource := feature.MakeRandomK8sName("kafkaSource")
	kafkaSink := feature.MakeRandomK8sName("kafkaSink")
	topic := feature.MakeRandomK8sName("topic")

	env.Test(ctx, t, features.KafkaSourceTLS(kafkaSource, kafkaSink, topic))
}

func TestKafkaSourceSASL(t *testing.T) {

	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.WithPollTimings(PollInterval, PollTimeout),
		environment.Managed(t),
	)

	env.Test(ctx, t, features.KafkaSourceSASL())
}

func TestKafkaSourceUpdate(t *testing.T) {

	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.WithPollTimings(PollInterval, PollTimeout),
		environment.Managed(t),
	)

	kafkaSource := feature.MakeRandomK8sName("kafkaSource")
	kafkaSink := feature.MakeRandomK8sName("kafkaSink")
	topic := feature.MakeRandomK8sName("topic")

	// First, send an arbitrary event to Kafka and let KafkaSource
	// forward the event to the sink.
	env.Test(ctx, t, features.KafkaSourceTLS(kafkaSource, kafkaSink, topic))

	// Second, use the same KafkaSource, update it, send a new event to
	// Kafka (through the same KafkaSink using same Kafka Topic). And verify that
	// the new event is delivered properly.
	env.Test(ctx, t, features.KafkaSourceWithEventAfterUpdate(kafkaSource, kafkaSink, topic))
}

func TestKafkaSourceKedaScaling(t *testing.T) {
	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.WithPollTimings(5*time.Second, 4*time.Minute),
		environment.Managed(t),
	)

	env.Test(ctx, t, features.KafkaSourceScalesToZeroWithKeda())

}

func TestKafkaSourceScaledObject(t *testing.T) {
	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.WithPollTimings(5*time.Second, 4*time.Minute),
		environment.Managed(t),
	)

	env.Test(ctx, t, features.KafkaSourceScaledObjectHasNoEmptyAuthRef())

}

func TestKafkaSourceTLSSink(t *testing.T) {

	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.WithPollTimings(PollInterval, PollTimeout),
		eventshub.WithTLS(t),
		environment.Managed(t),
	)

	env.ParallelTest(ctx, t, features.KafkaSourceTLSSink())
}

func TestKafkaSourceTLSTrustBundle(t *testing.T) {

	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.WithPollTimings(PollInterval, PollTimeout),
		environment.Managed(t),
	)

	env.ParallelTest(ctx, t, features.KafkaSourceTLSSinkTrustBundle())
}
