//go:build e2e
// +build e2e

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

package e2e_new

import (
	"context"
	"testing"

	"knative.dev/pkg/system"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/k8s"
	"knative.dev/reconciler-test/pkg/knative"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	"knative.dev/eventing-kafka-broker/test/e2e_new/single_partition_config"
	newfilters "knative.dev/eventing/test/rekt/features/new_trigger_filters"
	"knative.dev/eventing/test/rekt/resources/broker"
)

func TestNewTriggerFilters(t *testing.T) {
	t.Parallel()

	ctx, env := global.Environment(
		knative.WithKnativeNamespace(system.Namespace()),
		knative.WithLoggingConfig,
		knative.WithTracingConfig,
		k8s.WithEventListener,
		environment.Managed(t),
	)

	env.ParallelTestSet(ctx, t, newfilters.NewFiltersFeatureSet(InstallKafkaBrokerStepFn))
}

func InstallKafkaBrokerStepFn(brokerName string) feature.StepFn {
	return func(ctx context.Context, t feature.T) {
		install, cmName := single_partition_config.MakeInstall()
		install(ctx, t)
		broker.Install(
			brokerName,
			broker.WithBrokerClass(kafka.BrokerClass),
			broker.WithConfig(cmName),
		)(ctx, t)
	}
}
