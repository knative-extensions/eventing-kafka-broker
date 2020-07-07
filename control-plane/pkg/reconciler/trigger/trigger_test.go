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

package trigger

import (
	"context"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	fakeeventingclient "knative.dev/eventing/pkg/client/injection/client/fake"
	triggerreconciler "knative.dev/eventing/pkg/client/injection/reconciler/eventing/v1beta1/trigger"
	"knative.dev/eventing/pkg/logging"
	reconcilertesting "knative.dev/eventing/pkg/reconciler/testing/v1beta1"
	"knative.dev/pkg/controller"

	. "knative.dev/pkg/reconciler/testing"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/broker"
	. "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/testing"
)

const (
	// name of the trigger under test
	triggerName = "test-trigger"
	// namespace of the trigger under test
	triggerNamespace = "test-namespace"
	// broker name associated with trigger under test
	brokerName = "test-broker"
)

var (
	finalizerUpdatedEvent = Eventf(
		corev1.EventTypeNormal,
		"FinalizerUpdate",
		fmt.Sprintf(`Updated %q finalizers`, triggerName),
	)
)

func TestTriggerReconciliation(t *testing.T) {

	testKey := fmt.Sprintf("%s/%s", triggerNamespace, triggerName)

	configs := *DefaultConfigs

	table := TableTest{
		{
			Name: "Reconciled normal",
			Objects: []runtime.Object{
				newTrigger(),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
				Eventf(
					corev1.EventTypeNormal,
					triggerReconciled,
					fmt.Sprintf(`%s reconciled: "%s/%s"`, trigger, triggerNamespace, triggerName),
				),
			},
		},
	}

	table.Test(t, NewFactory(&configs, func(ctx context.Context, listers *Listers, configs *broker.Configs, row *TableRow) controller.Reconciler {

		logger := logging.FromContext(ctx)

		reconciler := &Reconciler{
			logger: logger,
		}

		return triggerreconciler.NewReconciler(
			ctx,
			logger.Sugar(),
			fakeeventingclient.Get(ctx),
			listers.GetTriggerLister(),
			controller.GetEventRecorder(ctx),
			reconciler,
		)
	}))
}

func newTrigger(options ...reconcilertesting.TriggerOption) runtime.Object {
	return reconcilertesting.NewTrigger(
		triggerName,
		triggerNamespace,
		brokerName,
		options...,
	)
}
