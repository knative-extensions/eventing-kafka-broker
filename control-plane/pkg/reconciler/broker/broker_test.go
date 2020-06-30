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

package broker

import (
	"context"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	. "knative.dev/pkg/reconciler/testing"

	fakeeventingclient "knative.dev/eventing/pkg/client/injection/client/fake"
	brokerreconciler "knative.dev/eventing/pkg/client/injection/reconciler/eventing/v1beta1/broker"
	reconcilertesting "knative.dev/eventing/pkg/reconciler/testing/v1beta1"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka"
	. "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/testing"
)

const (
	brokerNamespace = "test-namespace"
	brokerName      = "test-broker"
)

var (
	finalizerUpdatedEvent = Eventf(
		corev1.EventTypeNormal,
		"FinalizerUpdate",
		fmt.Sprintf(`Updated %q finalizers`, brokerName),
	)
)

func TestBrokerReconciliation(t *testing.T) {

	testKey := fmt.Sprintf("%s/%s", brokerNamespace, brokerName)

	table := TableTest{
		{
			Name: "Reconciled normal",
			Objects: []runtime.Object{
				NewBroker(),
			},
			Key: testKey,
			WantEvents: []string{
				finalizerUpdatedEvent,
				Eventf(
					corev1.EventTypeNormal,
					brokerReconciled,
					fmt.Sprintf(`%s reconciled: "%s/%s"`, broker, brokerNamespace, brokerName),
				),
			},
		},
	}

	table.Test(t, NewFactory(func(ctx context.Context, listers *Listers) controller.Reconciler {

		logger := logging.FromContext(ctx)

		reconciler := &Reconciler{
			logger: logger.Desugar(),
		}

		return brokerreconciler.NewReconciler(
			ctx,
			logger,
			fakeeventingclient.Get(ctx),
			listers.GetBrokerLister(),
			controller.GetEventRecorder(ctx),
			reconciler,
			kafka.BrokerClass,
		)
	}))
}

// NewBroker creates a new Broker with broker class equals to kafka.BrokerClass
func NewBroker(options ...reconcilertesting.BrokerOption) runtime.Object {
	return reconcilertesting.NewBroker(
		brokerName,
		brokerNamespace,
		append(
			options,
			reconcilertesting.WithBrokerClass(kafka.BrokerClass),
		)...,
	)
}
