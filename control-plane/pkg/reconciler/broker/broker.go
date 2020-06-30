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

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1beta1"
	"knative.dev/pkg/reconciler"
)

const (
	broker           = "Broker"
	brokerReconciled = broker + "Reconciled"
)

type Reconciler struct {
	logger *zap.Logger
}

func (r *Reconciler) ReconcileKind(ctx context.Context, broker *eventing.Broker) reconciler.Event {

	r.logger.Debug("reconciling Broker", zap.Any("broker", broker))

	return reconciledNormal(broker.Namespace, broker.Name)
}

func (r *Reconciler) FinalizeKind(ctx context.Context, broker *eventing.Broker) reconciler.Event {
	r.logger.Debug("finalizing Broker", zap.Any("broker", broker))

	return reconciledNormal(broker.Namespace, broker.Name)
}

func reconciledNormal(namespace, name string) reconciler.Event {
	return reconciler.NewEvent(
		corev1.EventTypeNormal,
		brokerReconciled,
		fmt.Sprintf(`%s reconciled: "%s/%s"`, broker, namespace, name),
	)
}
