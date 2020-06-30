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

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"

	"knative.dev/eventing/pkg/apis/eventing"
	"knative.dev/eventing/pkg/apis/eventing/v1beta1"
	brokerinformer "knative.dev/eventing/pkg/client/injection/informers/eventing/v1beta1/broker"
	triggerinformer "knative.dev/eventing/pkg/client/injection/informers/eventing/v1beta1/trigger"
	triggerreconciler "knative.dev/eventing/pkg/client/injection/reconciler/eventing/v1beta1/trigger"
	eventinglisters "knative.dev/eventing/pkg/client/listers/eventing/v1beta1"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka"
)

func NewController(ctx context.Context, _ configmap.Watcher) *controller.Impl {

	brokerInformer := brokerinformer.Get(ctx)
	triggerInformer := triggerinformer.Get(ctx)
	triggerLister := triggerInformer.Lister()

	reconciler := &Reconciler{
		logger: logging.FromContext(ctx).Desugar(),
	}

	impl := triggerreconciler.NewImpl(ctx, reconciler)

	// Filter Brokers and enqueue associated Triggers
	brokerInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: kafka.BrokerClassFilter(),
		Handler:    enqueueTriggers(reconciler.logger, triggerLister, impl.Enqueue),
	})

	return impl
}

func enqueueTriggers(
	logger *zap.Logger,
	lister eventinglisters.TriggerLister,
	enqueue func(obj interface{})) cache.ResourceEventHandler {

	return controller.HandleAll(func(obj interface{}) {

		if broker, ok := obj.(*v1beta1.Broker); ok {

			selector := labels.SelectorFromSet(map[string]string{eventing.BrokerLabelKey: broker.Name})
			triggers, err := lister.Triggers(broker.Namespace).List(selector)
			if err != nil {
				logger.Warn("Failed to list triggers", zap.Any("broker", broker), zap.Error(err))
				return
			}

			for _, trigger := range triggers {
				enqueue(trigger)
			}
		}
	})
}
