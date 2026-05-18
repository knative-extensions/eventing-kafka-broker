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

package v2

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"
	configmapinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/configmap"
	serviceaccountinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/serviceaccount/filtered"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"

	"knative.dev/eventing/pkg/apis/feature"
	"knative.dev/eventing/pkg/auth"
	eventingclient "knative.dev/eventing/pkg/client/injection/client"
	brokerinformer "knative.dev/eventing/pkg/client/injection/informers/eventing/v1/broker"
	triggerinformer "knative.dev/eventing/pkg/client/injection/informers/eventing/v1/trigger"
	triggerreconciler "knative.dev/eventing/pkg/client/injection/reconciler/eventing/v1/trigger"

	apiseventing "knative.dev/eventing/pkg/apis/eventing"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1"
	eventinglisters "knative.dev/eventing/pkg/client/listers/eventing/v1"

	apisconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/config"
	consumergroupclient "knative.dev/eventing-kafka-broker/control-plane/pkg/client/injection/client"
	consumergroupinformer "knative.dev/eventing-kafka-broker/control-plane/pkg/client/injection/informers/internalskafkaeventing/v1alpha1/consumergroup"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/consumergroup"

	kubeclient "knative.dev/pkg/client/injection/kube/client"
	secretinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/secret"
)

const (
	ControllerAgentName = "kafka-trigger-controller"

	FinalizerName = "kafka.triggers.eventing.knative.dev"
)

func NewController(ctx context.Context, watcher configmap.Watcher, configs *config.Env) *controller.Impl {

	logger := logging.FromContext(ctx).Desugar()

	brokerInformer := brokerinformer.Get(ctx)
	triggerInformer := triggerinformer.Get(ctx)
	triggerLister := triggerInformer.Lister()
	consumerGroupInformer := consumergroupinformer.Get(ctx)
	oidcServiceAccountInformer := serviceaccountinformer.Get(ctx, auth.OIDCLabelSelector)

	var globalResync func()

	coreFeatureStore := feature.NewStore(logger.Sugar().Named("feature-config-store"), func(_ string, _ interface{}) {
		if globalResync != nil {
			globalResync()
		}
	})
	coreFeatureStore.WatchConfigs(watcher)

	kafkaFeatureStore := apisconfig.NewStore(ctx, func(_ string, _ *apisconfig.KafkaFeatureFlags) {
		if globalResync != nil {
			globalResync()
		}
	})
	kafkaFeatureStore.WatchConfigs(watcher)

	reconciler := &Reconciler{
		BrokerLister:         brokerInformer.Lister(),
		ConfigMapLister:      configmapinformer.Get(ctx).Lister(),
		ServiceAccountLister: oidcServiceAccountInformer.Lister(),
		EventingClient:       eventingclient.Get(ctx),
		Env:                  configs,
		ConsumerGroupLister:  consumerGroupInformer.Lister(),
		InternalsClient:      consumergroupclient.Get(ctx),
		SecretLister:         secretinformer.Get(ctx).Lister(),
		KubeClient:           kubeclient.Get(ctx),
	}

	impl := triggerreconciler.NewImpl(ctx, reconciler, func(impl *controller.Impl) controller.Options {
		return controller.Options{
			ConfigStore:       apisconfig.Stores{coreFeatureStore, kafkaFeatureStore},
			FinalizerName:     FinalizerName,
			AgentName:         ControllerAgentName,
			SkipStatusUpdates: false,
			PromoteFilterFunc: filterTriggers(reconciler.BrokerLister),
		}
	})

	globalResync = func() {
		impl.FilteredGlobalResync(filterTriggers(reconciler.BrokerLister), triggerInformer.Informer())
	}

	triggerInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: filterTriggers(reconciler.BrokerLister),
		Handler:    controller.HandleAll(impl.Enqueue),
	})

	// Filter Brokers and enqueue associated Triggers
	brokerInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: kafka.BrokerClassFilter(),
		Handler:    enqueueTriggers(logger, triggerLister, impl.Enqueue, coreFeatureStore),
	})

	// ConsumerGroup changes and enqueue associated Trigger
	consumerGroupInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: consumergroup.Filter("trigger"),
		Handler:    controller.HandleAll(consumergroup.Enqueue("trigger", impl.EnqueueKey)),
	})

	// Reconciler Trigger when the OIDC service account changes
	oidcServiceAccountInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: filterOIDCServiceAccounts(triggerInformer.Lister(), brokerInformer.Lister()),
		Handler:    controller.HandleAll(impl.EnqueueControllerOf),
	})

	return impl
}

func filterTriggers(lister eventinglisters.BrokerLister) func(interface{}) bool {
	return func(obj interface{}) bool {
		trigger, ok := obj.(*eventing.Trigger)
		if !ok {
			return false
		}

		if hasKafkaBrokerTriggerFinalizer(trigger.Finalizers, FinalizerName) {
			return true
		}

		broker, err := lister.Brokers(trigger.Namespace).Get(trigger.Spec.Broker)
		if err != nil {
			return false
		}

		value, ok := broker.GetAnnotations()[apiseventing.BrokerClassKey]
		return ok && value == kafka.BrokerClass
	}
}

// filterOIDCServiceAccounts returns a function that returns true if the resource passed
// is a service account, which is owned by a trigger pointing to a the given broker class.
func filterOIDCServiceAccounts(triggerLister eventinglisters.TriggerLister, brokerLister eventinglisters.BrokerLister) func(interface{}) bool {
	return func(obj interface{}) bool {
		controlledByTrigger := controller.FilterController(&eventing.Trigger{})(obj)
		if !controlledByTrigger {
			return false
		}

		sa, ok := obj.(*corev1.ServiceAccount)
		if !ok {
			return false
		}

		owner := metav1.GetControllerOf(sa)
		if owner == nil {
			return false
		}

		trigger, err := triggerLister.Triggers(sa.Namespace).Get(owner.Name)
		if err != nil {
			return false
		}

		return filterTriggers(brokerLister)(trigger)
	}
}

func hasKafkaBrokerTriggerFinalizer(finalizers []string, finalizerName string) bool {
	for _, f := range finalizers {
		if f == finalizerName {
			return true
		}
	}
	return false
}

func enqueueTriggers(
	logger *zap.Logger,
	lister eventinglisters.TriggerLister,
	enqueue func(obj interface{}),
	featureStore *feature.Store) cache.ResourceEventHandler {

	return controller.HandleAll(func(obj interface{}) {

		if broker, ok := obj.(*eventing.Broker); ok {
			features := featureStore.Load()

			selector := labels.SelectorFromSet(map[string]string{apiseventing.BrokerLabelKey: broker.Name})
			triggers, err := lister.Triggers(metav1.NamespaceAll).List(selector)
			if err != nil {
				logger.Warn("Failed to list triggers", zap.Any("broker", broker), zap.Error(err))
				return
			}

			for _, trigger := range triggers {
				if features.IsCrossNamespaceEventLinks() {
					if trigger.Spec.BrokerRef != nil && trigger.Spec.BrokerRef.Namespace == broker.Namespace {
						enqueue(trigger)
					}
				} else if trigger.Namespace == broker.Namespace {
					enqueue(trigger)
				}
			}
		}
	})
}
