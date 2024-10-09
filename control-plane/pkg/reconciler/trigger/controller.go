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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"
	"knative.dev/eventing/pkg/auth"
	v1 "knative.dev/eventing/pkg/client/informers/externalversions/eventing/v1"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	configmapinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/configmap"
	podinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/pod"
	secretinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/secret"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/resolver"

	apisconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka/clientpool"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka/offset"

	apiseventing "knative.dev/eventing/pkg/apis/eventing"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1"
	"knative.dev/eventing/pkg/apis/feature"
	eventingclient "knative.dev/eventing/pkg/client/injection/client"
	brokerinformer "knative.dev/eventing/pkg/client/injection/informers/eventing/v1/broker"
	triggerinformer "knative.dev/eventing/pkg/client/injection/informers/eventing/v1/trigger"
	triggerreconciler "knative.dev/eventing/pkg/client/injection/reconciler/eventing/v1/trigger"
	eventinglisters "knative.dev/eventing/pkg/client/listers/eventing/v1"

	serviceaccountinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/serviceaccount/filtered"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
)

const (
	ControllerAgentName = "kafka-trigger-controller"

	FinalizerName = "kafka.triggers.eventing.knative.dev"
)

func NewController(ctx context.Context, watcher configmap.Watcher, configs *config.Env) *controller.Impl {

	logger := logging.FromContext(ctx).Desugar()

	configmapInformer := configmapinformer.Get(ctx)
	brokerInformer := brokerinformer.Get(ctx)
	triggerInformer := triggerinformer.Get(ctx)
	triggerLister := triggerInformer.Lister()
	oidcServiceaccountInformer := serviceaccountinformer.Get(ctx, auth.OIDCLabelSelector)

	reconciler := &Reconciler{
		Reconciler: &base.Reconciler{
			KubeClient:                   kubeclient.Get(ctx),
			PodLister:                    podinformer.Get(ctx).Lister(),
			SecretLister:                 secretinformer.Get(ctx).Lister(),
			DataPlaneConfigMapNamespace:  configs.DataPlaneConfigMapNamespace,
			DataPlaneConfigConfigMapName: configs.DataPlaneConfigConfigMapName,
			ContractConfigMapName:        configs.ContractConfigMapName,
			ContractConfigMapFormat:      configs.ContractConfigMapFormat,
			DataPlaneNamespace:           configs.SystemNamespace,
			DispatcherLabel:              base.BrokerDispatcherLabel,
			ReceiverLabel:                base.BrokerReceiverLabel,
		},
		FlagsHolder: &FlagsHolder{
			Flags: feature.Flags{},
		},
		BrokerLister:              brokerInformer.Lister(),
		ConfigMapLister:           configmapInformer.Lister(),
		EventingClient:            eventingclient.Get(ctx),
		Env:                       configs,
		BrokerClass:               kafka.BrokerClass,
		DataPlaneConfigMapLabeler: base.NoopConfigmapOption,
		KafkaFeatureFlags:         apisconfig.DefaultFeaturesConfig(),
		InitOffsetsFunc:           offset.InitOffsets,
		ServiceAccountLister:      oidcServiceaccountInformer.Lister(),
	}

	clientPool := clientpool.Get(ctx)
	if clientPool == nil {
		reconciler.GetKafkaClusterAdmin = clientpool.DisabledGetKafkaClusterAdminFunc
		reconciler.GetKafkaClient = clientpool.DisabledGetClient
	} else {
		reconciler.GetKafkaClusterAdmin = clientPool.GetClusterAdmin
		reconciler.GetKafkaClient = clientPool.GetClient
	}

	impl := triggerreconciler.NewImpl(ctx, reconciler, func(impl *controller.Impl) controller.Options {
		return controller.Options{
			FinalizerName:     FinalizerName,
			AgentName:         ControllerAgentName,
			SkipStatusUpdates: false,
			PromoteFilterFunc: filterTriggers(reconciler.BrokerLister, kafka.BrokerClass, FinalizerName),
		}
	})

	setupFeatureStore(ctx, watcher, reconciler.FlagsHolder, impl, triggerInformer)

	reconciler.Resolver = resolver.NewURIResolverFromTracker(ctx, impl.Tracker)

	triggerInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: filterTriggers(reconciler.BrokerLister, kafka.BrokerClass, FinalizerName),
		Handler:    controller.HandleAll(impl.Enqueue),
	})

	// Filter Brokers and enqueue associated Triggers
	brokerInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: kafka.BrokerClassFilter(),
		Handler:    enqueueTriggers(logger, triggerLister, impl.Enqueue),
	})

	globalResync := func(_ interface{}) {
		impl.FilteredGlobalResync(filterTriggers(reconciler.BrokerLister, kafka.BrokerClass, FinalizerName), triggerInformer.Informer())
	}

	kafkaConfigStore := apisconfig.NewStore(ctx, func(name string, value *apisconfig.KafkaFeatureFlags) {
		reconciler.KafkaFeatureFlags.Reset(value)
		if globalResync != nil {
			globalResync(nil)
		}
	})
	kafkaConfigStore.WatchConfigs(watcher)

	featureStore := feature.NewStore(logging.FromContext(ctx).Named("feature-config-store"), func(name string, value interface{}) {
		if globalResync != nil {
			globalResync(nil)
		}
	})
	featureStore.WatchConfigs(watcher)

	configmapInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: controller.FilterWithNameAndNamespace(configs.DataPlaneConfigMapNamespace, configs.ContractConfigMapName),
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc:    globalResync,
			DeleteFunc: globalResync,
		},
	})

	reconciler.Tracker = impl.Tracker
	secretinformer.Get(ctx).Informer().AddEventHandler(controller.HandleAll(reconciler.Tracker.OnChanged))

	// Reconciler Trigger when the OIDC service account changes
	oidcServiceaccountInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: filterOIDCServiceAccounts(triggerInformer.Lister(), brokerInformer.Lister(), kafka.BrokerClass, FinalizerName),
		Handler:    controller.HandleAll(impl.EnqueueControllerOf),
	})

	return impl
}

func filterTriggers(lister eventinglisters.BrokerLister, brokerClass string, finalizer string) func(interface{}) bool {
	return func(obj interface{}) bool {
		trigger, ok := obj.(*eventing.Trigger)
		if !ok {
			return false
		}

		if hasKafkaBrokerTriggerFinalizer(trigger.Finalizers, finalizer) {
			return true
		}

		broker, err := lister.Brokers(trigger.Namespace).Get(trigger.Spec.Broker)
		if err != nil {
			return false
		}

		value, ok := broker.GetAnnotations()[apiseventing.BrokerClassKey]
		return ok && value == brokerClass
	}
}

// filterOIDCServiceAccounts returns a function that returns true if the resource passed
// is a service account, which is owned by a trigger pointing to a the given broker class.
func filterOIDCServiceAccounts(triggerLister eventinglisters.TriggerLister, brokerLister eventinglisters.BrokerLister, brokerClass string, finalizer string) func(interface{}) bool {
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

		return filterTriggers(brokerLister, brokerClass, finalizer)(trigger)
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
	enqueue func(obj interface{})) cache.ResourceEventHandler {

	return controller.HandleAll(func(obj interface{}) {

		if broker, ok := obj.(*eventing.Broker); ok {

			selector := labels.SelectorFromSet(map[string]string{apiseventing.BrokerLabelKey: broker.Name})
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

func setupFeatureStore(ctx context.Context, watcher configmap.Watcher, flagsHolder *FlagsHolder, impl *controller.Impl, triggerInformer v1.TriggerInformer) {
	featureStore := feature.NewStore(
		logging.FromContext(ctx).Named("feature-config-eventing-store"),
		func(name string, value interface{}) {
			logger := logging.FromContext(ctx).Desugar()
			flags, ok := value.(feature.Flags)
			if !ok {
				logger.Warn("Features ConfigMap " + name + " updated but we didn't get expected flags. Skipping updating cached features")
			}
			logger.Debug("Features ConfigMap " + name + " updated. Updating cached features.")
			flagsHolder.FlagsLock.Lock()
			defer flagsHolder.FlagsLock.Unlock()
			flagsHolder.Flags = flags
			impl.GlobalResync(triggerInformer.Informer())
		},
	)
	featureStore.WatchConfigs(watcher)
}
