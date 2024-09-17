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

package trigger

import (
	"context"

	apisconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka/clientpool"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka/offset"

	"k8s.io/client-go/tools/cache"
	"knative.dev/eventing/pkg/auth"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	configmapinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/configmap"
	podinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/pod"
	secretinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/secret"

	serviceaccountinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/serviceaccount/filtered"

	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/resolver"

	"knative.dev/eventing/pkg/apis/feature"
	eventingclient "knative.dev/eventing/pkg/client/injection/client"
	brokerinformer "knative.dev/eventing/pkg/client/injection/informers/eventing/v1/broker"
	triggerinformer "knative.dev/eventing/pkg/client/injection/informers/eventing/v1/trigger"
	triggerreconciler "knative.dev/eventing/pkg/client/injection/reconciler/eventing/v1/trigger"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
)

const (
	NamespacedControllerAgentName = "kafka-namespaced-trigger-controller"

	NamespacedFinalizerName = "kafka.namespaced.triggers.eventing.knative.dev"
)

func NewNamespacedController(ctx context.Context, watcher configmap.Watcher, configs *config.Env) *controller.Impl {

	logger := logging.FromContext(ctx).Desugar()

	configmapInformer := configmapinformer.Get(ctx)
	brokerInformer := brokerinformer.Get(ctx)
	triggerInformer := triggerinformer.Get(ctx)
	triggerLister := triggerInformer.Lister()
	oidcServiceaccountInformer := serviceaccountinformer.Get(ctx, auth.OIDCLabelSelector)

	reconciler := &NamespacedReconciler{
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
		BrokerLister:         brokerInformer.Lister(),
		ConfigMapLister:      configmapInformer.Lister(),
		ServiceAccountLister: oidcServiceaccountInformer.Lister(),
		EventingClient:       eventingclient.Get(ctx),
		Env:                  configs,
		InitOffsetsFunc:      offset.InitOffsets,
		KafkaFeatureFlags:    apisconfig.DefaultFeaturesConfig(),
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
			FinalizerName:     NamespacedFinalizerName,
			AgentName:         NamespacedControllerAgentName,
			SkipStatusUpdates: false,
			PromoteFilterFunc: filterTriggers(reconciler.BrokerLister, kafka.NamespacedBrokerClass, NamespacedFinalizerName),
		}
	})

	setupFeatureStore(ctx, watcher, reconciler.FlagsHolder, impl, triggerInformer)

	reconciler.Resolver = resolver.NewURIResolverFromTracker(ctx, impl.Tracker)

	triggerInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: filterTriggers(reconciler.BrokerLister, kafka.NamespacedBrokerClass, NamespacedFinalizerName),
		Handler:    controller.HandleAll(impl.Enqueue),
	})

	// Filter Brokers and enqueue associated Triggers
	brokerInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: kafka.NamespacedBrokerClassFilter(),
		Handler:    enqueueTriggers(logger, triggerLister, impl.Enqueue),
	})

	globalResync := func(_ interface{}) {
		impl.FilteredGlobalResync(filterTriggers(reconciler.BrokerLister, kafka.NamespacedBrokerClass, NamespacedFinalizerName), triggerInformer.Informer())
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
		FilterFunc: kafka.FilterWithLabel(kafka.NamespacedBrokerDataplaneLabelKey, kafka.NamespacedBrokerDataplaneLabelValue),
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				globalResync(obj)
			},
			DeleteFunc: func(obj interface{}) {
				globalResync(obj)
			},
		},
	})

	reconciler.Tracker = impl.Tracker
	secretinformer.Get(ctx).Informer().AddEventHandler(controller.HandleAll(reconciler.Tracker.OnChanged))

	// Reconciler Trigger when the OIDC service account changes
	oidcServiceaccountInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: filterOIDCServiceAccounts(triggerInformer.Lister(), brokerInformer.Lister(), kafka.NamespacedBrokerClass, FinalizerName),
		Handler:    controller.HandleAll(impl.EnqueueControllerOf),
	})

	return impl
}
