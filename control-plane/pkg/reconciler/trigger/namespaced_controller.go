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

	"k8s.io/client-go/tools/cache"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	podinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/pod"
	secretinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/secret"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/resolver"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	"knative.dev/eventing/pkg/apis/feature"
	eventingclient "knative.dev/eventing/pkg/client/injection/client"
	brokerinformer "knative.dev/eventing/pkg/client/injection/informers/eventing/v1/broker"
	triggerinformer "knative.dev/eventing/pkg/client/injection/informers/eventing/v1/trigger"
	triggerreconciler "knative.dev/eventing/pkg/client/injection/reconciler/eventing/v1/trigger"
)

// TODO: COPY & PASTE for now

const (
	NamespacedControllerAgentName = "kafka-namespaced-trigger-controller"

	NamespacedFinalizerName = "kafka.namespaced.triggers.eventing.knative.dev"
)

func NewNamespacedController(ctx context.Context, watcher configmap.Watcher, configs *config.Env) *controller.Impl {

	logger := logging.FromContext(ctx).Desugar()

	//configmapInformer := configmapinformer.Get(ctx)
	brokerInformer := brokerinformer.Get(ctx)
	triggerInformer := triggerinformer.Get(ctx)
	triggerLister := triggerInformer.Lister()

	reconciler := &NamespacedReconciler{
		Reconciler: &base.Reconciler{
			KubeClient:                  kubeclient.Get(ctx),
			PodLister:                   podinformer.Get(ctx).Lister(),
			SecretLister:                secretinformer.Get(ctx).Lister(),
			DataPlaneConfigMapNamespace: configs.DataPlaneConfigMapNamespace,
			DataPlaneConfigMapName:      configs.DataPlaneConfigMapName,
			DataPlaneConfigFormat:       configs.DataPlaneConfigFormat,
			DataPlaneNamespace:          configs.SystemNamespace,
			DispatcherLabel:             base.BrokerDispatcherLabel,
			ReceiverLabel:               base.BrokerReceiverLabel,
		},
		BrokerLister:   brokerInformer.Lister(),
		EventingClient: eventingclient.Get(ctx),
		Env:            configs,
		Flags:          feature.Flags{},
	}

	impl := triggerreconciler.NewImpl(ctx, reconciler, func(impl *controller.Impl) controller.Options {
		return controller.Options{
			FinalizerName:     NamespacedFinalizerName,
			AgentName:         NamespacedControllerAgentName,
			SkipStatusUpdates: false,
			PromoteFilterFunc: filterTriggers(reconciler.BrokerLister, kafka.NamespacedBrokerClass, NamespacedFinalizerName),
		}
	})

	featureStore := feature.NewStore(
		logging.FromContext(ctx).Named("feature-config-eventing-store"),
		func(name string, value interface{}) {
			flags, ok := value.(feature.Flags)
			if !ok {
				logger.Warn("Features ConfigMap " + name + " updated but we didn't get expected flags. Skipping updating cached features")
			}
			logger.Debug("Features ConfigMap " + name + " updated. Updating cached features.")
			reconciler.FlagsLock.Lock()
			defer reconciler.FlagsLock.Unlock()
			reconciler.Flags = flags
			impl.GlobalResync(triggerInformer.Informer())
		},
	)
	featureStore.WatchConfigs(watcher)

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

	//globalResync := func(_ interface{}) {
	//	impl.GlobalResync(triggerInformer.Informer())
	//}

	// TODO: we cannot have this watch since we don't know the trigger namespace yet.
	// TODO: Solution: we need to set this watch more generically by adding a label to the contract configmap
	//configmapInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
	//	FilterFunc: controller.FilterWithNameAndNamespace(configs.DataPlaneConfigMapNamespace, configs.DataPlaneConfigMapName),
	//	Handler: cache.ResourceEventHandlerFuncs{
	//		AddFunc:    globalResync,
	//		DeleteFunc: globalResync,
	//	},
	//})

	return impl
}
