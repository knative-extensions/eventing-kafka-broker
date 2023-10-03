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

	"github.com/IBM/sarama"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka/offset"

	"k8s.io/client-go/tools/cache"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	configmapinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/configmap"
	podinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/pod"
	secretinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/secret"
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
		BrokerLister:               brokerInformer.Lister(),
		ConfigMapLister:            configmapInformer.Lister(),
		EventingClient:             eventingclient.Get(ctx),
		Env:                        configs,
		NewKafkaClient:             sarama.NewClient,
		NewKafkaClusterAdminClient: sarama.NewClusterAdmin,
		InitOffsetsFunc:            offset.InitOffsets,
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
		impl.GlobalResync(brokerInformer.Informer())
	}

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

	return impl
}
