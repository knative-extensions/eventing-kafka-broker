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

package broker

import (
	"context"
	"net/http"

	"github.com/Shopify/sarama"
	mfclient "github.com/manifestival/client-go-client"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"

	eventing "knative.dev/eventing/pkg/apis/eventing/v1"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/prober"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/injection"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/resolver"

	kubeclient "knative.dev/pkg/client/injection/kube/client"

	brokerinformer "knative.dev/eventing/pkg/client/injection/informers/eventing/v1/broker"
	brokerreconciler "knative.dev/eventing/pkg/client/injection/reconciler/eventing/v1/broker"
	deploymentinformer "knative.dev/pkg/client/injection/kube/informers/apps/v1/deployment"
	configmapinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/configmap"
	podinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/pod"
	secretinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/secret"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
)

func NewNamespacedController(ctx context.Context, watcher configmap.Watcher, env *config.Env) *controller.Impl {
	logger := logging.FromContext(ctx)

	eventing.RegisterAlternateBrokerConditionSet(base.IngressConditionSet)

	configmapInformer := configmapinformer.Get(ctx)

	cfg := injection.GetConfig(ctx)
	mfc, err := mfclient.NewClient(cfg)
	if err != nil {
		logger.Fatal("unable to create Manifestival client-go client", zap.Error(err))
	}

	reconciler := &NamespacedReconciler{
		Reconciler: &base.Reconciler{
			KubeClient:                  kubeclient.Get(ctx),
			PodLister:                   podinformer.Get(ctx).Lister(),
			SecretLister:                secretinformer.Get(ctx).Lister(),
			DataPlaneConfigMapNamespace: env.DataPlaneConfigMapNamespace,
			DataPlaneConfigMapName:      env.DataPlaneConfigMapName,
			DataPlaneConfigFormat:       env.DataPlaneConfigFormat,
			DataPlaneNamespace:          env.SystemNamespace,
			DispatcherLabel:             base.BrokerDispatcherLabel,
			ReceiverLabel:               base.BrokerReceiverLabel,
		},
		NewKafkaClusterAdminClient: sarama.NewClusterAdmin,
		ConfigMapLister:            configmapInformer.Lister(),
		Env:                        env,
		ManifestivalClient:         mfc,
	}

	impl := brokerreconciler.NewImpl(ctx, reconciler, kafka.NamespacedBrokerClass, func(impl *controller.Impl) controller.Options {
		return controller.Options{PromoteFilterFunc: kafka.NamespacedBrokerClassFilter()}
	})

	reconciler.Resolver = resolver.NewURIResolverFromTracker(ctx, impl.Tracker)
	reconciler.IPsLister = prober.NewIPListerWithMapping()
	reconciler.Prober = prober.NewAsync(ctx, http.DefaultClient, env.IngressPodPort, reconciler.IPsLister.List, impl.EnqueueKey)

	brokerInformer := brokerinformer.Get(ctx)

	brokerInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: kafka.NamespacedBrokerClassFilter(),
		Handler:    controller.HandleAll(impl.Enqueue),
	})

	reconciler.SecretTracker = impl.Tracker
	secretinformer.Get(ctx).Informer().AddEventHandler(controller.HandleAll(reconciler.SecretTracker.OnChanged))

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

	deploymentinformer.Get(ctx).Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: kafka.FilterAny(
			kafka.FilterWithLabel("app", "kafka-broker-dispatcher"),
			kafka.FilterWithLabel("app", "kafka-broker-receiver"),
		),
		Handler: controller.HandleAll(globalResync),
	})

	reconciler.ConfigMapTracker = impl.Tracker
	configmapinformer.Get(ctx).Informer().AddEventHandler(controller.HandleAll(
		// Call the tracker's OnChanged method, but we've seen the objects
		// coming through this path missing TypeMeta, so ensure it is properly
		// populated.
		controller.EnsureTypeMeta(
			reconciler.ConfigMapTracker.OnChanged,
			corev1.SchemeGroupVersion.WithKind("ConfigMap"),
		),
	))

	brokerInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: kafka.NamespacedBrokerClassFilter(),
		Handler: cache.ResourceEventHandlerFuncs{
			DeleteFunc: reconciler.OnDeleteObserver,
		},
	})

	return impl
}
