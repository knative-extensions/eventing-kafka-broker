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

package sink

import (
	"context"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	"k8s.io/client-go/tools/cache"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	configmapinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/configmap"
	podinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/pod"
	secretinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/secret"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/tracker"

	eventing "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/eventing/v1alpha1"
	sinkinformer "knative.dev/eventing-kafka-broker/control-plane/pkg/client/injection/informers/eventing/v1alpha1/kafkasink"
	sinkreconciler "knative.dev/eventing-kafka-broker/control-plane/pkg/client/injection/reconciler/eventing/v1alpha1/kafkasink"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
)

func NewController(ctx context.Context, _ configmap.Watcher, configs *config.Env) *controller.Impl {

	eventing.RegisterConditionSet(base.ConditionSet)

	logger := logging.FromContext(ctx)

	configmapInformer := configmapinformer.Get(ctx)

	reconciler := &Reconciler{
		Reconciler: &base.Reconciler{
			KubeClient:                  kubeclient.Get(ctx),
			PodLister:                   podinformer.Get(ctx).Lister(),
			SecretLister:                secretinformer.Get(ctx).Lister(),
			DataPlaneConfigMapNamespace: configs.DataPlaneConfigMapNamespace,
			DataPlaneConfigMapName:      configs.DataPlaneConfigMapName,
			DataPlaneConfigFormat:       configs.DataPlaneConfigFormat,
			SystemNamespace:             configs.SystemNamespace,
			ReceiverLabel:               base.SinkReceiverLabel,
		},
		ConfigMapLister: configmapInformer.Lister(),
		ClusterAdmin:    sarama.NewClusterAdmin,
		Configs:         configs,
	}

	_, err := reconciler.GetOrCreateDataPlaneConfigMap(ctx)
	if err != nil {
		logger.Fatal("Failed to get or create data plane config map",
			zap.String("configmap", configs.DataPlaneConfigMapAsString()),
			zap.Error(err),
		)
	}

	impl := sinkreconciler.NewImpl(ctx, reconciler)

	sinkInformer := sinkinformer.Get(ctx)

	sinkInformer.Informer().AddEventHandler(controller.HandleAll(impl.Enqueue))

	globalResync := func(_ interface{}) {
		impl.GlobalResync(sinkInformer.Informer())
	}

	configmapInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: controller.FilterWithNameAndNamespace(configs.DataPlaneConfigMapNamespace, configs.DataPlaneConfigMapName),
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				globalResync(obj)
			},
			DeleteFunc: func(obj interface{}) {
				globalResync(obj)
			},
		},
	})

	reconciler.SecretTracker = tracker.New(impl.EnqueueKey, controller.GetTrackerLease(ctx))
	secretinformer.Get(ctx).Informer().AddEventHandler(controller.HandleAll(reconciler.SecretTracker.OnChanged))

	sinkInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		DeleteFunc: reconciler.OnDeleteObserver,
	})

	return impl
}
