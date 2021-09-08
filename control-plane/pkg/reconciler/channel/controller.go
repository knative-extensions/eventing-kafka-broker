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

package channel

import (
	"context"

	"github.com/Shopify/sarama"
	corev1 "k8s.io/api/core/v1"

	kafkachannelreconciler "knative.dev/eventing-kafka/pkg/client/injection/reconciler/messaging/v1beta1/kafkachannel"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	configmapinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/configmap"
	podinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/pod"
	secretinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/secret"

	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/resolver"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
)

func NewController(ctx context.Context, watcher configmap.Watcher, configs *config.Env) *controller.Impl {

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
			DispatcherLabel:             base.ChannelDispatcherLabel,
			ReceiverLabel:               base.ChannelReceiverLabel,
		},
		ClusterAdmin:    sarama.NewClusterAdmin,
		Configs:         configs,
		ConfigMapLister: configmapInformer.Lister(),
	}

	impl := kafkachannelreconciler.NewImpl(ctx, reconciler)

	reconciler.Resolver = resolver.NewURIResolverFromTracker(ctx, impl.Tracker)

	reconciler.SecretTracker = impl.Tracker
	secretinformer.Get(ctx).Informer().AddEventHandler(controller.HandleAll(reconciler.SecretTracker.OnChanged))

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

	return impl
}
