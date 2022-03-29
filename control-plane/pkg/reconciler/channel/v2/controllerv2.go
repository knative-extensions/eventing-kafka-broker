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
	"net/http"

	"github.com/Shopify/sarama"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
	"knative.dev/pkg/resolver"

	messagingv1beta "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	kafkachannelinformer "knative.dev/eventing-kafka/pkg/client/injection/informers/messaging/v1beta1/kafkachannel"
	kafkachannelreconciler "knative.dev/eventing-kafka/pkg/client/injection/reconciler/messaging/v1beta1/kafkachannel"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/network"

	configmapinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/configmap"
	podinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/pod"
	secretinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/secret"

	consumergroupclient "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/client"
	consumergroupinformer "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/informers/eventing/v1alpha1/consumergroup"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/prober"

	"knative.dev/pkg/controller"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/consumergroup"
)

func NewController(ctx context.Context, configs *config.Env) *controller.Impl {

	configmapInformer := configmapinformer.Get(ctx)
	channelInformer := kafkachannelinformer.Get(ctx)
	consumerGroupInformer := consumergroupinformer.Get(ctx)

	messagingv1beta.RegisterAlternateKafkaChannelConditionSet(conditionSet)

	reconciler := &Reconciler{
		Reconciler: &base.Reconciler{
			KubeClient:                  kubeclient.Get(ctx),
			PodLister:                   podinformer.Get(ctx).Lister(),
			SecretLister:                secretinformer.Get(ctx).Lister(),
			DataPlaneConfigMapNamespace: configs.DataPlaneConfigMapNamespace,
			DataPlaneConfigMapName:      configs.DataPlaneConfigMapName,
			DataPlaneConfigFormat:       configs.DataPlaneConfigFormat,
			SystemNamespace:             configs.SystemNamespace,
		},
		NewKafkaClient:             sarama.NewClient,
		NewKafkaClusterAdminClient: sarama.NewClusterAdmin,
		Env:                        configs,
		ConfigMapLister:            configmapInformer.Lister(),
		ConsumerGroupLister:        consumerGroupInformer.Lister(),
		InternalsClient:            consumergroupclient.Get(ctx),
	}

	impl := kafkachannelreconciler.NewImpl(ctx, reconciler)
	reconciler.Prober = prober.NewAsync(ctx, http.DefaultClient, configs.IngressPodPort, reconciler.ReceiverSelector(), impl.EnqueueKey)
	reconciler.IngressHost = network.GetServiceHostname(configs.IngressName, configs.SystemNamespace)
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
	channelInformer.Informer().AddEventHandler(controller.HandleAll(impl.Enqueue))

	// ConsumerGroup changes and enqueue associated channel
	consumerGroupInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: consumergroup.Filter("kafkachannel"),
		Handler:    controller.HandleAll(consumergroup.Enqueue("kafkachannel", impl.EnqueueKey)),
	})

	return impl
}
