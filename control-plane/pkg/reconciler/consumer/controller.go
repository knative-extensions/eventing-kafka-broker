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

package consumer

import (
	"context"
	"fmt"

	"github.com/kelseyhightower/envconfig"
	"knative.dev/eventing/pkg/eventingtls"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	configmapinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/configmap/filtered"
	podinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/pod"
	secretinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/secret"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/resolver"
	"knative.dev/pkg/system"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/apis/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/informers/eventing/v1alpha1/consumer"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/informers/eventing/v1alpha1/consumergroup"
	creconciler "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/reconciler/eventing/v1alpha1/consumer"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
	cgreconciler "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/consumergroup"
)

type ControllerConfig struct {
	ContractConfigMapFormat string `required:"true" split_words:"true"`
}

func NewController(ctx context.Context, watcher configmap.Watcher) *controller.Impl {

	controllerConfig := &ControllerConfig{}
	if err := envconfig.Process("CONSUMER", controllerConfig); err != nil {
		panic(fmt.Errorf("failed to process env variables for consumer controller, prefix CONSUMER: %v", err))
	}

	consumerInformer := consumer.Get(ctx)
	trustBundleConfigMapInformer := configmapinformer.Get(ctx, eventingtls.TrustBundleLabelSelector)

	r := &Reconciler{
		SerDe:                      formatSerDeFromString(controllerConfig.ContractConfigMapFormat),
		ConsumerGroupLister:        consumergroup.Get(ctx).Lister(),
		SecretLister:               secretinformer.Get(ctx).Lister(),
		PodLister:                  podinformer.Get(ctx).Lister(),
		KubeClient:                 kubeclient.Get(ctx),
		KafkaFeatureFlags:          config.DefaultFeaturesConfig(),
		TrustBundleConfigMapLister: trustBundleConfigMapInformer.Lister().ConfigMaps(system.Namespace()),
	}

	impl := creconciler.NewImpl(ctx, r)

	configStore := config.NewStore(ctx, func(name string, value *config.KafkaFeatureFlags) {
		r.KafkaFeatureFlags.Reset(value)
		impl.GlobalResync(consumerInformer.Informer())
	})
	configStore.WatchConfigs(watcher)

	r.Resolver = resolver.NewURIResolverFromTracker(ctx, impl.Tracker)

	consumerInformer.Informer().AddEventHandler(controller.HandleAll(impl.Enqueue))

	r.Tracker = impl.Tracker
	secretinformer.Get(ctx).Informer().AddEventHandler(controller.HandleAll(r.Tracker.OnChanged))

	globalResync := func(interface{}) {
		impl.GlobalResync(consumerInformer.Informer())
	}

	cgreconciler.ResyncOnStatefulSetChange(ctx, globalResync)

	trustBundleConfigMapInformer.Informer().AddEventHandler(controller.HandleAll(globalResync))

	return impl
}

func formatSerDeFromString(val string) contract.FormatSerDe {
	switch val {
	case "protobuf":
		return contract.FormatSerDe{Format: contract.Protobuf}
	default:
		return contract.FormatSerDe{Format: contract.Json}
	}
}
