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

	kubeclient "knative.dev/pkg/client/injection/kube/client"
	podinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/pod"
	secretinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/secret"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/resolver"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/informers/eventing/v1alpha1/consumer"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/informers/eventing/v1alpha1/consumergroup"
	creconciler "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/reconciler/eventing/v1alpha1/consumer"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
	cgreconciler "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/consumergroup"
)

func NewController(ctx context.Context, configs *config.Env) *controller.Impl {

	consumerInformer := consumer.Get(ctx)

	r := &Reconciler{
		SerDe:               formatSerDeFromString(configs.DataPlaneConfigFormat),
		ConsumerGroupLister: consumergroup.Get(ctx).Lister(),
		SecretLister:        secretinformer.Get(ctx).Lister(),
		PodLister:           podinformer.Get(ctx).Lister(),
		KubeClient:          kubeclient.Get(ctx),
	}

	impl := creconciler.NewImpl(ctx, r)

	r.Resolver = resolver.NewURIResolverFromTracker(ctx, impl.Tracker)
	r.Tracker = impl.Tracker

	consumerInformer.Informer().AddEventHandler(controller.HandleAll(impl.Enqueue))

	secretinformer.Get(ctx).Informer().AddEventHandler(controller.HandleAll(r.Tracker.OnChanged))

	globalResync := func(interface{}) {
		impl.GlobalResync(consumerInformer.Informer())
	}

	cgreconciler.ResyncOnStatefulSetChange(ctx, globalResync)

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
