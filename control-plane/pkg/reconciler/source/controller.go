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

package source

import (
	"context"

	"k8s.io/client-go/tools/cache"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/apis/config"
	consumergroupclient "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/client"
	consumergroupinformer "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/informers/eventing/v1alpha1/consumergroup"

	sources "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/sources/v1beta1"

	kafkainformer "knative.dev/eventing-kafka-broker/control-plane/pkg/client/injection/informers/sources/v1beta1/kafkasource"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/client/injection/reconciler/sources/v1beta1/kafkasource"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/consumergroup"
	kedaclient "knative.dev/eventing-kafka-broker/third_party/pkg/client/injection/client"
)

func NewController(ctx context.Context, watcher configmap.Watcher) *controller.Impl {

	kafkaInformer := kafkainformer.Get(ctx)
	consumerGroupInformer := consumergroupinformer.Get(ctx)

	sources.RegisterAlternateKafkaConditionSet(conditionSet)

	r := &Reconciler{
		ConsumerGroupLister: consumerGroupInformer.Lister(),
		InternalsClient:     consumergroupclient.Get(ctx),
		KedaClient:          kedaclient.Get(ctx),
		KafkaFeatureFlags:   config.DefaultFeaturesConfig(),
	}

	impl := kafkasource.NewImpl(ctx, r)

	kafkaInformer.Informer().AddEventHandler(controller.HandleAll(impl.Enqueue))

	configStore := config.NewStore(ctx, func(name string, value *config.KafkaFeatureFlags) {
		r.KafkaFeatureFlags.Reset(value)
		impl.GlobalResync(kafkaInformer.Informer())
	})
	configStore.WatchConfigs(watcher)

	// ConsumerGroup changes and enqueue associated KafkaSource
	consumerGroupInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: consumergroup.Filter("kafkasource"),
		Handler:    controller.HandleAll(consumergroup.Enqueue("kafkasource", impl.EnqueueKey)),
	})
	return impl
}
