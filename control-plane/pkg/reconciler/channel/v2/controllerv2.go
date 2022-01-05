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

	"k8s.io/client-go/tools/cache"

	messagingv1beta "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	kafkachannelinformer "knative.dev/eventing-kafka/pkg/client/injection/informers/messaging/v1beta1/kafkachannel"
	kafkachannelreconciler "knative.dev/eventing-kafka/pkg/client/injection/reconciler/messaging/v1beta1/kafkachannel"

	consumergroupclient "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/client"
	consumergroupinformer "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/informers/eventing/v1alpha1/consumergroup"

	"knative.dev/pkg/controller"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/consumergroup"
)

func NewController(ctx context.Context, configs *config.Env) *controller.Impl {

	channelInformer := kafkachannelinformer.Get(ctx)
	consumerGroupInformer := consumergroupinformer.Get(ctx)

	messagingv1beta.RegisterAlternateKafkaChannelConditionSet(conditionSet)

	reconciler := &Reconciler{
		ConsumerGroupLister: consumerGroupInformer.Lister(),
		InternalsClient:     consumergroupclient.Get(ctx),
	}

	impl := kafkachannelreconciler.NewImpl(ctx, reconciler)

	channelInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		Handler: controller.HandleAll(impl.Enqueue),
	})

	// ConsumerGroup changes and enqueue associated channel
	consumerGroupInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: consumergroup.Filter("kafkachannel"),
		Handler:    controller.HandleAll(impl.Enqueue),
	})

	return impl
}
