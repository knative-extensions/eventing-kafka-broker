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

package main

import (
	"context"
	"log"

	filteredFactory "knative.dev/pkg/client/injection/kube/informers/factory/filtered"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/injection"
	"knative.dev/pkg/injection/sharedmain"
	"knative.dev/pkg/signals"

	"knative.dev/eventing/pkg/auth"
	"knative.dev/eventing/pkg/eventingtls"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka/clientpool"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/broker"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/channel"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/consumer"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/consumergroup"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/sink"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/source"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/trigger"
	triggerv2 "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/trigger/v2"
)

const (
	component = "kafka-broker-controller"
)

func main() {

	brokerEnv, err := config.GetEnvConfig("BROKER", broker.ValidateDefaultBackoffDelayMs)
	if err != nil {
		log.Fatal("cannot process environment variables with prefix BROKER", err)
	}

	channelEnv, err := config.GetEnvConfig("CHANNEL")
	if err != nil {
		log.Fatal("cannot process environment variables with prefix CHANNEL", err)
	}

	sinkEnv, err := config.GetEnvConfig("SINK")
	if err != nil {
		log.Fatal("cannot process environment variables with prefix SINK", err)
	}

	ctx := signals.NewContext()
	ctx = filteredFactory.WithSelectors(ctx,
		eventingtls.TrustBundleLabelSelector,
		auth.OIDCLabelSelector,
		eventing.DispatcherLabelSelectorStr,
	)
	ctx = clientpool.WithKafkaClientPool(ctx)

	sharedmain.MainNamed(ctx, component,

		// Broker controller
		injection.NamedControllerConstructor{
			Name: "broker-controller",
			ControllerConstructor: func(ctx context.Context, watcher configmap.Watcher) *controller.Impl {
				return broker.NewController(ctx, watcher, brokerEnv)
			},
		},

		// Trigger controller
		injection.NamedControllerConstructor{
			Name: "trigger-controller",
			ControllerConstructor: func(ctx context.Context, watcher configmap.Watcher) *controller.Impl {
				return triggerv2.NewController(ctx, watcher, brokerEnv)
			},
		},

		// Namespaced broker controller
		injection.NamedControllerConstructor{
			Name: "namespaced-broker-controller",
			ControllerConstructor: func(ctx context.Context, watcher configmap.Watcher) *controller.Impl {
				return broker.NewNamespacedController(ctx, watcher, brokerEnv)
			},
		},

		// Namespaced trigger controller
		injection.NamedControllerConstructor{
			Name: "namespaced-trigger-controller",
			ControllerConstructor: func(ctx context.Context, watcher configmap.Watcher) *controller.Impl {
				return trigger.NewNamespacedController(ctx, watcher, brokerEnv)
			},
		},

		// Channel controller
		injection.NamedControllerConstructor{
			Name: "channel-controller",
			ControllerConstructor: func(ctx context.Context, watcher configmap.Watcher) *controller.Impl {
				return channel.NewController(ctx, watcher, channelEnv)
			},
		},

		// KafkaSink controller
		injection.NamedControllerConstructor{
			Name: "sink-controller",
			ControllerConstructor: func(ctx context.Context, watcher configmap.Watcher) *controller.Impl {
				return sink.NewController(ctx, watcher, sinkEnv)
			},
		},

		// KafkaSource controller
		injection.NamedControllerConstructor{
			Name: "source-controller",
			ControllerConstructor: func(ctx context.Context, watcher configmap.Watcher) *controller.Impl {
				return source.NewController(ctx, watcher)
			},
		},

		// ConsumerGroup controller
		injection.NamedControllerConstructor{
			Name: "consumergroup-controller",
			ControllerConstructor: func(ctx context.Context, watcher configmap.Watcher) *controller.Impl {
				return consumergroup.NewController(ctx, watcher)
			},
		},

		// Consumer controller
		injection.NamedControllerConstructor{
			Name: "consumer-controller",
			ControllerConstructor: func(ctx context.Context, watcher configmap.Watcher) *controller.Impl {
				return consumer.NewController(ctx, watcher)
			},
		},
	)
}
