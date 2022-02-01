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

	"knative.dev/pkg/injection"
	"knative.dev/pkg/signals"

	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/injection/sharedmain"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/broker"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/channel"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/consumer"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/consumergroup"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/sink"
	sourcev2 "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/source/v2"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/trigger"
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

	sourceEnv, err := config.GetEnvConfig("SOURCE")
	if err != nil {
		log.Fatal("cannot process environment variables with prefix SINK", err)
	}

	sharedmain.MainNamed(signals.NewContext(), component,

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
				return trigger.NewController(ctx, watcher, brokerEnv)
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
				return sourcev2.NewController(ctx, sourceEnv)
			},
		},

		injection.NamedControllerConstructor{
			Name: "consumergroup-controller",
			ControllerConstructor: func(ctx context.Context, watcher configmap.Watcher) *controller.Impl {
				return consumergroup.NewController(ctx)
			},
		},
		injection.NamedControllerConstructor{
			Name: "consumer-controller",
			ControllerConstructor: func(ctx context.Context, watcher configmap.Watcher) *controller.Impl {
				// TODO(pierDipi) pass consumer controller specific config
				return consumer.NewController(ctx, brokerEnv)
			},
		},
	)
}
