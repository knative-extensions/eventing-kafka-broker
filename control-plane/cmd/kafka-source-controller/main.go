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

package main

import (
	"context"

	"knative.dev/pkg/injection"
	"knative.dev/pkg/signals"

	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/injection/sharedmain"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/consumer"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/consumergroup"
	sourcev2 "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/source/v2"
)

const (
	component = "kafka-source-controller"
)

func main() {

	sharedmain.MainNamed(signals.NewContext(), component,

		// KafkaSource controller
		injection.NamedControllerConstructor{
			Name: "source-controller",
			ControllerConstructor: func(ctx context.Context, watcher configmap.Watcher) *controller.Impl {
				return sourcev2.NewController(ctx)
			},
		},

		// ConsumerGroup controller
		injection.NamedControllerConstructor{
			Name: "consumergroup-controller",
			ControllerConstructor: func(ctx context.Context, watcher configmap.Watcher) *controller.Impl {
				return consumergroup.NewController(ctx)
			},
		},

		// Consumer controller
		injection.NamedControllerConstructor{
			Name: "consumer-controller",
			ControllerConstructor: func(ctx context.Context, watcher configmap.Watcher) *controller.Impl {
				return consumer.NewController(ctx)
			},
		},
	)
}
