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

package testing

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	fakeeventingclient "knative.dev/eventing/pkg/client/injection/client/fake"
	"knative.dev/pkg/client/injection/ducks/duck/v1/addressable"
	fakekubeclient "knative.dev/pkg/client/injection/kube/client/fake"
	pkgcontroller "knative.dev/pkg/controller"
	fakedynamicclient "knative.dev/pkg/injection/clients/dynamicclient/fake"
	"knative.dev/pkg/reconciler"
	. "knative.dev/pkg/reconciler/testing"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/broker"
)

const (
	// recorderBufferSize is the estimated max number of event notifications that
	// can be buffered during reconciliation.
	recorderBufferSize = 20
)

var DefaultConfigs = &broker.Configs{

	EnvConfigs: broker.EnvConfigs{
		DataPlaneConfigMapNamespace: "kafka-broker-brokers-triggers",
		DataPlaneConfigMapName:      "knative-eventing",
		BrokerIngressName:           "kafka-broker-receiver",
		SystemNamespace:             "knative-eventing",
		DataPlaneConfigFormat:       base.Json,
	},

	BootstrapServers: "",
}

// Ctor functions create a k8s controller with given params.
type Ctor func(ctx context.Context, listers *Listers, configs *broker.Configs, row *TableRow) pkgcontroller.Reconciler

func NewFactory(configs *broker.Configs, ctor Ctor) Factory {
	return func(t *testing.T, row *TableRow) (pkgcontroller.Reconciler, ActionRecorderList, EventList) {

		listers := newListers(row.Objects)
		ctx := context.Background()

		ctx, eventingClient := fakeeventingclient.With(ctx, listers.GetEventingObjects()...)
		ctx, kubeClient := fakekubeclient.With(
			ctx,
			listers.GetKubeObjects()...,
		)

		dynamicScheme := runtime.NewScheme()
		for _, addTo := range clientSetSchemes {
			_ = addTo(dynamicScheme)
		}

		ctx, dynamicClient := fakedynamicclient.With(
			ctx,
			dynamicScheme,
			listers.GetAllObjects()...,
		)

		eventRecorder := record.NewFakeRecorder(recorderBufferSize)
		ctx = pkgcontroller.WithEventRecorder(ctx, eventRecorder)

		ctx = addressable.WithDuck(ctx)

		controller := ctor(ctx, listers, configs, row)

		if la, ok := controller.(reconciler.LeaderAware); ok {
			_ = la.Promote(reconciler.UniversalBucket(), func(reconciler.Bucket, types.NamespacedName) {})
		}

		for _, reactor := range row.WithReactors {
			kubeClient.PrependReactor("*", "*", reactor)
			dynamicClient.PrependReactor("*", "*", reactor)
			eventingClient.PrependReactor("*", "*", reactor)
		}

		actionRecorderList := ActionRecorderList{
			dynamicClient,
			kubeClient,
		}

		eventList := EventList{
			Recorder: eventRecorder,
		}

		return controller, actionRecorderList, eventList
	}
}
