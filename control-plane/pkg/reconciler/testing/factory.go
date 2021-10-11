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
	"encoding/json"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"
	fakeeventingclient "knative.dev/eventing/pkg/client/injection/client/fake"
	"knative.dev/pkg/client/injection/ducks/duck/v1/addressable"
	fakekubeclient "knative.dev/pkg/client/injection/kube/client/fake"
	pkgcontroller "knative.dev/pkg/controller"
	fakedynamicclient "knative.dev/pkg/injection/clients/dynamicclient/fake"
	"knative.dev/pkg/reconciler"
	. "knative.dev/pkg/reconciler/testing"

	fakeeventingkafkakafkaclient "knative.dev/eventing-kafka/pkg/client/injection/client/fake"

	fakeeventingkafkabrokerclient "knative.dev/eventing-kafka-broker/control-plane/pkg/client/injection/client/fake"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/broker"
)

const (
	// recorderBufferSize is the estimated max number of event notifications that
	// can be buffered during reconciliation.
	recorderBufferSize = 20
)

var DefaultConfigs = &broker.Configs{

	Env: config.Env{
		DataPlaneConfigMapNamespace: "knative-eventing",
		DataPlaneConfigMapName:      "kafka-broker-brokers-triggers",
		IngressName:                 "kafka-broker-receiver",
		SystemNamespace:             "knative-eventing",
		DataPlaneConfigFormat:       base.Json,
		DefaultBackoffDelayMs:       1000,
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
		ctx, sourcesKafkaClient := fakeeventingkafkakafkaclient.With(ctx, listers.GetEventingKafkaObjects()...)
		ctx, eventingKafkaClient := fakeeventingkafkabrokerclient.With(ctx, listers.GetEventingKafkaBrokerObjects()...)
		ctx, kubeClient := fakekubeclient.With(ctx, listers.GetKubeObjects()...)

		ctx, dynamicClient := fakedynamicclient.With(ctx,
			newScheme(),
			ToUnstructured(t, row.Objects)...,
		)

		dynamicScheme := runtime.NewScheme()
		for _, addTo := range clientSetSchemes {
			_ = addTo(dynamicScheme)
		}

		// The dynamic client's support for patching is BS.  Implement it
		// here via PrependReactor (this can be overridden below by the
		// provided reactors).
		dynamicClient.PrependReactor("patch", "*", func(action ktesting.Action) (bool, runtime.Object, error) {
			return true, nil, nil
		})

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
			eventingKafkaClient.PrependReactor("*", "*", reactor)
			sourcesKafkaClient.PrependReactor("*", "*", reactor)
		}

		actionRecorderList := ActionRecorderList{
			dynamicClient,
			kubeClient,
			eventingClient,
			eventingKafkaClient,
			sourcesKafkaClient,
		}

		eventList := EventList{
			Recorder: eventRecorder,
		}

		return controller, actionRecorderList, eventList
	}
}

// ToUnstructured takes a list of k8s resources and converts them to
// Unstructured objects.
// We must pass objects as Unstructured to the dynamic client fake, or it
// won't handle them properly.
func ToUnstructured(t *testing.T, objs []runtime.Object) (us []runtime.Object) {
	sch := newScheme()
	for _, obj := range objs {
		obj = obj.DeepCopyObject() // Don't mess with the primary copy
		// Determine and set the TypeMeta for this object based on our test scheme.
		gvks, _, err := sch.ObjectKinds(obj)
		if err != nil {
			t.Fatalf("Unable to determine kind for type: %v", err)
		}
		apiv, k := gvks[0].ToAPIVersionAndKind()
		ta, err := meta.TypeAccessor(obj)
		if err != nil {
			t.Fatalf("Unable to create type accessor: %v", err)
		}
		ta.SetAPIVersion(apiv)
		ta.SetKind(k)

		b, err := json.Marshal(obj)
		if err != nil {
			t.Fatalf("Unable to marshal: %v", err)
		}
		u := &unstructured.Unstructured{}
		if err := json.Unmarshal(b, u); err != nil {
			t.Fatalf("Unable to unmarshal: %v", err)
		}
		us = append(us, u)
	}
	return
}
