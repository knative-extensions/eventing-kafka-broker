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
	rtesting "knative.dev/pkg/reconciler/testing"

	fakeeventingkafkabrokerclient "knative.dev/eventing-kafka-broker/control-plane/pkg/client/injection/client/fake"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"

	fakekafkainternals "knative.dev/eventing-kafka-broker/control-plane/pkg/client/injection/client/fake"
	fakekeda "knative.dev/eventing-kafka-broker/third_party/pkg/client/injection/client/fake"
)

const (
	// recorderBufferSize is the estimated max number of event notifications that
	// can be buffered during reconciliation.
	recorderBufferSize = 20
)

// Ctor functions create a k8s controller with given params.
type Ctor func(ctx context.Context, listers *Listers, env *config.Env, row *rtesting.TableRow) pkgcontroller.Reconciler

func NewFactory(env *config.Env, ctor Ctor) rtesting.Factory {
	return func(t *testing.T, row *rtesting.TableRow) (pkgcontroller.Reconciler, rtesting.ActionRecorderList, rtesting.EventList) {

		listers := newListers(row.Objects)
		var ctx context.Context
		if row.Ctx != nil {
			ctx = row.Ctx
		} else {
			ctx = context.Background()
		}

		ctx, eventingClient := fakeeventingclient.With(ctx, listers.GetEventingObjects()...)
		ctx, eventingKafkaBrokerClient := fakeeventingkafkabrokerclient.With(ctx, listers.GetEventingKafkaBrokerObjects()...)
		ctx, kubeClient := fakekubeclient.With(ctx, listers.GetKubeObjects()...)
		ctx, kafkaInternalsClient := fakekafkainternals.With(ctx, listers.GetKafkaInternalsObjects()...)
		ctx, kedaClient := fakekeda.With(ctx, []runtime.Object{}...)

		ctx, dynamicClient := fakedynamicclient.With(ctx,
			newScheme(),
			ToUnstructuredList(t, row.Objects)...,
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

		controller := ctor(ctx, listers, env, row)

		if la, ok := controller.(reconciler.LeaderAware); ok {
			_ = la.Promote(reconciler.UniversalBucket(), func(reconciler.Bucket, types.NamespacedName) {})
		}

		for _, reactor := range row.WithReactors {
			kubeClient.PrependReactor("*", "*", reactor)
			dynamicClient.PrependReactor("*", "*", reactor)
			eventingClient.PrependReactor("*", "*", reactor)
			eventingKafkaBrokerClient.PrependReactor("*", "*", reactor)
			kafkaInternalsClient.PrependReactor("*", "*", reactor)
			kedaClient.PrependReactor("*", "*", reactor)
		}

		actionRecorderList := rtesting.ActionRecorderList{
			dynamicClient,
			kubeClient,
			eventingClient,
			eventingKafkaBrokerClient,
			kafkaInternalsClient,
		}

		eventList := rtesting.EventList{
			Recorder: eventRecorder,
		}

		return controller, actionRecorderList, eventList
	}
}

// ToUnstructuredList takes a list of k8s resources and converts them to
// Unstructured objects.
// We must pass objects as Unstructured to the dynamic client fake, or it
// won't handle them properly.
func ToUnstructuredList(t *testing.T, objs []runtime.Object) (us []runtime.Object) {
	for _, obj := range objs {
		u := ToUnstructured(t, obj)
		us = append(us, u)
	}
	return
}

type UnstructuredMutator func(u *unstructured.Unstructured)

// ToUnstructured takes a single k8s resource and converts it to
// Unstructured object.
func ToUnstructured(t *testing.T, obj runtime.Object, mutators ...UnstructuredMutator) runtime.Object {
	sch := newScheme()
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
	for _, m := range mutators {
		m(u)
	}
	return u
}
