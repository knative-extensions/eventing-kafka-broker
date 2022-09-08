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

package v1

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
)

func TestValidate(t *testing.T) {
	tests := []struct {
		name string
		b    BrokerStub
		want *apis.FieldError
	}{{
		name: "missing annotation",
		b:    BrokerStub{},
	}, {
		name: "empty annotation",
		b: BrokerStub{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{"eventing.knative.dev/broker.class": ""},
			},
		},
	}, {
		name: "other broker class",
		b: BrokerStub{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{"eventing.knative.dev/broker.class": "Foo"},
			},
		},
	}, {
		name: "no spec.config",
		b: BrokerStub{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{"eventing.knative.dev/broker.class": "Kafka"},
			},
		},
	}, {
		name: "no spec.config.namespace",
		b: BrokerStub{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{"eventing.knative.dev/broker.class": "Kafka"},
			},
			Spec: eventingv1.BrokerSpec{
				Config: &duckv1.KReference{
					Kind: "ConfigMap",
				},
			},
		},
		want: apis.ErrMissingField("", "namespace").ViaField("config").ViaField("spec"),
	}, {
		name: "spec.config is not configmap",
		b: BrokerStub{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{"eventing.knative.dev/broker.class": "Kafka"},
			},
			Spec: eventingv1.BrokerSpec{
				Config: &duckv1.KReference{
					Kind:      "Service",
					Namespace: "foo",
				},
			},
		},
		want: apis.ErrInvalidValue("Service", "kind", "Expected ConfigMap").ViaField("config").ViaField("spec"),
		// TODO: uncomment the following check when we backport namespaced broker into 1.6
		//}, {
		//	name: "spec.config.namespace is different",
		//	b: BrokerStub{
		//		ObjectMeta: metav1.ObjectMeta{
		//			Namespace:   "my-namespace",
		//			Annotations: map[string]string{"eventing.knative.dev/broker.class": "KafkaNamespaced"},
		//		},
		//		Spec: eventingv1.BrokerSpec{
		//			Config: &duckv1.KReference{
		//				Kind:      "ConfigMap",
		//				Namespace: "foo",
		//			},
		//		},
		//	},
		//	want: apis.ErrInvalidValue("foo", "namespace", "Expected ConfigMap in same namespace with broker resource").ViaField("config").ViaField("spec"),
		// TODO: uncomment the following check when we backport namespaced broker into 1.6
		//}, {
		//	name: "valid config - namespaced broker",
		//	b: BrokerStub{
		//		ObjectMeta: metav1.ObjectMeta{
		//			Namespace:   "my-namespace",
		//			Annotations: map[string]string{"eventing.knative.dev/broker.class": "KafkaNamespaced"},
		//		},
		//		Spec: eventingv1.BrokerSpec{
		//			Config: &duckv1.KReference{
		//				Namespace:  "my-namespace",
		//				Name:       "name",
		//				Kind:       "ConfigMap",
		//				APIVersion: "v1",
		//			},
		//		},
		//	},
	}, {
		name: "valid config - regular broker",
		b: BrokerStub{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   "my-namespace",
				Annotations: map[string]string{"eventing.knative.dev/broker.class": "Kafka"},
			},
			Spec: eventingv1.BrokerSpec{
				Config: &duckv1.KReference{
					Name:       "name",
					Namespace:  "my-namespace",
					Kind:       "ConfigMap",
					APIVersion: "v1",
				},
			},
		},
	}}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := test.b.Validate(context.Background())
			if diff := cmp.Diff(test.want.Error(), got.Error()); diff != "" {
				t.Error("Broker.Validate (-want, +got) =", diff)
			}
		})
	}
}
