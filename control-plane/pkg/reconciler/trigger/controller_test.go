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

package trigger

import (
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	_ "knative.dev/pkg/client/injection/ducks/duck/v1/addressable/fake"
	_ "knative.dev/pkg/client/injection/kube/informers/core/v1/configmap/fake"
	_ "knative.dev/pkg/client/injection/kube/informers/core/v1/pod/fake"
	_ "knative.dev/pkg/client/injection/kube/informers/core/v1/secret/fake"
	"knative.dev/pkg/configmap"
	reconcilertesting "knative.dev/pkg/reconciler/testing"

	eventing "knative.dev/eventing/pkg/apis/eventing/v1"
	brokerinformer "knative.dev/eventing/pkg/client/injection/informers/eventing/v1/broker"
	_ "knative.dev/eventing/pkg/client/injection/informers/eventing/v1/broker/fake"
	_ "knative.dev/eventing/pkg/client/injection/informers/eventing/v1/trigger/fake"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
)

func TestNewController(t *testing.T) {
	ctx, _ := reconcilertesting.SetupFakeContext(t)

	controller := NewController(ctx, configmap.NewStaticWatcher(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: "config-features",
		},
	}), &config.Env{})
	if controller == nil {
		t.Error("failed to create controller: <nil>")
	}
}

func TestFilterTriggers(t *testing.T) {
	ctx, _ := reconcilertesting.SetupFakeContext(t)

	tt := []struct {
		name    string
		trigger interface{}
		pass    bool
		brokers []*eventing.Broker
	}{
		{
			name:    "unknown type",
			trigger: &eventing.Broker{},
			pass:    false,
		},
		{
			name: "non existing broker",
			trigger: &eventing.Trigger{
				Spec: eventing.TriggerSpec{
					Broker: "not-exists",
				},
			},
			pass: false,
		},
		{
			name: "non existing broker and trigger with kafka broker finalizer",
			trigger: &eventing.Trigger{
				ObjectMeta: metav1.ObjectMeta{
					Finalizers: []string{FinalizerName},
				},
				Spec: eventing.TriggerSpec{
					Broker: "not-exists",
				},
			},
			pass: true,
		},
		{
			name: "exiting kafka broker",
			trigger: &eventing.Trigger{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:  "ns",
					Name:       "tr",
					Finalizers: []string{FinalizerName},
				},
				Spec: eventing.TriggerSpec{
					Broker: "br",
				},
			},
			brokers: []*eventing.Broker{
				{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "ns",
						Name:      "br",
						Annotations: map[string]string{
							eventing.BrokerClassAnnotationKey: kafka.BrokerClass,
						},
					},
				},
			},
			pass: true,
		},
		{
			name: "exiting non kafka broker",
			trigger: &eventing.Trigger{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "ns",
					Name:      "tr",
				},
				Spec: eventing.TriggerSpec{
					Broker: "br",
				},
			},
			brokers: []*eventing.Broker{
				{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "ns",
						Name:      "br",
						Annotations: map[string]string{
							eventing.BrokerClassAnnotationKey: kafka.BrokerClass + "-boh",
						},
					},
				},
			},
			pass: false,
		},
		{
			name: "exiting non kafka broker - trigger with kafka broker finalizer",
			trigger: &eventing.Trigger{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:  "ns",
					Name:       "tr",
					Finalizers: []string{FinalizerName},
				},
				Spec: eventing.TriggerSpec{
					Broker: "br",
				},
			},
			brokers: []*eventing.Broker{
				{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "ns",
						Name:      "br",
						Annotations: map[string]string{
							eventing.BrokerClassAnnotationKey: kafka.BrokerClass + "-boh",
						},
					},
				},
			},
			pass: true,
		},
	}

	for _, tc := range tt {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			brokerInformer := brokerinformer.Get(ctx)
			for _, obj := range tc.brokers {
				_ = brokerInformer.Informer().GetStore().Add(obj)
			}
			filter := filterTriggers(brokerInformer.Lister(), kafka.BrokerClass)
			pass := filter(tc.trigger)
			assert.Equal(t, tc.pass, pass)
		})
	}
}
