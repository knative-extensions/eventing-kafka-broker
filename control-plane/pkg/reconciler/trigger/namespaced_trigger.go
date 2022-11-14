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

package trigger

import (
	"context"

	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"knative.dev/pkg/reconciler"

	eventing "knative.dev/eventing/pkg/apis/eventing/v1"
	eventingclientset "knative.dev/eventing/pkg/client/clientset/versioned"
	eventinglisters "knative.dev/eventing/pkg/client/listers/eventing/v1"

	internalsclient "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/clientset/versioned"
	internalslst "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/listers/eventing/v1alpha1"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
)

type NamespacedReconciler struct {
	BrokerLister        eventinglisters.BrokerLister
	ConfigMapLister     corelisters.ConfigMapLister
	EventingClient      eventingclientset.Interface
	ConsumerGroupLister internalslst.ConsumerGroupLister
	InternalsClient     internalsclient.Interface
	SecretLister        corelisters.SecretLister
	KubeClient          kubernetes.Interface

	Env *config.Env
}

func (r *NamespacedReconciler) ReconcileKind(ctx context.Context, trigger *eventing.Trigger) reconciler.Event {
	br := r.createReconcilerForTriggerInstance(trigger)

	return br.ReconcileKind(ctx, trigger)
}

func (r *NamespacedReconciler) createReconcilerForTriggerInstance(trigger *eventing.Trigger) *Reconciler {
	return &Reconciler{
		BrokerLister:        r.BrokerLister,
		ConfigMapLister:     r.ConfigMapLister,
		EventingClient:      r.EventingClient,
		Env:                 r.Env,
		ConsumerGroupLister: r.ConsumerGroupLister,
		InternalsClient:     r.InternalsClient,
		SecretLister:        r.SecretLister,
		KubeClient:          r.KubeClient,
		// override
		BrokerClass:               kafka.NamespacedBrokerClass,
		DataPlaneConfigMapLabeler: kafka.NamespacedDataplaneLabelConfigmapOption,
	}
}
