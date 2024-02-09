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

	corelisters "k8s.io/client-go/listers/core/v1"
	"knative.dev/pkg/reconciler"
	"knative.dev/pkg/resolver"

	eventing "knative.dev/eventing/pkg/apis/eventing/v1"
	eventingclientset "knative.dev/eventing/pkg/client/clientset/versioned"
	eventinglisters "knative.dev/eventing/pkg/client/listers/eventing/v1"

	apisconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
)

type NamespacedReconciler struct {
	*base.Reconciler
	*FlagsHolder

	BrokerLister         eventinglisters.BrokerLister
	ConfigMapLister      corelisters.ConfigMapLister
	ServiceAccountLister corelisters.ServiceAccountLister
	EventingClient       eventingclientset.Interface
	Resolver             *resolver.URIResolver

	Env *config.Env

	NewKafkaClusterAdminClient kafka.NewClusterAdminClientFunc
	NewKafkaClient             kafka.NewClientFunc
	InitOffsetsFunc            kafka.InitOffsetsFunc
}

func (r *NamespacedReconciler) ReconcileKind(ctx context.Context, trigger *eventing.Trigger) reconciler.Event {
	br := r.createReconcilerForTriggerInstance(trigger)

	return br.ReconcileKind(ctx, trigger)
}

func (r *NamespacedReconciler) FinalizeKind(ctx context.Context, trigger *eventing.Trigger) reconciler.Event {
	br := r.createReconcilerForTriggerInstance(trigger)

	return br.FinalizeKind(ctx, trigger)
}

func (r *NamespacedReconciler) createReconcilerForTriggerInstance(trigger *eventing.Trigger) *Reconciler {
	return &Reconciler{
		Reconciler: &base.Reconciler{
			KubeClient:                   r.Reconciler.KubeClient,
			PodLister:                    r.Reconciler.PodLister,
			SecretLister:                 r.Reconciler.SecretLister,
			Tracker:                      r.Reconciler.Tracker,
			ContractConfigMapName:        r.Reconciler.ContractConfigMapName,
			ContractConfigMapFormat:      r.Reconciler.ContractConfigMapFormat,
			DataPlaneConfigConfigMapName: r.DataPlaneConfigConfigMapName,
			DispatcherLabel:              r.DispatcherLabel,
			ReceiverLabel:                r.ReceiverLabel,

			// override
			DataPlaneNamespace:          trigger.Namespace,
			DataPlaneConfigMapNamespace: trigger.Namespace,
		},
		FlagsHolder: &FlagsHolder{
			Flags: r.Flags,
		},
		BrokerLister:         r.BrokerLister,
		ConfigMapLister:      r.ConfigMapLister,
		ServiceAccountLister: r.ServiceAccountLister,
		EventingClient:       r.EventingClient,
		Resolver:             r.Resolver,
		Env:                  r.Env,
		// override
		BrokerClass:                kafka.NamespacedBrokerClass,
		DataPlaneConfigMapLabeler:  kafka.NamespacedDataplaneLabelConfigmapOption,
		KafkaFeatureFlags:          apisconfig.DefaultFeaturesConfig(),
		NewKafkaClusterAdminClient: r.NewKafkaClusterAdminClient,
		NewKafkaClient:             r.NewKafkaClient,
		InitOffsetsFunc:            r.InitOffsetsFunc,
	}
}
