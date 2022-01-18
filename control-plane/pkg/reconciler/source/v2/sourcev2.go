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

package v2

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	sources "knative.dev/eventing-kafka/pkg/apis/sources/v1beta1"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/kmeta"
	"knative.dev/pkg/reconciler"

	internals "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing"
	internalscg "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing/v1alpha1"
	internalsclient "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/clientset/versioned"
	internalslst "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/listers/eventing/v1alpha1"
)

const (
	DefaultDeliveryOrder = internals.Ordered

	KafkaConditionConsumerGroup apis.ConditionType = "ConsumerGroup" //condition is registered by controller
)

var (
	conditionSet = apis.NewLivingConditionSet(
		KafkaConditionConsumerGroup,
	)
)

type ReconcilerV2 struct {
	ConsumerGroupLister internalslst.ConsumerGroupLister
	InternalsClient     internalsclient.Interface
}

func (r *ReconcilerV2) ReconcileKind(ctx context.Context, ks *sources.KafkaSource) reconciler.Event {

	cg, err := r.reconcileConsumerGroup(ctx, ks)
	if err != nil {
		ks.GetConditionSet().Manage(&ks.Status).MarkFalse(KafkaConditionConsumerGroup, "failed to reconcile consumer group", err.Error())
		return err
	}
	if cg.IsReady() {
		ks.GetConditionSet().Manage(&ks.Status).MarkTrue(KafkaConditionConsumerGroup)
	} else {
		topLevelCondition := cg.GetConditionSet().Manage(cg.GetStatus()).GetTopLevelCondition()
		ks.GetConditionSet().Manage(&ks.Status).MarkFalse(
			KafkaConditionConsumerGroup,
			topLevelCondition.Reason,
			topLevelCondition.Message,
		)
	}

	ks.Status.MarkSink(cg.Status.SubscriberURI)
	ks.Status.Placeable = cg.Status.Placeable
	if cg.Status.Replicas != nil {
		ks.Status.Consumers = *cg.Status.Replicas
	}

	return nil
}

func (r ReconcilerV2) reconcileConsumerGroup(ctx context.Context, ks *sources.KafkaSource) (*internalscg.ConsumerGroup, error) {

	newcg := &internalscg.ConsumerGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      string(ks.UID),
			Namespace: ks.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*kmeta.NewControllerRef(ks),
			},
		},
		Spec: internalscg.ConsumerGroupSpec{
			Template: internalscg.ConsumerTemplateSpec{
				Spec: internalscg.ConsumerSpec{
					Topics: ks.Spec.Topics,
					Configs: internalscg.ConsumerConfigs{Configs: map[string]string{
						"group.id":          ks.Spec.ConsumerGroup,
						"bootstrap.servers": strings.Join(ks.Spec.BootstrapServers, ","),
					}},
					Auth: &internalscg.Auth{
						NetSpec: &ks.Spec.KafkaAuthSpec.Net,
					},
					Delivery: &internalscg.DeliverySpec{
						Ordering: DefaultDeliveryOrder,
					},
					Subscriber: ks.Spec.Sink,
					CloudEventOverrides: &duckv1.CloudEventOverrides{
						Extensions: ks.Spec.CloudEventOverrides.Extensions,
					},
				},
			},
			Replicas: ks.Spec.Consumers,
		},
	}

	cg, err := r.ConsumerGroupLister.ConsumerGroups(ks.GetNamespace()).Get(string(ks.UID)) //Get by consumer group id
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, err
	}

	if apierrors.IsNotFound(err) {
		cg, err := r.InternalsClient.InternalV1alpha1().ConsumerGroups(newcg.GetNamespace()).Create(ctx, newcg, metav1.CreateOptions{})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("failed to create consumer group %s/%s: %w", newcg.GetNamespace(), newcg.GetName(), err)
		}
		if apierrors.IsAlreadyExists(err) {
			return newcg, nil
		}
		return cg, nil
	}

	if equality.Semantic.DeepDerivative(newcg.Spec, cg.Spec) {
		return cg, nil
	}

	newCg := &internalscg.ConsumerGroup{
		TypeMeta:   cg.TypeMeta,
		ObjectMeta: cg.ObjectMeta,
		Spec:       newcg.Spec,
		Status:     cg.Status,
	}
	if cg, err = r.InternalsClient.InternalV1alpha1().ConsumerGroups(cg.GetNamespace()).Update(ctx, newCg, metav1.UpdateOptions{}); err != nil {
		return nil, fmt.Errorf("failed to update consumer group %s/%s: %w", newCg.GetNamespace(), newCg.GetName(), err)
	}

	return cg, nil
}
