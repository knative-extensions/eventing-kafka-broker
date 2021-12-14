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

package consumergroup

import (
	"context"
	"fmt"
	"sort"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"knative.dev/pkg/apis"
	"knative.dev/pkg/reconciler"

	kafkainternals "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing/v1alpha1"
	internalv1alpha1 "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/clientset/versioned/typed/eventing/v1alpha1"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/reconciler/eventing/v1alpha1/consumergroup"
	kafkainternalslisters "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/listers/eventing/v1alpha1"
)

const (
	// KafkaConditionConsumerGroupReady has status True when the KafkaChannel has created a consumer group.
	KafkaConditionConsumerGroupReady apis.ConditionType = "ConsumerGroupReady"

	// KafkaConditionConsumerGroupNotReady has status False when the KafkaChannel has failed to create a consumer group.
	KafkaConditionConsumerGroupNotReady apis.ConditionType = "ConsumerGroupNotReady"
)

type Reconciler struct {
	ConsumerLister  kafkainternalslisters.ConsumerLister
	InternalsClient internalv1alpha1.InternalV1alpha1Interface
}

func (r Reconciler) ReconcileKind(ctx context.Context, cg *kafkainternals.ConsumerGroup) reconciler.Event {
	if err := r.reconcileConsumers(ctx, cg); err != nil {
		return err
	}
	cg.MarkReconcileConsumersSucceeded()

	return nil
}

func (r Reconciler) reconcileConsumers(ctx context.Context, cg *kafkainternals.ConsumerGroup) error {

	// Get consumers associated with the ConsumerGroup.
	existingConsumers, err := r.ConsumerLister.Consumers(cg.GetNamespace()).List(labels.SelectorFromSet(cg.Spec.Selector))
	if err != nil {
		return cg.MarkReconcileConsumersFailed("ListConsumers", err)
	}

	// Stable sort consumers so that we give consumers different deletion
	// priorities based on their state.
	//
	// Consumers at the tail of the list are deleted first.
	sort.SliceStable(existingConsumers, func(i, j int) bool {
		return existingConsumers[i].IsLessThan(existingConsumers[j])
	})

	nConsumers := int32(len(existingConsumers))

	for i := int32(0); i < *cg.Spec.Replicas && i < nConsumers; i++ {
		if err := r.reconcileConsumer(ctx, cg, existingConsumers[i]); err != nil {
			return cg.MarkReconcileConsumersFailed("ReconcileConsumer", err)
		}
	}

	if nConsumers > *cg.Spec.Replicas {
		toBeDeleted := existingConsumers[:nConsumers-*cg.Spec.Replicas]
		for _, c := range toBeDeleted {
			if err := r.finalizeConsumer(ctx, c); err != nil {
				return cg.MarkReconcileConsumersFailed("FinalizeConsumer", err)
			}
		}
		return nil
	}
	if nConsumers < *cg.Spec.Replicas {
		for i := int32(0); i < *cg.Spec.Replicas-nConsumers; i++ {
			if err := r.reconcileConsumer(ctx, cg, nil); err != nil {
				return cg.MarkReconcileConsumersFailed("ReconcileConsumer", err)
			}
		}
		return nil
	}

	return nil
}

func (r Reconciler) reconcileConsumer(ctx context.Context, cg *kafkainternals.ConsumerGroup, c *kafkainternals.Consumer) error {
	if c == nil {
		// Consumer doesn't exist, create it.
		c = cg.ConsumerFromTemplate()

		if _, err := r.InternalsClient.Consumers(cg.GetNamespace()).Create(ctx, c, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("failed to create consumer %s/%s: %w", c.GetNamespace(), c.GetName(), err)
		}
		return nil
	}
	if equality.Semantic.DeepDerivative(cg.Spec.Template, c.Spec) {
		// Consumer is equal to the template.
		return nil
	}
	// Update existing Consumer.
	newC := &kafkainternals.Consumer{
		TypeMeta:   c.TypeMeta,
		ObjectMeta: c.ObjectMeta,
		Spec:       cg.Spec.Template.Spec,
		Status:     c.Status,
	}
	if _, err := r.InternalsClient.Consumers(cg.GetNamespace()).Update(ctx, newC, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("failed to update consumer %s/%s: %w", newC.GetNamespace(), newC.GetName(), err)
	}

	return nil
}

func (r Reconciler) finalizeConsumer(ctx context.Context, consumer *kafkainternals.Consumer) error {
	dOpts := metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{UID: &consumer.UID},
	}
	err := r.InternalsClient.Consumers(consumer.GetNamespace()).Delete(ctx, consumer.GetName(), dOpts)
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to remove consumer %s/%s: %w", consumer.GetNamespace(), consumer.GetName(), err)
	}
	return nil
}

var (
	_ consumergroup.Interface = &Reconciler{}
)
