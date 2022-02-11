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
	"errors"
	"fmt"
	"math"
	"sort"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/utils/pointer"
	eventingduckv1alpha1 "knative.dev/eventing/pkg/apis/duck/v1alpha1"
	"knative.dev/pkg/reconciler"

	"knative.dev/eventing/pkg/scheduler"

	kafkainternals "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing/v1alpha1"
	internalv1alpha1 "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/clientset/versioned/typed/eventing/v1alpha1"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/reconciler/eventing/v1alpha1/consumergroup"
	kafkainternalslisters "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/listers/eventing/v1alpha1"
)

var (
	ErrNoSubscriberURI     = errors.New("no subscriber URI resolved")
	ErrNoDeadLetterSinkURI = errors.New("no dead letter sink URI resolved")
)

type schedulerFunc func(s string) scheduler.Scheduler

type Reconciler struct {
	SchedulerFunc   schedulerFunc
	ConsumerLister  kafkainternalslisters.ConsumerLister
	InternalsClient internalv1alpha1.InternalV1alpha1Interface

	NameGenerator names.NameGenerator

	SystemNamespace string
}

func (r Reconciler) ReconcileKind(ctx context.Context, cg *kafkainternals.ConsumerGroup) reconciler.Event {
	if err := r.schedule(cg); err != nil {
		return err
	}
	cg.MarkScheduleSucceeded()

	if err := r.reconcileConsumers(ctx, cg); err != nil {
		return err
	}

	if err := r.propagateStatus(cg); err != nil {
		return cg.MarkReconcileConsumersFailed("PropagateConsumerStatus", err)
	}

	if cg.Status.SubscriberURI == nil {
		_ = cg.MarkReconcileConsumersFailed("PropagateSubscriberURI", ErrNoSubscriberURI)
		return nil
	}
	if cg.HasDeadLetterSink() && cg.Status.DeadLetterSinkURI == nil {
		_ = cg.MarkReconcileConsumersFailed("PropagateDeadLetterSinkURI", ErrNoDeadLetterSinkURI)
		return nil
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

	placementConsumers := r.joinConsumersByPlacement(cg.Status.Placements, existingConsumers)

	for _, pc := range placementConsumers {
		if pc.Placement == nil {
			// There is no placement for pc.Consumers, so they need to be finalized.
			for _, c := range pc.Consumers {
				if err := r.finalizeConsumer(ctx, c); err != nil {
					return cg.MarkReconcileConsumersFailed("FinalizeConsumer", err)
				}
			}
			continue
		}

		if err := r.reconcileConsumersInPlacement(ctx, cg, pc); err != nil {
			return cg.MarkReconcileConsumersFailed("ReconcileConsumer", err)
		}
	}

	return nil
}

func (r Reconciler) reconcileConsumersInPlacement(
	ctx context.Context,
	cg *kafkainternals.ConsumerGroup,
	pc ConsumersPerPlacement) error {

	placement := *pc.Placement
	consumers := pc.Consumers

	// Check if there is a consumer for the given placement.
	if len(consumers) == 0 {
		return r.createConsumer(ctx, cg, placement)
	}

	// Stable sort consumers so that we give consumers different deletion
	// priorities based on their state (readiness, etc).
	//
	// Consumers at the tail of the list are deleted.
	sort.Stable(kafkainternals.ByReadinessAndCreationTime(consumers))

	for _, c := range consumers[1:] {
		if err := r.finalizeConsumer(ctx, c); err != nil {
			return cg.MarkReconcileConsumersFailed("FinalizeConsumer", err)
		}
	}

	// Do not modify informer copy.
	c := &kafkainternals.Consumer{}
	consumers[0].DeepCopyInto(c)

	expectedSpec := kafkainternals.ConsumerSpec{}
	cg.Spec.Template.Spec.DeepCopyInto(&expectedSpec)

	expectedSpec.VReplicas = pointer.Int32Ptr(placement.VReplicas)

	if equality.Semantic.DeepDerivative(expectedSpec, c.Spec) {
		// Consumer is equal to the template.
		return nil
	}

	c.Spec.PodBind = &kafkainternals.PodBind{PodName: placement.PodName, PodNamespace: r.SystemNamespace}
	c.Spec.VReplicas = pointer.Int32Ptr(*expectedSpec.VReplicas)

	// Update existing Consumer.
	if _, err := r.InternalsClient.Consumers(cg.GetNamespace()).Update(ctx, c, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("failed to update consumer %s/%s: %w", c.GetNamespace(), c.GetName(), err)
	}

	return nil
}

func (r Reconciler) createConsumer(ctx context.Context, cg *kafkainternals.ConsumerGroup, placement eventingduckv1alpha1.Placement) error {
	c := cg.ConsumerFromTemplate()

	c.Name = r.NameGenerator.GenerateName(cg.GetName() + "-")
	c.Spec.VReplicas = pointer.Int32Ptr(placement.VReplicas)
	c.Spec.PodBind = &kafkainternals.PodBind{PodName: placement.PodName, PodNamespace: r.SystemNamespace}

	if _, err := r.InternalsClient.Consumers(cg.GetNamespace()).Create(ctx, c, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("failed to create consumer %s/%s: %w", c.GetNamespace(), c.GetName(), err)
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

func (r Reconciler) schedule(cg *kafkainternals.ConsumerGroup) error {
	placements, err := r.SchedulerFunc(cg.GetUserFacingResourceRef().Kind).Schedule(cg)
	if err != nil {
		return cg.MarkScheduleConsumerFailed("Schedule", err)
	}
	// Sort placements by pod name.
	sort.SliceStable(placements, func(i, j int) bool { return placements[i].PodName < placements[j].PodName })

	cg.Status.Placements = placements

	return nil
}

type ConsumersPerPlacement struct {
	Placement *eventingduckv1alpha1.Placement
	Consumers []*kafkainternals.Consumer
}

func (r Reconciler) joinConsumersByPlacement(placements []eventingduckv1alpha1.Placement, consumers []*kafkainternals.Consumer) []ConsumersPerPlacement {
	placementConsumers := make([]ConsumersPerPlacement, 0, int(math.Max(float64(len(placements)), float64(len(consumers)))))

	// Group consumers by Pod bind.
	consumersByPod := make(map[kafkainternals.PodBind][]*kafkainternals.Consumer, len(consumers))
	for i := range consumers {
		consumersByPod[*consumers[i].Spec.PodBind] = append(consumersByPod[*consumers[i].Spec.PodBind], consumers[i])
	}

	// Group placements by Pod bind.
	placementsByPod := make(map[kafkainternals.PodBind]eventingduckv1alpha1.Placement, len(placements))
	for i := range placements {
		pb := kafkainternals.PodBind{
			PodName:      placements[i].PodName,
			PodNamespace: r.SystemNamespace,
		}

		v := placementsByPod[pb]
		placementsByPod[pb] = eventingduckv1alpha1.Placement{
			PodName:   pb.PodName,
			VReplicas: v.VReplicas + placements[i].VReplicas,
		}
	}

	for k := range placementsByPod {
		if _, ok := consumersByPod[k]; !ok {
			consumersByPod[k] = nil
		}
	}

	for pb := range consumersByPod {

		var p *eventingduckv1alpha1.Placement
		if v, ok := placementsByPod[pb]; ok {
			p = &v
		}

		c := consumersByPod[pb]

		placementConsumers = append(placementConsumers, ConsumersPerPlacement{Placement: p, Consumers: c})
	}

	sort.Slice(placementConsumers, func(i, j int) bool {
		if placementConsumers[i].Placement == nil {
			return true
		}
		if placementConsumers[j].Placement == nil {
			return false
		}
		return placementConsumers[i].Placement.PodName < placementConsumers[j].Placement.PodName
	})

	return placementConsumers
}

func (r Reconciler) propagateStatus(cg *kafkainternals.ConsumerGroup) error {
	consumers, err := r.ConsumerLister.Consumers(cg.GetNamespace()).List(labels.SelectorFromSet(cg.Spec.Selector))
	if err != nil {
		return fmt.Errorf("failed to list consumers for selector %+v: %w", cg.Spec.Selector, err)
	}
	count := int32(0)
	for _, c := range consumers {
		if c.IsReady() {
			if c.Spec.VReplicas != nil {
				count += *c.Spec.VReplicas
			}
			if c.Status.SubscriberURI != nil {
				cg.Status.SubscriberURI = c.Status.SubscriberURI
			}
			if c.Status.DeliveryStatus.DeadLetterSinkURI != nil {
				cg.Status.DeliveryStatus.DeadLetterSinkURI = c.Status.DeadLetterSinkURI
			}
		}
	}
	cg.Status.Replicas = pointer.Int32(count)

	return nil
}

var (
	_ consumergroup.Interface = &Reconciler{}
)
