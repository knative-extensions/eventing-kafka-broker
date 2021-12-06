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
	"context"
	"fmt"

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1"
	eventingclientset "knative.dev/eventing/pkg/client/clientset/versioned"
	eventinglisters "knative.dev/eventing/pkg/client/listers/eventing/v1"
	"knative.dev/pkg/kmeta"
	"knative.dev/pkg/reconciler"

	internals "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing"
	internalscg "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing/v1alpha1"
	internalsclient "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/clientset/versioned"
	internalslst "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/listers/eventing/v1alpha1"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	kafkalogging "knative.dev/eventing-kafka-broker/control-plane/pkg/logging"
)

type ReconcilerV2 struct {
	BrokerLister        eventinglisters.BrokerLister
	EventingClient      eventingclientset.Interface
	Env                 *config.Env
	ConsumerGroupLister internalslst.ConsumerGroupLister
	InternalsClient     internalsclient.Interface
}

func (r *ReconcilerV2) ReconcileKind(ctx context.Context, trigger *eventing.Trigger) reconciler.Event {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		return r.reconcileKind(ctx, trigger)
	})
}

func (r *ReconcilerV2) reconcileKind(ctx context.Context, trigger *eventing.Trigger) reconciler.Event {
	logger := kafkalogging.CreateReconcileMethodLogger(ctx, trigger)

	broker, err := r.BrokerLister.Brokers(trigger.Namespace).Get(trigger.Spec.Broker)
	if err != nil && !apierrors.IsNotFound(err) {
		trigger.Status.MarkBrokerFailed("Failed to get broker", "%v", err)
		return fmt.Errorf("failed to get broker: %w", err)
	}

	if apierrors.IsNotFound(err) {
		// Actually check if the broker doesn't exist.
		// Note: do not introduce another `broker` variable with `:`
		broker, err = r.EventingClient.EventingV1().Brokers(trigger.Namespace).Get(ctx, trigger.Spec.Broker, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to get broker: %w", err)
		}
	}

	// Ignore Triggers that are associated with a Broker we don't own.
	if isKnativeKafkaBroker, brokerClass := isKnativeKafkaBroker(broker); !isKnativeKafkaBroker {
		logger.Debug("Ignoring Trigger", zap.String(eventing.BrokerClassAnnotationKey, brokerClass))
		return nil
	}

	trigger.Status.PropagateBrokerCondition(broker.Status.GetTopLevelCondition())

	if !broker.IsReady() {
		// Trigger will get re-queued once this broker is ready.
		return nil
	}

	consgroup, err := r.reconcileConsumerGroup(ctx, trigger)
	if err != nil {
		trigger.Status.MarkDependencyFailed("failed to reconcile consumer group", err.Error())
		return err
	}
	trigger.Status.MarkDependencySucceeded()

	trigger.Status.SubscriberURI = consgroup.Status.SubscriberURI
	trigger.Status.MarkSubscriberResolvedSucceeded()

	trigger.Status.DeadLetterSinkURI = consgroup.Status.DeadLetterSinkURI
	trigger.Status.MarkDeadLetterSinkResolvedSucceeded()

	return nil
}

func (r ReconcilerV2) reconcileConsumerGroup(ctx context.Context, trigger *eventing.Trigger) (consgroup *internalscg.ConsumerGroup, err error) {

	newcg := &internalscg.ConsumerGroup{
		TypeMeta: metav1.TypeMeta{
			APIVersion: internalscg.ConsumerGroupGroupVersionKind.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      string(trigger.UID),
			Namespace: trigger.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*kmeta.NewControllerRef(trigger),
			},
		},
		Spec: internalscg.ConsumerGroupSpec{
			Template: internalscg.ConsumerTemplateSpec{
				Spec: internalscg.ConsumerSpec{
					//Topics: []string{}, //todo from contract resource
					//Configs: internalscg.ConsumerConfigs{Configs: map[string]string{"group.id": ""}}, //todo
					Delivery: &internalscg.DeliverySpec{
						Delivery: trigger.Spec.Delivery,
						Ordering: internals.Ordered,
					},
					Filters: &internalscg.Filters{
						Filters: trigger.Spec.Filters,
					},
					Subscriber: trigger.Spec.Subscriber,
				},
			},
		},
	}

	cg, err := r.ConsumerGroupLister.ConsumerGroups(trigger.GetNamespace()).Get(string(trigger.UID)) //Get by consumer group name
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, err
	}

	if apierrors.IsNotFound(err) {
		cg, err := r.InternalsClient.InternalV1alpha1().ConsumerGroups(newcg.GetNamespace()).Create(ctx, newcg, metav1.CreateOptions{})
		if err != nil {
			return nil, err
		}
		fmt.Printf("created consumer group%s/%s", newcg.GetNamespace(), newcg.GetName())
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
	if _, err := r.InternalsClient.InternalV1alpha1().ConsumerGroups(cg.GetNamespace()).Update(ctx, newCg, metav1.UpdateOptions{}); err != nil {
		return nil, fmt.Errorf("failed to update consumer group %s/%s: %w", newCg.GetNamespace(), newCg.GetName(), err)
	}

	return cg, nil
}
