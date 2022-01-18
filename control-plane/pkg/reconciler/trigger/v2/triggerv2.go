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

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka"
)

const (
	deliveryOrderAnnotation = "kafka.eventing.knative.dev/delivery.order"
)

type Reconciler struct {
	BrokerLister        eventinglisters.BrokerLister
	EventingClient      eventingclientset.Interface
	Env                 *config.Env
	ConsumerGroupLister internalslst.ConsumerGroupLister
	InternalsClient     internalsclient.Interface
}

func (r *Reconciler) ReconcileKind(ctx context.Context, trigger *eventing.Trigger) reconciler.Event {
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

	cg, err := r.reconcileConsumerGroup(ctx, trigger)
	if err != nil {
		trigger.Status.MarkDependencyFailed("failed to reconcile consumer group", err.Error())
		return err
	}
	if cg.IsReady() {
		trigger.Status.MarkDependencySucceeded()
	} else {
		topLevelCondition := cg.GetConditionSet().Manage(cg.GetStatus()).GetTopLevelCondition()
		if topLevelCondition == nil {
			trigger.Status.MarkDependencyUnknown("failed to reconcile consumer group", "consumer group not ready")
		} else {
			trigger.Status.MarkDependencyFailed(topLevelCondition.Reason, topLevelCondition.Message)
		}
	}
	trigger.Status.SubscriberURI = cg.Status.SubscriberURI
	trigger.Status.MarkSubscriberResolvedSucceeded()

	trigger.Status.DeadLetterSinkURI = cg.Status.DeadLetterSinkURI
	trigger.Status.MarkDeadLetterSinkResolvedSucceeded()

	return nil
}

func (r Reconciler) reconcileConsumerGroup(ctx context.Context, trigger *eventing.Trigger) (*internalscg.ConsumerGroup, error) {

	var deliveryOrdering = internals.Ordered
	var err error
	deliveryOrderingAnnotationValue, ok := trigger.Annotations[deliveryOrderAnnotation]
	if ok {
		deliveryOrdering, err = deliveryOrderingFromString(deliveryOrderingAnnotationValue)
		if err != nil {
			return nil, err
		}
	}

	newcg := &internalscg.ConsumerGroup{
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
					Topics:  []string{},                                                                               //todo get topics from broker resource
					Configs: internalscg.ConsumerConfigs{Configs: map[string]string{"group.id": string(trigger.UID)}}, //todo get bootstrap.servers from broker resource
					Delivery: &internalscg.DeliverySpec{
						DeliverySpec: trigger.Spec.Delivery,
						Ordering:     deliveryOrdering},
					Filters: &internalscg.Filters{
						Filter:  trigger.Spec.Filter,
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

func isKnativeKafkaBroker(broker *eventing.Broker) (bool, string) {
	brokerClass := broker.GetAnnotations()[eventing.BrokerClassAnnotationKey]
	return brokerClass == kafka.BrokerClass, brokerClass
}

func deliveryOrderingFromString(val string) (internals.DeliveryOrdering, error) {
	switch strings.ToLower(val) {
	case string(internals.Ordered):
		return internals.Ordered, nil
	case string(internals.Unordered):
		return internals.Unordered, nil
	default:
		return internals.Unordered, fmt.Errorf("invalid annotation %s value: %s. Allowed values [ %q | %q ]", deliveryOrderAnnotation, val, internals.Ordered, internals.Unordered)
	}
}
