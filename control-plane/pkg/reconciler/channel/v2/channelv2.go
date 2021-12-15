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

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"knative.dev/pkg/kmeta"
	"knative.dev/pkg/reconciler"

	messagingv1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/consumergroup"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka"

	internals "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing"
	internalscg "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing/v1alpha1"
	internalsclient "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/clientset/versioned"
	internalslst "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/listers/eventing/v1alpha1"
)

const (
	// TopicPrefix is the Kafka Channel topic prefix - (topic name: knative-messaging-kafka.<channel-namespace>.<channel-name>).
	TopicPrefix          = "knative-messaging-kafka"
	DefaultDeliveryOrder = internals.Ordered
)

type Reconciler struct {
	ConsumerGroupLister internalslst.ConsumerGroupLister
	InternalsClient     internalsclient.Interface
}

func (r *Reconciler) ReconcileKind(ctx context.Context, channel *messagingv1beta1.KafkaChannel) reconciler.Event {

	consgroup, err := r.reconcileConsumerGroup(ctx, channel)
	if err != nil {
		channel.GetConditionSet().Manage(&channel.Status).MarkFalse(consumergroup.KafkaConditionConsumerGroupNotReady, "failed to reconcile consumer group", err.Error())
		return err
	}
	channel.GetConditionSet().Manage(&channel.Status).MarkTrue(consumergroup.KafkaConditionConsumerGroupReady)

	//todo other status fields relevant for channels

	//SubscribableStatus
	//AddressStatus

	channel.Status.DeadLetterSinkURI = consgroup.Status.DeadLetterSinkURI //DeliveryStatus
	return nil
}

func (r Reconciler) reconcileConsumerGroup(ctx context.Context, channel *messagingv1beta1.KafkaChannel) (*internalscg.ConsumerGroup, error) {

	topicName := kafka.ChannelTopic(TopicPrefix, channel)
	newcg := &internalscg.ConsumerGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      string(channel.UID),
			Namespace: channel.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*kmeta.NewControllerRef(channel),
			},
		},
		Spec: internalscg.ConsumerGroupSpec{
			Template: internalscg.ConsumerTemplateSpec{
				Spec: internalscg.ConsumerSpec{
					Topics: []string{topicName},
					Configs: internalscg.ConsumerConfigs{Configs: map[string]string{
						"group.id": string(channel.UID),
					}},
					Delivery: &internalscg.DeliverySpec{
						DeliverySpec: channel.Spec.Delivery,
						Ordering:     DefaultDeliveryOrder,
					},
				},
			},
		},
	}

	cg, err := r.ConsumerGroupLister.ConsumerGroups(channel.GetNamespace()).Get(string(channel.UID)) //Get by consumer group id
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, err
	}

	if err := newcg.Validate(ctx); err != nil {
		return nil, fmt.Errorf("failed to validate expected consumer group %s/%s: %w", newcg.GetNamespace(), newcg.GetName(), err)
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
	if _, err := r.InternalsClient.InternalV1alpha1().ConsumerGroups(cg.GetNamespace()).Update(ctx, newCg, metav1.UpdateOptions{}); err != nil {
		return nil, fmt.Errorf("failed to update consumer group %s/%s: %w", newCg.GetNamespace(), newCg.GetName(), err)
	}

	return cg, nil
}
