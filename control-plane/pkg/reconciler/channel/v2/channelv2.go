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

	"go.uber.org/multierr"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/pkg/apis"
	"knative.dev/pkg/kmeta"
	"knative.dev/pkg/reconciler"

	messagingv1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"

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

	KafkaChannelConditionConsumerGroup apis.ConditionType = "ConsumerGroup" //condition is registered by controller
)

var (
	conditionSet = apis.NewLivingConditionSet(
		KafkaChannelConditionConsumerGroup,
	)
)

type Reconciler struct {
	ConsumerGroupLister internalslst.ConsumerGroupLister
	InternalsClient     internalsclient.Interface
}

func (r *Reconciler) ReconcileKind(ctx context.Context, channel *messagingv1beta1.KafkaChannel) reconciler.Event {

	err := r.reconcileConsumerGroup(ctx, channel)
	if err != nil {
		channel.GetConditionSet().Manage(&channel.Status).MarkFalse(KafkaChannelConditionConsumerGroup, "failed to reconcile consumer group", err.Error())
		return err
	}
	channel.GetConditionSet().Manage(&channel.Status).MarkTrue(KafkaChannelConditionConsumerGroup)

	//todo other status fields relevant for channels
	//SubscribableStatus
	//AddressStatus
	//DeadLetterSinkURI

	return nil
}

func (r Reconciler) reconcileConsumerGroup(ctx context.Context, channel *messagingv1beta1.KafkaChannel) error {

	topicName := kafka.ChannelTopic(TopicPrefix, channel)
	var globalErr error

	for i := range channel.Spec.Subscribers {
		s := &channel.Spec.Subscribers[i]
		newcg := &internalscg.ConsumerGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      string(s.UID),
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
							"group.id": consumerGroup(channel, s),
						}},
						Delivery: &internalscg.DeliverySpec{
							DeliverySpec: channel.Spec.Delivery,
							Ordering:     DefaultDeliveryOrder,
						},
					},
				},
			},
		}

		cg, err := r.ConsumerGroupLister.ConsumerGroups(channel.GetNamespace()).Get(string(s.UID)) //Get by consumer group id
		if err != nil && !apierrors.IsNotFound(err) {
			globalErr = multierr.Append(globalErr, err)
		}

		if apierrors.IsNotFound(err) {
			_, err = r.InternalsClient.InternalV1alpha1().ConsumerGroups(newcg.GetNamespace()).Create(ctx, newcg, metav1.CreateOptions{})
			if err != nil && !apierrors.IsAlreadyExists(err) {
				globalErr = multierr.Append(globalErr, fmt.Errorf("failed to create consumer group %s/%s: %w", newcg.GetNamespace(), newcg.GetName(), err))
			}
			continue
		}

		if !equality.Semantic.DeepDerivative(newcg.Spec, cg.Spec) {
			newCg := &internalscg.ConsumerGroup{
				TypeMeta:   cg.TypeMeta,
				ObjectMeta: cg.ObjectMeta,
				Spec:       newcg.Spec,
				Status:     cg.Status,
			}
			if _, err = r.InternalsClient.InternalV1alpha1().ConsumerGroups(cg.GetNamespace()).Update(ctx, newCg, metav1.UpdateOptions{}); err != nil {
				globalErr = multierr.Append(globalErr, fmt.Errorf("failed to update consumer group %s/%s: %w", newCg.GetNamespace(), newCg.GetName(), err))
			}
		}
	}

	return globalErr
}

// consumerGroup returns a consumerGroup name for the given channel and subscription
func consumerGroup(channel *messagingv1beta1.KafkaChannel, s *v1.SubscriberSpec) string {
	return fmt.Sprintf("kafka.%s.%s.%s", channel.Namespace, channel.Name, string(s.UID))
}
