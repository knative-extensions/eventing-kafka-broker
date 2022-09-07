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
	corelisters "k8s.io/client-go/listers/core/v1"
	"knative.dev/eventing-autoscaler-keda/pkg/reconciler/keda"
	sources "knative.dev/eventing-kafka/pkg/apis/sources/v1beta1"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
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
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	kafkalogging "knative.dev/eventing-kafka-broker/control-plane/pkg/logging"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/security"

	kedaclientset "knative.dev/eventing-autoscaler-keda/third_party/pkg/client/clientset/versioned"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/apis/eventing/v1alpha1"
)

const (
	deliveryOrderAnnotation = "kafka.eventing.knative.dev/delivery.order"
	// TopicPrefix is the Kafka Broker topic prefix - (topic name: knative-broker-<broker-namespace>-<broker-name>).
	TopicPrefix = "knative-broker-"

	// ExternalTopicAnnotation for using external kafka topic for the broker
	ExternalTopicAnnotation = "kafka.eventing.knative.dev/external.topic"
)

type Reconciler struct {
	BrokerLister        eventinglisters.BrokerLister
	ConfigMapLister     corelisters.ConfigMapLister
	EventingClient      eventingclientset.Interface
	Env                 *config.Env
	ConsumerGroupLister internalslst.ConsumerGroupLister
	InternalsClient     internalsclient.Interface
	KedaClient          kedaclientset.Interface
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

			logger.Debug("broker not found", zap.String("finalizeDuringReconcile", "notFound"))
			return fmt.Errorf("failed to get broker: %w", err)
		}
	}

	// Ignore Triggers that are associated with a Broker we don't own.
	if hasRelevantBroker, brokerClass := r.hasRelevantBrokerClass(broker); !hasRelevantBroker {
		logger.Debug("Ignoring Trigger", zap.String(eventing.BrokerClassAnnotationKey, brokerClass))
		return nil
	}

	trigger.Status.PropagateBrokerCondition(broker.Status.GetTopLevelCondition())

	if !broker.IsReady() {
		// Trigger will get re-queued once this broker is ready.
		return nil
	}

	cg, err := r.reconcileConsumerGroup(ctx, broker, trigger)
	if err != nil {
		trigger.Status.MarkDependencyFailed("failed to reconcile consumer group", err.Error())
		return err
	}
	propagateConsumerGroupStatus(cg, trigger)

	return nil
}

func (r Reconciler) reconcileConsumerGroup(ctx context.Context, broker *eventing.Broker, trigger *eventing.Trigger) (*internalscg.ConsumerGroup, error) {

	var deliveryOrdering = internals.Unordered
	var err error
	deliveryOrderingAnnotationValue, ok := trigger.Annotations[deliveryOrderAnnotation]
	if ok {
		deliveryOrdering, err = deliveryOrderingFromString(deliveryOrderingAnnotationValue)
		if err != nil {
			return nil, err
		}
	}

	offset := sources.OffsetLatest
	isLatestOffset, err := kafka.IsOffsetLatest(r.ConfigMapLister, r.Env.DataPlaneConfigMapNamespace, r.Env.ContractConfigMapName, "config-kafka-broker-consumer.properties")
	if err != nil {
		return nil, fmt.Errorf("failed to determine initial offset: %w", err)
	}
	if !isLatestOffset {
		offset = sources.OffsetEarliest
	}

	secretName := broker.Status.Annotations[security.AuthSecretNameKey]
	//secretNamespace := broker.Status.Annotations[security.AuthSecretNamespaceKey]
	bootstrapServers := broker.Status.Annotations[kafka.BootstrapServersConfigMapKey]
	topicName, ok := broker.Annotations[ExternalTopicAnnotation]
	if !ok {
		topicName = kafka.BrokerTopic(TopicPrefix, broker)
	}

	expectedCg := &internalscg.ConsumerGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      string(trigger.UID),
			Namespace: trigger.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*kmeta.NewControllerRef(trigger),
			},
			Labels: map[string]string{
				internalscg.UserFacingResourceLabelSelector: strings.ToLower(trigger.GetGroupVersionKind().Kind),
			},
		},
		Spec: internalscg.ConsumerGroupSpec{
			Template: internalscg.ConsumerTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						internalscg.ConsumerLabelSelector: string(trigger.UID),
					},
				},
				Spec: internalscg.ConsumerSpec{
					Topics: []string{topicName},
					Configs: internalscg.ConsumerConfigs{Configs: map[string]string{
						"group.id":          string(trigger.UID),
						"bootstrap.servers": bootstrapServers,
					}},
					Delivery: &internalscg.DeliverySpec{
						DeliverySpec:  deliverySpec(broker, trigger),
						Ordering:      deliveryOrdering,
						InitialOffset: offset,
					},
					Filters: &internalscg.Filters{
						Filter:  trigger.Spec.Filter,
						Filters: trigger.Spec.Filters,
					},
					Subscriber: trigger.Spec.Subscriber,
					Reply:      &internalscg.ReplyStrategy{TopicReply: &internalscg.TopicReply{Enabled: true}},
				},
			},
		},
	}

	//expectedCg.Spec.Replicas = ptr.Int32(2) //to be removed

	// TODO: make these parameters below configurable and maybe unexposed
	if broker.Annotations != nil {
		expectedCg.Annotations = map[string]string{}
		if broker.GetAnnotations()[keda.AutoscalingClassAnnotation] != "" {
			expectedCg.Annotations[keda.AutoscalingClassAnnotation] = broker.GetAnnotations()[keda.AutoscalingClassAnnotation]
		}
		if broker.GetAnnotations()[keda.AutoscalingMinScaleAnnotation] != "" {
			expectedCg.Annotations[keda.AutoscalingMinScaleAnnotation] = broker.GetAnnotations()[keda.AutoscalingMinScaleAnnotation]
		}
		if broker.GetAnnotations()[keda.AutoscalingMaxScaleAnnotation] != "" {
			expectedCg.Annotations[keda.AutoscalingMaxScaleAnnotation] = broker.GetAnnotations()[keda.AutoscalingMaxScaleAnnotation]
		}
		if broker.GetAnnotations()[keda.KedaAutoscalingPollingIntervalAnnotation] != "" {
			expectedCg.Annotations[keda.KedaAutoscalingPollingIntervalAnnotation] = broker.GetAnnotations()[keda.KedaAutoscalingPollingIntervalAnnotation]
		}
		if broker.GetAnnotations()[keda.KedaAutoscalingCooldownPeriodAnnotation] != "" {
			expectedCg.Annotations[keda.KedaAutoscalingCooldownPeriodAnnotation] = broker.GetAnnotations()[keda.KedaAutoscalingCooldownPeriodAnnotation]
		}
		if broker.GetAnnotations()[keda.KedaAutoscalingKafkaLagThreshold] != "" {
			expectedCg.Annotations[keda.KedaAutoscalingKafkaLagThreshold] = broker.GetAnnotations()[keda.KedaAutoscalingKafkaLagThreshold]
		}
	}

	if secretName != "" {
		expectedCg.Spec.Template.Spec.Auth = &internalscg.Auth{
			AuthSpec: &v1alpha1.Auth{
				Secret: &v1alpha1.Secret{
					Ref: &v1alpha1.SecretReference{
						Name: secretName,
					},
				},
			},
		}
	}

	cg, err := r.ConsumerGroupLister.ConsumerGroups(trigger.GetNamespace()).Get(string(trigger.UID)) //Get by consumer group name
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, err
	}
	if apierrors.IsNotFound(err) {
		cg, err := r.InternalsClient.InternalV1alpha1().ConsumerGroups(expectedCg.GetNamespace()).Create(ctx, expectedCg, metav1.CreateOptions{})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("failed to create consumer group %s/%s: %w", expectedCg.GetNamespace(), expectedCg.GetName(), err)
		}
		return cg, nil
	}

	if equality.Semantic.DeepDerivative(expectedCg.Spec, cg.Spec) {
		return cg, nil
	}

	newCg := &internalscg.ConsumerGroup{
		TypeMeta:   cg.TypeMeta,
		ObjectMeta: cg.ObjectMeta,
		Spec:       expectedCg.Spec,
		Status:     cg.Status,
	}
	if cg, err = r.InternalsClient.InternalV1alpha1().ConsumerGroups(cg.GetNamespace()).Update(ctx, newCg, metav1.UpdateOptions{}); err != nil {
		return nil, fmt.Errorf("failed to update consumer group %s/%s: %w", newCg.GetNamespace(), newCg.GetName(), err)
	}

	return cg, nil
}

func deliverySpec(broker *eventing.Broker, trigger *eventing.Trigger) *eventingduck.DeliverySpec {
	// TOOD(pierDipi) use `Merge` in https://github.com/knative/eventing/pull/6277/files
	if trigger.Spec.Delivery != nil {
		return trigger.Spec.Delivery
	}
	return broker.Spec.Delivery
}

func propagateConsumerGroupStatus(cg *internalscg.ConsumerGroup, trigger *eventing.Trigger) {
	if cg.IsReady() {
		trigger.Status.MarkDependencySucceeded()
	} else {
		topLevelCondition := cg.GetConditionSet().Manage(cg.GetStatus()).GetTopLevelCondition()
		if topLevelCondition == nil {
			trigger.Status.MarkDependencyUnknown("failed to reconcile consumer group", "consumer group is not ready")
		} else {
			trigger.Status.MarkDependencyFailed(topLevelCondition.Reason, topLevelCondition.Message)
		}
	}
	trigger.Status.SubscriberURI = cg.Status.SubscriberURI
	trigger.Status.MarkSubscriberResolvedSucceeded()

	trigger.Status.DeadLetterSinkURI = cg.Status.DeadLetterSinkURI
	trigger.Status.MarkDeadLetterSinkResolvedSucceeded()
}

func (r *Reconciler) hasRelevantBrokerClass(broker *eventing.Broker) (bool, string) {
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
