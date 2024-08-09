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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1"
	"knative.dev/eventing/pkg/apis/feature"
	"knative.dev/eventing/pkg/auth"
	eventingclientset "knative.dev/eventing/pkg/client/clientset/versioned"
	eventinglisters "knative.dev/eventing/pkg/client/listers/eventing/v1"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/kmeta"
	"knative.dev/pkg/reconciler"

	apisconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/config"
	sources "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/sources/v1beta1"

	internalscg "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing/v1alpha1"
	kedafunc "knative.dev/eventing-kafka-broker/control-plane/pkg/autoscaler/keda"
	internalsclient "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/clientset/versioned"
	internalslst "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/listers/eventing/v1alpha1"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	kafkalogging "knative.dev/eventing-kafka-broker/control-plane/pkg/logging"
	brokerreconciler "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/broker"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/security"
)

const (
	deliveryOrderAnnotation = "kafka.eventing.knative.dev/delivery.order"
	// TopicPrefix is the Kafka Broker topic prefix - (topic name: knative-broker-<broker-namespace>-<broker-name>).
	TopicPrefix = "knative-broker-"

	// ExternalTopicAnnotation for using external kafka topic for the broker
	ExternalTopicAnnotation = "kafka.eventing.knative.dev/external.topic"
)

type Reconciler struct {
	BrokerLister         eventinglisters.BrokerLister
	ConfigMapLister      corelisters.ConfigMapLister
	ServiceAccountLister corelisters.ServiceAccountLister
	EventingClient       eventingclientset.Interface
	Env                  *config.Env
	ConsumerGroupLister  internalslst.ConsumerGroupLister
	InternalsClient      internalsclient.Interface
	SecretLister         corelisters.SecretLister
	KubeClient           kubernetes.Interface
}

func (r *Reconciler) ReconcileKind(ctx context.Context, trigger *eventing.Trigger) reconciler.Event {
	logger := kafkalogging.CreateReconcileMethodLogger(ctx, trigger)

	if trigger.Status.Annotations == nil {
		trigger.Status.Annotations = make(map[string]string)
	}

	err := auth.SetupOIDCServiceAccount(ctx,
		feature.FromContext(ctx),
		r.ServiceAccountLister,
		r.KubeClient,
		eventing.SchemeGroupVersion.WithKind("Trigger"),
		trigger.ObjectMeta,
		&trigger.Status,
		func(a *duckv1.AuthStatus) { trigger.Status.Auth = a },
	)
	if err != nil {
		return fmt.Errorf("failed to setup OIDC service account: %w", err)
	}

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

func (r *Reconciler) FinalizeKind(context.Context, *eventing.Trigger) reconciler.Event {
	// No-op, left here for backward compatibility.
	// This configures the knative/pkg base reconciler to remove the finalizer,
	// for more details, see https://github.com/knative-extensions/eventing-kafka-broker/issues/4034
	return nil
}

func (r *Reconciler) reconcileConsumerGroup(ctx context.Context, broker *eventing.Broker, trigger *eventing.Trigger) (*internalscg.ConsumerGroup, error) {

	var deliveryOrdering = sources.Unordered
	var err error
	deliveryOrderingAnnotationValue, ok := trigger.Annotations[deliveryOrderAnnotation]
	if ok {
		deliveryOrdering, err = deliveryOrderingFromString(deliveryOrderingAnnotationValue)
		if err != nil {
			return nil, err
		}
	}

	offset := sources.OffsetLatest
	isLatestOffset, err := kafka.IsOffsetLatest(r.ConfigMapLister, r.Env.DataPlaneConfigMapNamespace, r.Env.DataPlaneConfigConfigMapName, brokerreconciler.ConsumerConfigKey)
	if err != nil {
		return nil, fmt.Errorf("failed to determine initial offset: %w", err)
	}
	if !isLatestOffset {
		offset = sources.OffsetEarliest
	}

	namespace := broker.GetNamespace()
	if broker.Spec.Config.Namespace != "" {
		namespace = broker.Spec.Config.Namespace
	}

	secret, err := security.Secret(ctx, &security.AnnotationsSecretLocator{Annotations: broker.Status.Annotations, Namespace: namespace}, security.DefaultSecretProviderFunc(r.SecretLister, r.KubeClient))
	if err != nil {
		return nil, fmt.Errorf("failed to get secret: %w", err)
	}

	bootstrapServers := broker.Status.Annotations[kafka.BootstrapServersConfigMapKey]
	topicName := broker.Status.Annotations[kafka.TopicAnnotation]

	// Existing Triggers might not yet have this annotation
	groupId, ok := trigger.Status.Annotations[kafka.GroupIdAnnotation]
	if !ok {
		groupId = string(trigger.UID)

		// Check if a consumer group exists with the old naming convention
		_, err := r.ConsumerGroupLister.ConsumerGroups(trigger.GetNamespace()).Get(string(trigger.UID))
		if err != nil && !apierrors.IsNotFound(err) {
			return nil, err
		}

		// No existing consumer groups, use new naming
		if apierrors.IsNotFound(err) {
			groupId, err = apisconfig.FromContext(ctx).ExecuteTriggersConsumerGroupTemplate(trigger.ObjectMeta)
			if err != nil {
				return nil, fmt.Errorf("couldn't generate new consumergroup id: %w", err)
			}
		}

		trigger.Status.Annotations[kafka.GroupIdAnnotation] = groupId
	}

	expectedCg := &internalscg.ConsumerGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      groupId,
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
						"group.id":          groupId,
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

	// TODO: make keda annotation values configurable and maybe unexposed
	expectedCg.Annotations = kedafunc.SetAutoscalingAnnotations(trigger.Annotations)

	if secret != nil {
		expectedCg.Spec.Template.Spec.Auth = &internalscg.Auth{
			SecretSpec: &internalscg.SecretSpec{
				Ref: &internalscg.SecretReference{
					Name:      secret.Name,
					Namespace: secret.Namespace,
				},
			},
		}
	}

	if trigger.Status.Auth != nil {
		expectedCg.Spec.OIDCServiceAccountName = trigger.Status.Auth.ServiceAccountName
	}

	cg, err := r.ConsumerGroupLister.ConsumerGroups(trigger.GetNamespace()).Get(groupId) //Get by consumer group name
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

	if equality.Semantic.DeepDerivative(expectedCg.Spec, cg.Spec) && equality.Semantic.DeepDerivative(expectedCg.Annotations, cg.Annotations) {
		return cg, nil
	}

	newCg := &internalscg.ConsumerGroup{
		TypeMeta:   cg.TypeMeta,
		ObjectMeta: cg.ObjectMeta,
		Spec:       expectedCg.Spec,
		Status:     cg.Status,
	}
	newCg.Annotations = expectedCg.Annotations

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

	trigger.Status.PropagateSubscriptionCondition(&apis.Condition{
		Status: corev1.ConditionTrue,
	})
}

func (r *Reconciler) hasRelevantBrokerClass(broker *eventing.Broker) (bool, string) {
	brokerClass := broker.GetAnnotations()[eventing.BrokerClassAnnotationKey]
	return brokerClass == kafka.BrokerClass, brokerClass
}

func deliveryOrderingFromString(val string) (sources.DeliveryOrdering, error) {
	switch strings.ToLower(val) {
	case string(sources.Ordered):
		return sources.Ordered, nil
	case string(sources.Unordered):
		return sources.Unordered, nil
	default:
		return sources.Unordered, fmt.Errorf("invalid annotation %s value: %s. Allowed values [ %q | %q ]", deliveryOrderAnnotation, val, sources.Ordered, sources.Unordered)
	}
}
