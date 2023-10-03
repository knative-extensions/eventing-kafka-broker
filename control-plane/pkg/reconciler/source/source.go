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

package source

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/utils/pointer"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/kmeta"
	"knative.dev/pkg/reconciler"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/apis/config"
	sources "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/sources/v1beta1"

	internalscg "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing/v1alpha1"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/autoscaler/keda"
	internalsclient "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/clientset/versioned"
	internalslst "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/listers/eventing/v1alpha1"

	kedaclientset "knative.dev/eventing-kafka-broker/third_party/pkg/client/clientset/versioned"
)

const (
	DefaultDeliveryOrder = sources.Ordered

	KafkaConditionConsumerGroup apis.ConditionType = "ConsumerGroup" //condition is registered by controller
)

var (
	conditionSet = apis.NewLivingConditionSet(
		KafkaConditionConsumerGroup,
		sources.KafkaConditionSinkProvided,
	)
)

type Reconciler struct {
	ConsumerGroupLister internalslst.ConsumerGroupLister
	InternalsClient     internalsclient.Interface
	KedaClient          kedaclientset.Interface
	KafkaFeatureFlags   *config.KafkaFeatureFlags
}

func (r *Reconciler) ReconcileKind(ctx context.Context, ks *sources.KafkaSource) reconciler.Event {

	selector, err := GetLabelsAsSelector(ks.Name)
	if err != nil {
		return fmt.Errorf("getting labels as selector: %v", err)
	}
	ks.Status.Selector = selector.String()

	cg, err := r.reconcileConsumerGroup(ctx, ks)
	if err != nil {
		ks.GetConditionSet().Manage(&ks.Status).MarkFalse(KafkaConditionConsumerGroup, "failed to reconcile consumer group", err.Error())
		return err
	}

	propagateConsumerGroupStatus(cg, ks)

	return nil
}

func GetLabelsAsSelector(name string) (labels.Selector, error) {
	labels := GetLabels(name)
	var labelSelector metav1.LabelSelector
	labelSelector.MatchLabels = labels
	selector, err := metav1.LabelSelectorAsSelector(&labelSelector)
	return selector, err
}

func GetLabels(name string) map[string]string {
	return map[string]string{
		"eventing.knative.dev/source":     "kafka-source-controller",
		"eventing.knative.dev/sourceName": name,
	}
}

// Need to have an empty definition here to ensure that we can delete older sources which had a finalizer
func (r Reconciler) FinalizeKind(ctx context.Context, ks *sources.KafkaSource) reconciler.Event {
	return nil
}

func (r Reconciler) reconcileConsumerGroup(ctx context.Context, ks *sources.KafkaSource) (*internalscg.ConsumerGroup, error) {
	var deliverySpec *internalscg.DeliverySpec
	deliveryOrder := DefaultDeliveryOrder
	if ks.Spec.Ordering != nil {
		deliveryOrder = *ks.Spec.Ordering
	}
	if ks.Spec.Delivery != nil {
		deliverySpec = &internalscg.DeliverySpec{
			InitialOffset: ks.Spec.InitialOffset,
			DeliverySpec:  ks.Spec.Delivery.DeepCopy(),
			Ordering:      deliveryOrder,
		}
	} else {
		backoffPolicy := eventingduck.BackoffPolicyExponential
		deliverySpec = &internalscg.DeliverySpec{
			InitialOffset: ks.Spec.InitialOffset,
			DeliverySpec: &eventingduck.DeliverySpec{
				Retry:         pointer.Int32(10),
				BackoffPolicy: &backoffPolicy,
				BackoffDelay:  pointer.String("PT0.3S"),
				Timeout:       pointer.String("PT600S"),
			},
			Ordering: deliveryOrder,
		}
	}

	expectedCg := &internalscg.ConsumerGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      string(ks.UID),
			Namespace: ks.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*kmeta.NewControllerRef(ks),
			},
			Labels: map[string]string{
				internalscg.UserFacingResourceLabelSelector: strings.ToLower(ks.GetGroupVersionKind().Kind),
			},
			Finalizers: []string{
				"consumergroups.internal.kafka.eventing.knative.dev",
			},
		},
		Spec: internalscg.ConsumerGroupSpec{
			Replicas: ks.Spec.Consumers,
			Template: internalscg.ConsumerTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						internalscg.ConsumerLabelSelector: string(ks.UID),
					},
				},
				Spec: internalscg.ConsumerSpec{
					Topics: ks.Spec.Topics,
					Configs: internalscg.ConsumerConfigs{Configs: map[string]string{
						"group.id":          ks.Spec.ConsumerGroup,
						"bootstrap.servers": strings.Join(ks.Spec.BootstrapServers, ","),
					}},
					Auth: &internalscg.Auth{
						NetSpec: &ks.Spec.KafkaAuthSpec.Net,
					},
					Delivery:   deliverySpec,
					Subscriber: ks.Spec.Sink,
					Reply:      &internalscg.ReplyStrategy{NoReply: &internalscg.NoReply{Enabled: true}},
				},
			},
		},
	}

	if ks.Spec.CloudEventOverrides != nil {
		expectedCg.Spec.Template.Spec.CloudEventOverrides = &duckv1.CloudEventOverrides{
			Extensions: ks.Spec.CloudEventOverrides.Extensions,
		}
	}

	if kt, ok := ks.Labels[sources.KafkaKeyTypeLabel]; ok && len(kt) > 0 {
		expectedCg.Spec.Template.Spec.Configs.KeyType = &kt
	}

	// TODO: make keda annotation values configurable and maybe unexposed
	expectedCg.Annotations = keda.SetAutoscalingAnnotations(ks.Annotations)

	// If KEDA is enabled, then we ignore KafkaSource replicas setting
	if keda.IsEnabled(ctx, r.KafkaFeatureFlags, r.KedaClient, ks) {
		expectedCg.Spec.Replicas = nil
	}

	cg, err := r.ConsumerGroupLister.ConsumerGroups(ks.GetNamespace()).Get(string(ks.UID)) //Get by consumer group id
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

func propagateConsumerGroupStatus(cg *internalscg.ConsumerGroup, ks *sources.KafkaSource) {
	if cg.IsReady() {
		ks.GetConditionSet().Manage(&ks.Status).MarkTrue(KafkaConditionConsumerGroup)
	} else {
		topLevelCondition := cg.GetConditionSet().Manage(cg.GetStatus()).GetTopLevelCondition()
		if topLevelCondition == nil {
			ks.GetConditionSet().Manage(&ks.Status).MarkUnknown(
				KafkaConditionConsumerGroup,
				"failed to reconcile consumer group",
				"consumer group is not ready",
			)
		} else {
			ks.GetConditionSet().Manage(&ks.Status).MarkFalse(
				KafkaConditionConsumerGroup,
				topLevelCondition.Reason,
				topLevelCondition.Message,
			)
		}
	}
	ks.Status.MarkSink(cg.Status.SubscriberURI)
	ks.Status.Placeable = cg.Status.Placeable
	if cg.Status.Replicas != nil {
		ks.Status.Consumers = *cg.Status.Replicas
	}
}
