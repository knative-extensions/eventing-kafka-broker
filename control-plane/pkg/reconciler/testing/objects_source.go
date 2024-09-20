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

package testing

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/pointer"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/eventing/pkg/apis/feature"
	"knative.dev/eventing/pkg/eventingtls/eventingtlstesting"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/apis/bindings/v1beta1"
	sources "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/sources/v1beta1"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
)

const (
	SourceName                      = "ks"
	SourceNamespace                 = "test-ns"
	SourceConsumerGroup             = "ks-group"
	SourceUUID                      = "uuid"
	SourceBootstrapServers          = "kafka:9092"
	SourceDeliverySpecRetry         = 3
	SourceDeliverySpecBackoffPolicy = eventingduck.BackoffPolicyExponential
	SourceDeliverySpecBackoffDelay  = "PT5S"
	SourceDeliverySpecTimeout       = "PT24H"
)

var (
	SourceTopics = []string{"t1", "t2"}
)

func NewSource(options ...KRShapedOption) *sources.KafkaSource {
	s := &sources.KafkaSource{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: SourceNamespace,
			Name:      SourceName,
			UID:       SourceUUID,
		},
		Spec: sources.KafkaSourceSpec{
			KafkaAuthSpec: v1beta1.KafkaAuthSpec{
				BootstrapServers: []string{SourceBootstrapServers},
				Net:              v1beta1.KafkaNetSpec{},
			},
			Topics:        SourceTopics,
			ConsumerGroup: SourceConsumerGroup,
			SourceSpec: duckv1.SourceSpec{
				Sink: NewSourceSinkReference(),
			},
		},
	}

	for _, opt := range options {
		opt(s)
	}
	return s
}

func NewDeletedSource(options ...KRShapedOption) runtime.Object {
	return NewSource(
		append(
			options,
			WithDeletedTimeStamp,
		)...,
	)
}

func WithKeyType(keyType string) KRShapedOption {
	return func(obj duckv1.KRShaped) {
		ks := obj.(*sources.KafkaSource)
		if ks.Labels == nil {
			ks.Labels = make(map[string]string, 1)
		}
		ks.Labels[sources.KafkaKeyTypeLabel] = keyType
	}
}

func WithDeliverySpec() KRShapedOption {
	return func(obj duckv1.KRShaped) {
		ks := obj.(*sources.KafkaSource)
		if ks.Spec.Delivery == nil {
			backoffPolicy := SourceDeliverySpecBackoffPolicy
			ks.Spec.Delivery = &eventingduck.DeliverySpec{
				Retry:         pointer.Int32(SourceDeliverySpecRetry),
				BackoffPolicy: &backoffPolicy,
				BackoffDelay:  pointer.String(SourceDeliverySpecBackoffDelay),
				Timeout:       pointer.String(SourceDeliverySpecTimeout),
			}
		}
		ks.Spec.Delivery.DeadLetterSink = &duckv1.Destination{URI: ConsumerDeadLetterSinkURI}
	}
}

func WithCloudEventOverrides(overrides *duckv1.CloudEventOverrides) KRShapedOption {
	return func(obj duckv1.KRShaped) {
		ks := obj.(*sources.KafkaSource)
		ks.Spec.CloudEventOverrides = overrides
	}
}

func WithAutoscalingAnnotationsSource() KRShapedOption {
	return func(obj duckv1.KRShaped) {
		ks := obj.(*sources.KafkaSource)

		if ks.Annotations == nil {
			ks.Annotations = make(map[string]string)
		}

		for k, v := range ConsumerGroupAnnotations {
			if _, ok := ks.Annotations[k]; !ok {
				ks.Annotations[k] = v
			}
		}
	}
}

func NewSourceSinkObject() *corev1.Service {
	return NewService()
}

func NewSourceSinkReference() duckv1.Destination {
	s := NewService()
	return duckv1.Destination{
		Ref: &duckv1.KReference{
			Kind:       s.Kind,
			Namespace:  s.Namespace,
			Name:       s.Name,
			APIVersion: s.APIVersion,
		},
	}
}

func NewSourceSinkReferenceWithCACert() duckv1.Destination {
	s := NewService()
	return duckv1.Destination{
		Ref: &duckv1.KReference{
			Kind:       s.Kind,
			Namespace:  s.Namespace,
			Name:       s.Name,
			APIVersion: s.APIVersion,
		},
		CACerts: pointer.String(string(eventingtlstesting.CA)),
	}
}

func NewSourceSink2Reference() duckv1.Destination {
	s := NewService2()
	return duckv1.Destination{
		Ref: &duckv1.KReference{
			Kind:       s.Kind,
			Namespace:  s.Namespace,
			Name:       s.Name,
			APIVersion: s.APIVersion,
		},
	}
}

func WithSourceSink(d duckv1.Destination) KRShapedOption {
	return func(obj duckv1.KRShaped) {
		s := obj.(*sources.KafkaSource)
		s.Spec.Sink = d
	}
}

func WithSourceConsumers(replicas int32) KRShapedOption {
	return func(obj duckv1.KRShaped) {
		s := obj.(*sources.KafkaSource)
		s.Spec.Consumers = pointer.Int32(replicas)
	}
}

func WithInitialOffset(offset sources.Offset) KRShapedOption {
	return func(obj duckv1.KRShaped) {
		s := obj.(*sources.KafkaSource)
		s.Spec.InitialOffset = offset
	}
}

func WithOrdering(ordering sources.DeliveryOrdering) KRShapedOption {
	return func(obj duckv1.KRShaped) {
		s := obj.(*sources.KafkaSource)
		s.Spec.Ordering = &ordering
	}
}

func SourceDispatcherPod(namespace string, annotations map[string]string) runtime.Object {
	return &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        "kafka-source-dispatcher",
			Namespace:   namespace,
			Annotations: annotations,
			Labels: map[string]string{
				"app":                    base.SourceDispatcherLabel,
				"app.kubernetes.io/kind": "kafka-dispatcher",
			},
		},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
}

func InitSourceConditions(obj duckv1.KRShaped) {
	sink := obj.(*sources.KafkaSource)
	sink.Status.InitializeConditions()
}

func StatusSourceSinkResolved(uri string) KRShapedOption {
	return func(obj duckv1.KRShaped) {
		ks := obj.(*sources.KafkaSource)
		res, _ := apis.ParseURL(uri)
		ks.Status.SinkURI = res
		if !res.IsEmpty() {
			ks.GetConditionSet().Manage(ks.GetStatus()).MarkTrue(sources.KafkaConditionSinkProvided)
		} else {
			ks.GetConditionSet().Manage(ks.GetStatus()).MarkUnknown(sources.KafkaConditionSinkProvided, "SinkEmpty", "Sink has resolved to empty.%s", "")
		}
	}
}

func StatusSourceSinkNotResolved(err string) KRShapedOption {
	return func(obj duckv1.KRShaped) {
		ks := obj.(*sources.KafkaSource)
		ks.Status.SinkURI = nil
		ks.GetConditionSet().Manage(ks.GetStatus()).MarkFalse(
			sources.KafkaConditionSinkProvided,
			"FailedToResolveSink",
			err,
		)
	}
}

func StatusSourceOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled() KRShapedOption {
	return func(obj duckv1.KRShaped) {
		ks := obj.(*sources.KafkaSource)
		ks.Status.MarkOIDCIdentityCreatedSucceededWithReason(fmt.Sprintf("%s feature disabled", feature.OIDCAuthentication), "")
	}
}

func StatusSourceOIDCIdentityCreatedSucceeded() KRShapedOption {
	return func(obj duckv1.KRShaped) {
		ks := obj.(*sources.KafkaSource)
		ks.Status.MarkOIDCIdentityCreatedSucceeded()
	}
}

func StatusSourceOIDCIdentity(saName string) KRShapedOption {
	return func(obj duckv1.KRShaped) {
		ks := obj.(*sources.KafkaSource)
		ks.Status.Auth = &duckv1.AuthStatus{
			ServiceAccountName: &saName,
		}
	}
}

func SourceReference() *contract.Reference {
	return &contract.Reference{
		Namespace: SourceNamespace,
		Name:      SourceName,
		Uuid:      SourceUUID,
	}
}

func SourceAsOwnerReference() metav1.OwnerReference {
	return metav1.OwnerReference{
		APIVersion:         sources.SchemeGroupVersion.String(),
		Kind:               "KafkaSource",
		Name:               SourceName,
		UID:                SourceUUID,
		Controller:         pointer.Bool(true),
		BlockOwnerDeletion: pointer.Bool(true),
	}
}
