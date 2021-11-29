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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	duckv1 "knative.dev/pkg/apis/duck/v1"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing"
)

// +genclient
// +genreconciler
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type Consumer struct {
	metav1.TypeMeta `json:",inline"`

	// Standard object's metadata.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Specification of the desired behavior of the consumer.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	Spec ConsumerSpec `json:"spec"`

	// Most recently observed status of the Consumer.
	// This data may not be up-to-date.
	// Populated by the system.
	// Read-only.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Status ConsumerStatus `json:"status,omitempty"`
}

type ConsumerSpec struct {
	// Topics is the list of topics to subscribe to.
	Topics []string `json:"topics"`

	// Configs are the Consumer configurations.
	// More info: https://kafka.apache.org/documentation/#consumerconfigs
	Configs ConsumerConfigs `json:"configs,omitempty"`

	// TODO Add auth

	// DeliverySpec contains the delivery options for event senders.
	// +optional
	Delivery *DeliverySpec `json:"delivery,omitempty"`

	// Filters is a set of filters.
	// +optional
	Filters *Filters `json:"filters,omitempty"`

	// Subscriber is the addressable that receives events that pass the Filters.
	Subscriber duckv1.Destination `json:"subscriber"`
}

type DeliverySpec struct {
	// DeliverySpec is the Knative core delivery spec.
	// DeliverySpec contains the delivery options for event senders.
	eventingduck.DeliverySpec `json:",inline,omitempty"`

	// Ordering is the ordering of the event delivery.
	Ordering eventing.DeliveryOrdering `json:"ordering"`

	// TODO Add rate limiting

	// TODO PT OPT
}

// ConsumerConfigs are the Consumer configurations.
// More info: https://kafka.apache.org/documentation/#consumerconfigs
type ConsumerConfigs struct {
	// +optional
	Configs map[string]string `json:",inline,omitempty"`
}

// ConsumerTemplateSpec describes the data a consumer should have when created from a template.
type ConsumerTemplateSpec struct {

	// Standard object's metadata.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Specification of the desired behavior of the consumer.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Spec ConsumerSpec `json:"spec,omitempty"`
}

type ConsumerStatus struct {
	// inherits duck/v1 Status, which currently provides:
	// * ObservedGeneration - the 'Generation' of the Consumer that was last processed by the controller.
	// * Conditions - the latest available observations of a resource's current state.
	duckv1.Status
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ConsumerList defines a list of Consumers.
type ConsumerList struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []Consumer `json:"items"`
}

// GetGroupVersionKind returns GroupVersionKind for Consumer.
func (c *Consumer) GetGroupVersionKind() schema.GroupVersionKind {
	return SchemeGroupVersion.WithKind("Consumer")
}

// GetUntypedSpec returns the spec of the Consumer.
func (c *Consumer) GetUntypedSpec() interface{} {
	return c.Spec
}

// GetStatus retrieves the status of the Consumer. Implements the KRShaped interface.
func (c *Consumer) GetStatus() *duckv1.Status {
	return &c.Status.Status
}
