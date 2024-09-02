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
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"

	bindings "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/bindings/v1beta1"
	sources "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/sources/v1beta1"

	eventingv1alpha1 "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/eventing/v1alpha1"
)

const (
	DispatcherVolumeName = "contract-resources"
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

// PodBind is a reference to a corev1.Pod.
type PodBind struct {
	PodName      string `json:"podName"`
	PodNamespace string `json:"podNamespace"`
}

type ConsumerSpec struct {
	// Topics is the list of topics to subscribe to.
	Topics []string `json:"topics"`

	// Configs are the Consumer configurations.
	// More info: https://kafka.apache.org/documentation/#consumerconfigs
	Configs ConsumerConfigs `json:"configs,omitempty"`

	// Auth is the auth configuration for the Consumer.
	// +optional
	Auth *Auth `json:"auth,omitempty"`

	// DeliverySpec contains the delivery options for event senders.
	// +optional
	Delivery *DeliverySpec `json:"delivery,omitempty"`

	// Reply is the strategy to handle event replies.
	// +optional
	Reply *ReplyStrategy `json:"reply,omitempty"`

	// Filters is a set of filters.
	// +optional
	Filters *Filters `json:"filters,omitempty"`

	// Subscriber is the addressable that receives events that pass the Filters.
	Subscriber duckv1.Destination `json:"subscriber"`

	// CloudEventOverrides defines overrides to control the output format and
	// modifications of the event sent to the subscriber.
	// +optional
	CloudEventOverrides *duckv1.CloudEventOverrides `json:"ceOverrides,omitempty"`

	// VReplicas is the number of virtual replicas for a consumer.
	VReplicas *int32 `json:"vReplicas"`

	// PodBind represents a reference to the pod in which the consumer should be placed.
	PodBind *PodBind `json:"podBind"`

	// OIDCServiceAccountName is the name of the generated service account
	// used for this components OIDC authentication.
	OIDCServiceAccountName *string `json:"oidcServiceAccountName,omitempty"`
}

type ReplyStrategy struct {
	TopicReply *TopicReply       `json:"topicReply,omitempty"`
	URLReply   *DestinationReply `json:"URLReply,omitempty"`
	NoReply    *NoReply          `json:"noReply,omitempty"`
}

type NoReply struct {
	Enabled bool `json:"enabled"`
}

type TopicReply struct {
	Enabled bool `json:"enabled"`
}

type DestinationReply struct {
	Enabled     bool `json:"enabled"`
	Destination duckv1.Destination
}

type Auth struct {
	NetSpec *bindings.KafkaNetSpec
	// Deprecated, use secret spec
	AuthSpec   *eventingv1alpha1.Auth `json:"AuthSpec,omitempty"`
	SecretSpec *SecretSpec            `json:"SecretSpec,omitempty"`
}

type SecretSpec struct {
	// Secret reference for SASL and SSL configurations.
	Ref *SecretReference `json:"ref,omitempty"`
}

type SecretReference struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

func (a *SecretSpec) HasSecret() bool {
	return a != nil && a.Ref != nil &&
		a.Ref.Name != "" && a.Ref.Namespace != ""
}

type DeliverySpec struct {
	// DeliverySpec is the Knative core delivery spec.
	// DeliverySpec contains the delivery options for event senders.
	*eventingduck.DeliverySpec `json:",inline,omitempty"`

	// Ordering is the ordering of the event delivery.
	Ordering sources.DeliveryOrdering `json:"ordering"`

	// InitialOffset initial offset.
	InitialOffset sources.Offset `json:"initialOffset"`

	// TODO Add rate limiting

	// TODO PT OPT
}

// ConsumerConfigs are the Consumer configurations.
// More info: https://kafka.apache.org/documentation/#consumerconfigs
type ConsumerConfigs struct {
	// +optional
	Configs map[string]string `json:",inline,omitempty"`

	// key-type label value
	// Possible values:
	// - "byte-array"
	// - "string":
	// - "int":
	// - "float":
	//
	// Default value: string
	KeyType *string `json:"keyType,omitempty"`
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

	// SubscriberURI is the resolved URI of the receiver for this Trigger.
	// +optional
	SubscriberURI *apis.URL `json:"subscriberUri,omitempty"`

	// SubscriberCACerts are Certification Authority (CA) certificates in PEM format
	// according to https://www.rfc-editor.org/rfc/rfc7468.
	// +optional
	SubscriberCACerts *string `json:"subscriberCACerts,omitempty"`

	// SubscriberAudience is the OIDC audience for the resolved URI
	// +optional
	SubscriberAudience *string `json:"subscriberAudience,omitempty"`

	// DeliveryStatus contains a resolved URL to the dead letter sink address, and any other
	// resolved delivery options.
	eventingduck.DeliveryStatus `json:",inline"`
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

func (c *Consumer) IsReady() bool {
	return c.Generation == c.Status.ObservedGeneration &&
		c.GetConditionSet().Manage(c.GetStatus()).IsHappy()
}

// ConsumerOption is a functional option for Consumer.
type ConsumerOption func(consumer *Consumer)

// GetConsumerGroup gets the resource reference to the ConsumerGroup
// using the OwnerReference list.
func (c *Consumer) GetConsumerGroup() *metav1.OwnerReference {
	for i, or := range c.OwnerReferences {
		if strings.EqualFold(or.Kind, ConsumerGroupGroupVersionKind.Kind) {
			return &c.OwnerReferences[i]
		}
	}
	return nil
}

func (c Consumer) HasDeadLetterSink() bool {
	return hasDeadLetterSink(c.Spec.Delivery)
}

type ByReadinessAndCreationTime []*Consumer

func (consumers ByReadinessAndCreationTime) Len() int {
	return len(consumers)
}

func (consumers ByReadinessAndCreationTime) Less(i, j int) bool {
	c, other := consumers[i], consumers[j]

	// Prefer ready instances.
	if c.IsReady() {
		return true
	}
	if other.IsReady() {
		return false
	}
	// Prefer older instances.
	return c.CreationTimestamp.Time.Before(other.CreationTimestamp.Time)
}

func (consumers ByReadinessAndCreationTime) Swap(i, j int) {
	tmp := consumers[i]
	consumers[i] = consumers[j]
	consumers[j] = tmp
}
