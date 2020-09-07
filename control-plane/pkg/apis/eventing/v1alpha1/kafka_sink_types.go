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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
)

const (
	// CloudEvents binary content mode.
	ModeBinary = "binary"
	// CloudEvents structured content mode.
	ModeStructured = "structured"
)

var allowedContentModes = sets.NewString(ModeStructured, ModeBinary)

// +genclient
// +genreconciler
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// KafkaSink is an addressable resource that represent a Kafka topic.
type KafkaSink struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Spec defines the desired state of the Kafka Sink.
	Spec KafkaSinkSpec `json:"spec,omitempty"`

	// Status represents the current state of the KafkaSink.
	// This data may be out of date.
	// +optional
	Status KafkaSinkStatus `json:"status,omitempty"`
}

// Check that Channel can be validated, can be defaulted, and has immutable fields.
var _ apis.Validatable = (*KafkaSink)(nil)
var _ apis.Defaultable = (*KafkaSink)(nil)
var _ runtime.Object = (*KafkaSink)(nil)
var _ duckv1.KRShaped = (*KafkaSink)(nil)
var _ apis.Convertible = (*KafkaSink)(nil)

// KafkaSinkSpec defines the desired state of the Kafka Sink.
type KafkaSinkSpec struct {

	// Topic name to send events.
	Topic string `json:"topic"`

	// Number of topic partitions.
	// +optional
	NumPartitions *int32 `json:"numPartitions,omitempty"`

	// Topic replication factor
	// +optional
	ReplicationFactor *int16 `json:"replicationFactor,omitempty"`

	// Kafka Broker bootstrap servers.
	BootstrapServers string `json:"bootstrapServers"`

	// CloudEvent content mode of Kafka messages sent to the topic.
	// Possible values:
	// - structured
	// - binary
	//
	// - default: structured.
	//
	// - https://github.com/cloudevents/spec/blob/v1.0/spec.md#message
	//	 - https://github.com/cloudevents/spec/blob/v1.0/kafka-protocol-binding.md#33-structured-content-mode
	//	 - https://github.com/cloudevents/spec/blob/v1.0/kafka-protocol-binding.md#32-binary-content-mode'
	//
	// +optional
	ContentMode *string `json:"contentMode,omitempty"`
}

// KafkaSinkStatus represents the current state of the KafkaSink.
type KafkaSinkStatus struct {
	// inherits duck/v1 Status, which currently provides:
	// * ObservedGeneration - the 'Generation' of the Kafka Sink that was last processed by the controller.
	// * Conditions - the latest available observations of a resource's current state.
	duckv1.Status `json:",inline"`

	// Kafka Sink is Addressable.
	Address duckv1.Addressable `json:"address,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// KafkaSinkList defines a list of Kafka Sink.
type KafkaSinkList struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []KafkaSink `json:"items"`
}

// GetGroupVersionKind returns GroupVersionKind for KafkaSinks.
func (ks *KafkaSink) GetGroupVersionKind() schema.GroupVersionKind {
	return SchemeGroupVersion.WithKind("KafkaSink")
}

// GetUntypedSpec returns the spec of the Kafka Sink.
func (ks *KafkaSink) GetUntypedSpec() interface{} {
	return ks.Spec
}

// GetStatus retrieves the status of the Kafka Sink. Implements the KRShaped interface.
func (ks *KafkaSink) GetStatus() *duckv1.Status {
	return &ks.Status.Status
}
