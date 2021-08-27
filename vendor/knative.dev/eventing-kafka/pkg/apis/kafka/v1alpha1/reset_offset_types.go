/*
Copyright 2021 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	"time"

	"github.com/Shopify/sarama"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/kmeta"
)

const (
	OffsetEarliest = "earliest"
	OffsetLatest   = "latest"
)

// +genclient
// +genreconciler
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ResetOffset is a resource representing a "command" for re-positioning the
// offsets of a specific Kafka resource (Subscription, Trigger, etc.)
type ResetOffset struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Spec defines the desired state of the ResetOffset.
	Spec ResetOffsetSpec `json:"spec,omitempty"`

	// Status represents the current state of the ResetOffset.
	// This data may be out of date.
	// +optional
	Status ResetOffsetStatus `json:"status,omitempty"`
}

var (
	// Check that this resource can be validated and defaulted.
	_ apis.Validatable = (*ResetOffset)(nil)
	_ apis.Defaultable = (*ResetOffset)(nil)

	_ runtime.Object = (*ResetOffset)(nil)

	// Check that we can create OwnerReferences to an this resource.
	_ kmeta.OwnerRefable = (*ResetOffset)(nil)

	// Check that the type conforms to the duck Knative Resource shape.
	_ duckv1.KRShaped = (*ResetOffset)(nil)
)

// ResetOffsetSpec defines the specification for a ResetOffset.
type ResetOffsetSpec struct {

	// Offset is an object representing the desired offset position to which all partitions
	// will be reset. It provides for future extensibility in supporting various types of
	// offset information (time based, explicit offset numbers, etc.)
	Offset OffsetSpec `json:"offset"`

	// Ref is a KReference specifying the Knative resource, related to a Kafka ConsumerGroup,
	// whose partitions offsets will be reset (e.g. Subscription, Trigger, etc.)  The referenced
	// object MUST be related to a Kafka Topic in such a way that it's specific partitions
	// can be identified.  Thus, even though the KReference is a wide-open type, it is up
	// to the user to provide an appropriate value as supported by the Controller in question
	// (KafkaChannel vs KafkaBroker, etc).  Failure to provide a valid value will result in
	// the ResetOffset operation being rejected as failed.
	Ref duckv1.KReference `json:"ref"`
}

// OffsetSpec defines the intended values to move the offsets to.
// Note: This simple wrapper might seem unnecessary, but is provided to allow future extension
//       in order to support specifying explicit offset (int64) values for each Partition.
type OffsetSpec struct {

	// Time is a string representing the desired offset position to which all partitions
	// will be reset.  Supported values include "earliest", "latest", or a valid date / time
	// string in the time.RFC3339 format. The "earliest" and "latest" values indicate the
	// beginning and end, respectively, of the persistence window of the Topic.  There is no
	// default value, and invalid values will result in the ResetOffset operation being
	// rejected as failed.
	Time string `json:"time"`
}

// IsOffsetEarliest returns True if the Offset value is "earliest"
func (ros *ResetOffsetSpec) IsOffsetEarliest() bool {
	return ros.Offset.Time == OffsetEarliest
}

// IsOffsetLatest returns True if the Offset value is "latest"
func (ros *ResetOffsetSpec) IsOffsetLatest() bool {
	return ros.Offset.Time == OffsetLatest
}

// ParseOffsetTime returns the parsed Offset Time if valid (RFC3339 format) or an error for invalid content.
func (ros *ResetOffsetSpec) ParseOffsetTime() (time.Time, error) {
	return time.Parse(time.RFC3339, ros.Offset.Time)
}

// ParseSaramaOffsetTime returns the Sarama Offset Time (millis since epoch) int64 as parsed from the specified ResetOffset.Spec
func (ros *ResetOffsetSpec) ParseSaramaOffsetTime() (int64, error) {
	var saramaOffsetTime int64
	if ros.IsOffsetEarliest() {
		saramaOffsetTime = sarama.OffsetOldest
	} else if ros.IsOffsetLatest() {
		saramaOffsetTime = sarama.OffsetNewest
	} else {
		offsetTime, err := ros.ParseOffsetTime()
		if err != nil {
			return 0, err
		}
		saramaOffsetTime = offsetTime.UnixNano() / 1000000 // Convert Nanos To Millis For Sarama
	}
	return saramaOffsetTime, nil
}

// ResetOffsetStatus represents the current state of a ResetOffset.
type ResetOffsetStatus struct {

	// Topic is a string representing the Kafka Topic name associated with the ResetOffsetSpec.Ref
	// +optional
	Topic string `json:"topic,omitempty"`

	// Group is a string representing the Kafka ConsumerGroup ID associated with the ResetOffsetSpec.Ref
	// +optional
	Group string `json:"group,omitempty"`

	// Partitions is an array of OffsetMapping structs which represent the Offsets (old / new) of
	// all Kafka Partitions associated with the ResetOffsetSpec.Ref
	// +optional
	Partitions []OffsetMapping `json:"partitions,omitempty"`

	// inherits duck/v1 Status, which currently provides:
	// * ObservedGeneration - the 'Generation' of the Service that was last processed by the controller.
	// * Conditions - the latest available observations of a resource's current state.
	// * Annotations - optional status information to be conveyed to users.
	duckv1.Status `json:",inline"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ResetOffsetList is a collection of ResetOffsets.
type ResetOffsetList struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ResetOffset `json:"items"`
}

// GetGroupVersionKind returns GroupVersionKind for ResetOffset
func (ro *ResetOffset) GetGroupVersionKind() schema.GroupVersionKind {
	return SchemeGroupVersion.WithKind("ResetOffset")
}

// GetStatus retrieves the duck status for this resource. Implements the KRShaped interface.
func (ro *ResetOffset) GetStatus() *duckv1.Status {
	return &ro.Status.Status
}

// OffsetMapping represents a single Kafka Partition's Offset values before and after repositioning.
type OffsetMapping struct {
	Partition int32 `json:"partition"`
	OldOffset int64 `json:"oldOffset"`
	NewOffset int64 `json:"newOffset"`
}
