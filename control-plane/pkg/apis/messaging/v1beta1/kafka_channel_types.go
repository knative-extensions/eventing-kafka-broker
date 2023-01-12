/*
Copyright 2020 The Knative Authors

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

package v1beta1

import (
	"time"

	"github.com/rickb777/date/period"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/kmeta"
)

// +genclient
// +genreconciler
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// KafkaChannel is a resource representing a Kafka Channel.
type KafkaChannel struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Spec defines the desired state of the Channel.
	Spec KafkaChannelSpec `json:"spec,omitempty"`

	// Status represents the current state of the KafkaChannel. This data may be out of
	// date.
	// +optional
	Status KafkaChannelStatus `json:"status,omitempty"`
}

var (
	// Check that this KafkaChannel can be validated and defaulted.
	_ apis.Validatable = (*KafkaChannel)(nil)
	_ apis.Defaultable = (*KafkaChannel)(nil)

	_ runtime.Object = (*KafkaChannel)(nil)

	// Check that we can create OwnerReferences to a KafkaChannel.
	_ kmeta.OwnerRefable = (*KafkaChannel)(nil)

	// Check that the type conforms to the duck Knative Resource shape.
	_ duckv1.KRShaped = (*KafkaChannel)(nil)
)

// KafkaChannelSpec defines the specification for a KafkaChannel.
type KafkaChannelSpec struct {
	// NumPartitions is the number of partitions of a Kafka topic. By default, it is set to 1.
	NumPartitions int32 `json:"numPartitions"`

	// ReplicationFactor is the replication factor of a Kafka topic. By default, it is set to 1.
	ReplicationFactor int16 `json:"replicationFactor"`

	// RetentionDuration is the duration for which events will be retained in the Kafka Topic.
	// By default, it is set to 168 hours, which is the precise form for 7 days.
	// More information on Duration format:
	//  - https://www.iso.org/iso-8601-date-and-time-format.html
	//  - https://en.wikipedia.org/wiki/ISO_8601
	RetentionDuration string `json:"retentionDuration"`

	// Channel conforms to Duck type Channelable.
	eventingduck.ChannelableSpec `json:",inline"`
}

// ParseRetentionDuration returns the parsed Offset Time if valid (RFC3339 format) or an error for invalid content.
// Note - If the optional RetentionDuration field is not present, or is invalid, a Duration of "-1" will be returned.
func (kcs *KafkaChannelSpec) ParseRetentionDuration() (time.Duration, error) {
	retentionPeriod, err := period.Parse(kcs.RetentionDuration)
	if err != nil {
		return time.Duration(-1), err
	}
	retentionDuration, _ := retentionPeriod.Duration() // Ignore precision flag and accept ISO8601 estimation
	return retentionDuration, nil
}

// KafkaChannelStatus represents the current state of a KafkaChannel.
type KafkaChannelStatus struct {
	// Channel conforms to Duck type ChannelableStatus.
	eventingduck.ChannelableStatus `json:",inline"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// KafkaChannelList is a collection of KafkaChannels.
type KafkaChannelList struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KafkaChannel `json:"items"`
}

// GetGroupVersionKind returns GroupVersionKind for KafkaChannels
func (kc *KafkaChannel) GetGroupVersionKind() schema.GroupVersionKind {
	return SchemeGroupVersion.WithKind("KafkaChannel")
}

// GetStatus retrieves the duck status for this resource. Implements the KRShaped interface.
func (kc *KafkaChannel) GetStatus() *duckv1.Status {
	return &kc.Status.Status
}
