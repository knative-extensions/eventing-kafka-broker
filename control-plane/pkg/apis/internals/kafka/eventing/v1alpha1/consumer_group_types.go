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
	duckv1 "knative.dev/pkg/apis/duck/v1"

	eventingduckv1alpha1 "knative.dev/eventing/pkg/apis/duck/v1alpha1"
)

// +genclient
// +genclient:method=GetScale,verb=get,subresource=scale,result=k8s.io/api/autoscaling/v1.Scale
// +genreconciler
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type ConsumerGroup struct {
	metav1.TypeMeta `json:",inline"`

	// Standard object's metadata.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Specification of the desired behavior of the ConsumerGroup.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Spec ConsumerGroupSpec `json:"spec,omitempty"`

	// Most recently observed status of the ConsumerGroup.
	// This data may not be up-to-date.
	// Populated by the system.
	// Read-only.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Status ConsumerGroupStatus `json:"status,omitempty"`
}

type ConsumerGroupSpec struct {

	// Template is the object that describes the consumer that will be created if
	// insufficient replicas are detected. Each consumer stamped out by the
	// ConsumerGroup will fulfill this Template, but have a unique identity from
	// the rest of the ConsumerGroup.
	Template ConsumerTemplateSpec `json:"template"`

	// Replicas is the desired number of replicas of the given Template.
	// These are replicas in the sense that they are instantiations of the
	// same Template, but individual replicas also have a consistent identity.
	// If unspecified, defaults to 1.
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`

	// Selector is a label query over consumers that should match the Replicas count.
	// If Selector is empty, it is defaulted to the labels present on the template.
	// Label keys and values that must match in order to be controlled by this
	// controller, if empty defaulted to labels on template.
	// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#label-selectors
	// +optional
	Selector map[string]string `json:"selector,omitempty" protobuf:"bytes,2,rep,name=selector"`
}

type ConsumerGroupStatus struct {
	// inherits duck/v1 Status, which currently provides:
	// * ObservedGeneration - the 'Generation' of the ConsumerGroup that was last processed by the controller.
	// * Conditions - the latest available observations of a resource's current state.
	duckv1.Status

	// inherits PlaceableStatus Status
	eventingduckv1alpha1.PlaceableStatus `json:",inline"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ConsumerGroupList defines a list of consumer groups.
type ConsumerGroupList struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []ConsumerGroup `json:"items"`
}

// GetGroupVersionKind returns GroupVersionKind for ConsumerGroup.
func (c *ConsumerGroup) GetGroupVersionKind() schema.GroupVersionKind {
	return SchemeGroupVersion.WithKind("ConsumerGroup")
}

// GetUntypedSpec returns the spec of the ConsumerGroup.
func (c *ConsumerGroup) GetUntypedSpec() interface{} {
	return c.Spec
}

// GetStatus retrieves the status of the ConsumerGroupt. Implements the KRShaped interface.
func (c *ConsumerGroup) GetStatus() *duckv1.Status {
	return &c.Status.Status
}
