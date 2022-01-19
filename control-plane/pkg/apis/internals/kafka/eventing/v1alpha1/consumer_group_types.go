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
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"

	eventingduckv1 "knative.dev/eventing/pkg/apis/duck/v1"
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

// GetKey implements scheduler.VPod interface.
func (cg *ConsumerGroup) GetKey() types.NamespacedName {
	return types.NamespacedName{
		Namespace: cg.GetNamespace(),
		Name:      cg.GetName(),
	}
}

// GetVReplicas implements scheduler.VPod interface.
func (cg *ConsumerGroup) GetVReplicas() int32 {
	return *cg.Spec.Replicas
}

// GetPlacements implements scheduler.VPod interface.
func (cg *ConsumerGroup) GetPlacements() []eventingduckv1alpha1.Placement {
	return cg.Status.Placements
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

	// SubscriberURI is the resolved URI of the receiver for this Trigger.
	// +optional
	SubscriberURI *apis.URL `json:"subscriberUri,omitempty"`

	// DeliveryStatus contains a resolved URL to the dead letter sink address, and any other
	// resolved delivery options.
	eventingduckv1.DeliveryStatus `json:",inline"`

	// Replicas is the latest observed number of replicas of the given Template.
	// These are replicas in the sense that they are instantiations of the
	// same Template, but individual replicas also have a consistent identity.
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`
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

// GetStatus retrieves the status of the ConsumerGroup. Implements the KRShaped interface.
func (c *ConsumerGroup) GetStatus() *duckv1.Status {
	return &c.Status.Status
}

// ConsumerFromTemplate returns a Consumer from the Consumer template in the ConsumerGroup spec.
func (cg *ConsumerGroup) ConsumerFromTemplate(options ...ConsumerOption) *Consumer {
	// TODO figure out naming strategy, is generateName enough?
	c := &Consumer{
		ObjectMeta: cg.Spec.Template.ObjectMeta,
		Spec:       cg.Spec.Template.Spec,
	}

	ownerRef := metav1.NewControllerRef(cg, ConsumerGroupGroupVersionKind)
	c.OwnerReferences = append(c.OwnerReferences, *ownerRef)

	for _, opt := range options {
		opt(c)
	}
	return c
}

func (cg *ConsumerGroup) IsReady() bool {
	return cg.Generation == cg.Status.ObservedGeneration &&
		cg.GetConditionSet().Manage(cg.GetStatus()).IsHappy()
}

// GetUserFacingResourceRef gets the resource reference to the user-facing resources
// that are backed by this ConsumerGroup using the OwnerReference list.
func (cg *ConsumerGroup) GetUserFacingResourceRef() *metav1.OwnerReference {
	for i, or := range cg.OwnerReferences {
		// TODO hardcoded resource kinds.
		if strings.EqualFold(or.Kind, "trigger") ||
			strings.EqualFold(or.Kind, "kafkasource") ||
			strings.EqualFold(or.Kind, "kafkachannel") {
			return &cg.OwnerReferences[i]
		}
	}
	return nil
}
