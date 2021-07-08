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
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
)

// ConditionAddressable is a reference to the Addressable condition in the base package.
// KafkaChannel uses the common Condition set defined in the base package.
const (
	ConditionAddressable apis.ConditionType = "Addressable"
)

var conditionSet apis.ConditionSet

func RegisterConditionSet(cs apis.ConditionSet) {
	conditionSet = cs
}

// GetConditionSet retrieves the condition set for this resource. Implements the KRShaped interface.
func (*KafkaChannel) GetConditionSet() apis.ConditionSet {
	return conditionSet
}

// GetConditionSet retrieves the condition set for this resource.
func (*KafkaChannelStatus) GetConditionSet() apis.ConditionSet {
	return conditionSet
}

// GetCondition returns the condition currently associated with the given type, or nil.
func (cs *KafkaChannelStatus) GetCondition(t apis.ConditionType) *apis.Condition {
	return cs.GetConditionSet().Manage(cs).GetCondition(t)
}

// IsReady returns true if the resource is ready overall.
func (cs *KafkaChannelStatus) IsReady() bool {
	return cs.GetConditionSet().Manage(cs).IsHappy()
}

// InitializeConditions sets relevant unset conditions to Unknown state.
func (cs *KafkaChannelStatus) InitializeConditions() {
	cs.GetConditionSet().Manage(cs).InitializeConditions()
}

// SetAddress sets the address (as part of Addressable contract) and marks the correct condition.
func (cs *KafkaChannelStatus) SetAddress(url *apis.URL) {
	if cs.Address == nil {
		cs.Address = &duckv1.Addressable{}
	}
	if url != nil {
		cs.Address.URL = url
		cs.GetConditionSet().Manage(cs).MarkTrue(ConditionAddressable)
	} else {
		cs.Address.URL = nil
		cs.GetConditionSet().Manage(cs).MarkFalse(ConditionAddressable, "nil URL", "URL is nil")
	}
}
