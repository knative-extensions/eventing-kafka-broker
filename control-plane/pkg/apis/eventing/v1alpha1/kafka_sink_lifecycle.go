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
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
)

const (
	ConditionAddressable        apis.ConditionType = "Addressable"
	ConditionEventPoliciesReady apis.ConditionType = "EventPoliciesReady"
)

var conditionSet apis.ConditionSet

func RegisterConditionSet(cs apis.ConditionSet) {
	conditionSet = cs
}

func (ks *KafkaSink) GetConditionSet() apis.ConditionSet {
	return conditionSet
}

func (kss *KafkaSinkStatus) GetConditionSet() apis.ConditionSet {
	return conditionSet
}

// SetAddress makes this Kafka Sink addressable by setting the URI. It also
// sets the ConditionAddressable to true.
func (kss *KafkaSinkStatus) SetAddress(addr *duckv1.Addressable) {
	kss.Address = addr
	if addr != nil {
		kss.GetConditionSet().Manage(kss).MarkTrue(ConditionAddressable)
	} else {
		kss.GetConditionSet().Manage(kss).MarkFalse(ConditionAddressable, "nil URL", "URL is nil")
	}
}

// InitializeConditions sets relevant unset conditions to Unknown state.
func (kss *KafkaSinkStatus) InitializeConditions() {
	kss.GetConditionSet().Manage(kss).InitializeConditions()
}

func (kss *KafkaSinkStatus) MarkEventPoliciesTrue() {
	kss.GetConditionSet().Manage(kss).MarkTrue(ConditionEventPoliciesReady)
}

func (kss *KafkaSinkStatus) MarkEventPoliciesTrueWithReason(reason, messageFormat string, messageA ...interface{}) {
	kss.GetConditionSet().Manage(kss).MarkTrueWithReason(ConditionEventPoliciesReady, reason, messageFormat, messageA...)
}

func (kss *KafkaSinkStatus) MarkEventPoliciesFailed(reason, messageFormat string, messageA ...interface{}) {
	kss.GetConditionSet().Manage(kss).MarkFalse(ConditionEventPoliciesReady, reason, messageFormat, messageA...)
}

func (kss *KafkaSinkStatus) MarkEventPoliciesUnknown(reason, messageFormat string, messageA ...interface{}) {
	kss.GetConditionSet().Manage(kss).MarkUnknown(ConditionEventPoliciesReady, reason, messageFormat, messageA...)
}
