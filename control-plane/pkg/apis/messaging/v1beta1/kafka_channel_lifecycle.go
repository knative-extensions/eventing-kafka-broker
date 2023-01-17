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
	"sync"

	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
)

// The consolidated and distributed KafkaChannel implementations require
// differentiated ConditionSets in order to accurately reflect their varied
// runtime architectures.  One of the channel specific "Register..." functions
// in pkg/channel/<type>/apis/messaging/kafka_channel_lifecycle.go should be
// called via an init() in the main() of associated components.
var kc apis.ConditionSet
var channelCondSetLock = sync.RWMutex{}

// Shared / Common Conditions Used By All Channel Implementations
const (

	// KafkaChannelConditionReady has status True when all sub-conditions below have been set to True.
	KafkaChannelConditionReady = apis.ConditionReady

	// KafkaChannelConditionAddressable has status true when this KafkaChannel meets
	// the Addressable contract and has a non-empty URL.
	KafkaChannelConditionAddressable apis.ConditionType = "Addressable"

	// KafkaChannelConditionConfigReady has status True when the Kafka configuration to use by the channel
	// exists and is valid (i.e. the connection has been established).
	KafkaChannelConditionConfigReady apis.ConditionType = "ConfigurationReady"

	// KafkaChannelConditionTopicReady has status True when the Kafka topic to use by the channel exists.
	KafkaChannelConditionTopicReady apis.ConditionType = "TopicReady"

	// KafkaChannelConditionChannelServiceReady has status True when the K8S Service representing the channel
	// is ready. Because this uses ExternalName, there are no endpoints to check.
	KafkaChannelConditionChannelServiceReady apis.ConditionType = "ChannelServiceReady"
)

// RegisterAlternateKafkaChannelConditionSet register a different apis.ConditionSet.
func RegisterAlternateKafkaChannelConditionSet(conditionSet apis.ConditionSet) {
	channelCondSetLock.Lock()
	defer channelCondSetLock.Unlock()

	kc = conditionSet
}

// GetConditionSet retrieves the condition set for this resource. Implements the KRShaped interface.
func (*KafkaChannel) GetConditionSet() apis.ConditionSet {
	channelCondSetLock.RLock()
	defer channelCondSetLock.RUnlock()

	return kc
}

// GetConditionSet retrieves the condition set for this resource.
func (*KafkaChannelStatus) GetConditionSet() apis.ConditionSet {
	channelCondSetLock.RLock()
	defer channelCondSetLock.RUnlock()

	return kc
}

// GetCondition returns the condition currently associated with the given type, or nil.
func (kcs *KafkaChannelStatus) GetCondition(t apis.ConditionType) *apis.Condition {
	return kcs.GetConditionSet().Manage(kcs).GetCondition(t)
}

// IsReady returns true if the resource is ready overall.
func (kcs *KafkaChannelStatus) IsReady() bool {
	return kcs.GetConditionSet().Manage(kcs).IsHappy()
}

// InitializeConditions sets relevant unset conditions to Unknown state.
func (kcs *KafkaChannelStatus) InitializeConditions() {
	kcs.GetConditionSet().Manage(kcs).InitializeConditions()
}

// SetAddress sets the address (as part of Addressable contract) and marks the correct condition.
func (kcs *KafkaChannelStatus) SetAddress(url *apis.URL) {
	if kcs.Address == nil {
		kcs.Address = &duckv1.Addressable{}
	}
	if url != nil {
		kcs.Address.URL = url
		kcs.GetConditionSet().Manage(kcs).MarkTrue(KafkaChannelConditionAddressable)
	} else {
		kcs.Address.URL = nil
		kcs.GetConditionSet().Manage(kcs).MarkFalse(KafkaChannelConditionAddressable, "EmptyURL", "URL is nil")
	}
}

func (kcs *KafkaChannelStatus) MarkConfigTrue() {
	kcs.GetConditionSet().Manage(kcs).MarkTrue(KafkaChannelConditionConfigReady)
}

func (kcs *KafkaChannelStatus) MarkConfigFailed(reason, messageFormat string, messageA ...interface{}) {
	kcs.GetConditionSet().Manage(kcs).MarkFalse(KafkaChannelConditionConfigReady, reason, messageFormat, messageA...)
}

func (kcs *KafkaChannelStatus) MarkTopicTrue() {
	kcs.GetConditionSet().Manage(kcs).MarkTrue(KafkaChannelConditionTopicReady)
}

func (kcs *KafkaChannelStatus) MarkTopicFailed(reason, messageFormat string, messageA ...interface{}) {
	kcs.GetConditionSet().Manage(kcs).MarkFalse(KafkaChannelConditionTopicReady, reason, messageFormat, messageA...)
}

func (kcs *KafkaChannelStatus) MarkChannelServiceFailed(reason, messageFormat string, messageA ...interface{}) {
	kcs.GetConditionSet().Manage(kcs).MarkFalse(KafkaChannelConditionChannelServiceReady, reason, messageFormat, messageA...)
}

func (kcs *KafkaChannelStatus) MarkChannelServiceTrue() {
	kcs.GetConditionSet().Manage(kcs).MarkTrue(KafkaChannelConditionChannelServiceReady)
}
