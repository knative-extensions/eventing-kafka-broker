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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
)

var kc = apis.NewLivingConditionSet(
	KafkaChannelConditionTopicReady,
	KafkaChannelConditionDispatcherReady,
	KafkaChannelConditionServiceReady,
	KafkaChannelConditionEndpointsReady,
	KafkaChannelConditionAddressable,
	KafkaChannelConditionChannelServiceReady,
	KafkaChannelConditionConfigReady)
var channelCondSetLock = sync.RWMutex{}

const (
	// KafkaChannelConditionReady has status True when all subconditions below have been set to True.
	KafkaChannelConditionReady = apis.ConditionReady

	// KafkaChannelConditionDispatcherReady has status True when a Dispatcher deployment is ready
	// Keyed off appsv1.DeploymentAvailable, which means minimum available replicas required are up
	// and running for at least minReadySeconds.
	KafkaChannelConditionDispatcherReady apis.ConditionType = "DispatcherReady"

	// KafkaChannelConditionServiceReady has status True when a k8s Service is ready. This
	// basically just means it exists because there's no meaningful status in Service. See Endpoints
	// below.
	KafkaChannelConditionServiceReady apis.ConditionType = "ServiceReady"

	// KafkaChannelConditionEndpointsReady has status True when a k8s Service Endpoints are backed
	// by at least one endpoint.
	KafkaChannelConditionEndpointsReady apis.ConditionType = "EndpointsReady"

	// KafkaChannelConditionAddressable has status true when this KafkaChannel meets
	// the Addressable contract and has a non-empty URL.
	KafkaChannelConditionAddressable apis.ConditionType = "Addressable"

	// KafkaChannelConditionServiceReady has status True when a k8s Service representing the channel is ready.
	// Because this uses ExternalName, there are no endpoints to check.
	KafkaChannelConditionChannelServiceReady apis.ConditionType = "ChannelServiceReady"

	// KafkaChannelConditionTopicReady has status True when the Kafka topic to use by the channel exists.
	KafkaChannelConditionTopicReady apis.ConditionType = "TopicReady"

	// KafkaChannelConditionConfigReady has status True when the Kafka configuration to use by the channel exists and is valid
	// (ie. the connection has been established).
	KafkaChannelConditionConfigReady apis.ConditionType = "ConfigurationReady"
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
		cs.GetConditionSet().Manage(cs).MarkTrue(KafkaChannelConditionAddressable)
	} else {
		cs.Address.URL = nil
		cs.GetConditionSet().Manage(cs).MarkFalse(KafkaChannelConditionAddressable, "EmptyURL", "URL is nil")
	}
}

func (cs *KafkaChannelStatus) MarkDispatcherFailed(reason, messageFormat string, messageA ...interface{}) {
	cs.GetConditionSet().Manage(cs).MarkFalse(KafkaChannelConditionDispatcherReady, reason, messageFormat, messageA...)
}

func (cs *KafkaChannelStatus) MarkDispatcherUnknown(reason, messageFormat string, messageA ...interface{}) {
	cs.GetConditionSet().Manage(cs).MarkUnknown(KafkaChannelConditionDispatcherReady, reason, messageFormat, messageA...)
}

// TODO: Unify this with the ones from Eventing. Say: Broker, Trigger.
func (cs *KafkaChannelStatus) PropagateDispatcherStatus(ds *appsv1.DeploymentStatus) {
	for _, cond := range ds.Conditions {
		if cond.Type == appsv1.DeploymentAvailable {
			if cond.Status == corev1.ConditionTrue {
				cs.GetConditionSet().Manage(cs).MarkTrue(KafkaChannelConditionDispatcherReady)
			} else if cond.Status == corev1.ConditionFalse {
				cs.MarkDispatcherFailed("DispatcherDeploymentFalse", "The status of Dispatcher Deployment is False: %s : %s", cond.Reason, cond.Message)
			} else if cond.Status == corev1.ConditionUnknown {
				cs.MarkDispatcherUnknown("DispatcherDeploymentUnknown", "The status of Dispatcher Deployment is Unknown: %s : %s", cond.Reason, cond.Message)
			}
		}
	}
}

func (cs *KafkaChannelStatus) MarkServiceFailed(reason, messageFormat string, messageA ...interface{}) {
	cs.GetConditionSet().Manage(cs).MarkFalse(KafkaChannelConditionServiceReady, reason, messageFormat, messageA...)
}

func (cs *KafkaChannelStatus) MarkServiceUnknown(reason, messageFormat string, messageA ...interface{}) {
	cs.GetConditionSet().Manage(cs).MarkUnknown(KafkaChannelConditionServiceReady, reason, messageFormat, messageA...)
}

func (cs *KafkaChannelStatus) MarkServiceTrue() {
	cs.GetConditionSet().Manage(cs).MarkTrue(KafkaChannelConditionServiceReady)
}

func (cs *KafkaChannelStatus) MarkChannelServiceFailed(reason, messageFormat string, messageA ...interface{}) {
	cs.GetConditionSet().Manage(cs).MarkFalse(KafkaChannelConditionChannelServiceReady, reason, messageFormat, messageA...)
}

func (cs *KafkaChannelStatus) MarkChannelServiceTrue() {
	cs.GetConditionSet().Manage(cs).MarkTrue(KafkaChannelConditionChannelServiceReady)
}

func (cs *KafkaChannelStatus) MarkEndpointsFailed(reason, messageFormat string, messageA ...interface{}) {
	cs.GetConditionSet().Manage(cs).MarkFalse(KafkaChannelConditionEndpointsReady, reason, messageFormat, messageA...)
}

func (cs *KafkaChannelStatus) MarkEndpointsTrue() {
	cs.GetConditionSet().Manage(cs).MarkTrue(KafkaChannelConditionEndpointsReady)
}

func (cs *KafkaChannelStatus) MarkTopicTrue() {
	cs.GetConditionSet().Manage(cs).MarkTrue(KafkaChannelConditionTopicReady)
}

func (cs *KafkaChannelStatus) MarkTopicFailed(reason, messageFormat string, messageA ...interface{}) {
	cs.GetConditionSet().Manage(cs).MarkFalse(KafkaChannelConditionTopicReady, reason, messageFormat, messageA...)
}

func (cs *KafkaChannelStatus) MarkConfigTrue() {
	cs.GetConditionSet().Manage(cs).MarkTrue(KafkaChannelConditionConfigReady)
}

func (cs *KafkaChannelStatus) MarkConfigFailed(reason, messageFormat string, messageA ...interface{}) {
	cs.GetConditionSet().Manage(cs).MarkFalse(KafkaChannelConditionConfigReady, reason, messageFormat, messageA...)
}
