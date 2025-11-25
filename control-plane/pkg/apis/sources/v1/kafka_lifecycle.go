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

package v1

import (
	"sync"

	appsv1 "k8s.io/api/apps/v1"
	"knative.dev/eventing/pkg/apis/duck"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
)

const (
	// KafkaConditionReady has status True when the KafkaSource is ready to send events.
	KafkaConditionReady = apis.ConditionReady

	// KafkaConditionSinkProvided has status True when the KafkaSource has been configured with a sink target.
	KafkaConditionSinkProvided apis.ConditionType = "SinkProvided"

	// KafkaConditionDeployed has status True when the KafkaSource has had it's receive adapter deployment created.
	KafkaConditionDeployed apis.ConditionType = "Deployed"

	// KafkaConditionKeyType is True when the KafkaSource has been configured with valid key type for
	// the key deserializer.
	KafkaConditionKeyType apis.ConditionType = "KeyTypeCorrect"

	// KafkaConditionConnectionEstablished has status True when the Kafka configuration to use by the source
	// succeeded in establishing a connection to Kafka.
	KafkaConditionConnectionEstablished apis.ConditionType = "ConnectionEstablished"

	// KafkaConditionInitialOffsetsCommitted is True when the KafkaSource has committed the
	// initial offset of all claims
	KafkaConditionInitialOffsetsCommitted apis.ConditionType = "InitialOffsetsCommitted"

	// KafkaConditionOIDCIdentityCreated has status True when the KafkaSource has created an OIDC identity.
	KafkaConditionOIDCIdentityCreated apis.ConditionType = "OIDCIdentityCreated"
)

var (
	KafkaSourceCondSet = apis.NewLivingConditionSet(
		KafkaConditionSinkProvided,
		KafkaConditionDeployed,
		KafkaConditionConnectionEstablished,
		KafkaConditionInitialOffsetsCommitted,
		KafkaConditionOIDCIdentityCreated,
	)

	kafkaCondSetLock = sync.RWMutex{}
)

// RegisterAlternateKafkaConditionSet register an alternate apis.ConditionSet.
func RegisterAlternateKafkaConditionSet(conditionSet apis.ConditionSet) {
	kafkaCondSetLock.Lock()
	defer kafkaCondSetLock.Unlock()

	KafkaSourceCondSet = conditionSet
}

// GetConditionSet retrieves the condition set for this resource. Implements the KRShaped interface.
func (ks *KafkaSource) GetConditionSet() apis.ConditionSet {
	return KafkaSourceCondSet
}

func (kss *KafkaSourceStatus) GetCondition(t apis.ConditionType) *apis.Condition {
	return KafkaSourceCondSet.Manage(kss).GetCondition(t)
}

// IsReady returns true if the resource is ready overall.
func (kss *KafkaSourceStatus) IsReady() bool {
	return KafkaSourceCondSet.Manage(kss).IsHappy()
}

// InitializeConditions sets relevant unset conditions to Unknown state.
func (kss *KafkaSourceStatus) InitializeConditions() {
	KafkaSourceCondSet.Manage(kss).InitializeConditions()
}

// MarkSink sets the condition that the source has a sink configured.
func (kss *KafkaSourceStatus) MarkSink(addr *duckv1.Addressable) {
	if addr.URL != nil && !addr.URL.IsEmpty() {
		kss.SinkURI = addr.URL
		kss.SinkCACerts = addr.CACerts
		kss.SinkAudience = addr.Audience
		KafkaSourceCondSet.Manage(kss).MarkTrue(KafkaConditionSinkProvided)
	} else {
		KafkaSourceCondSet.Manage(kss).MarkUnknown(KafkaConditionSinkProvided, "SinkEmpty", "Sink has resolved to empty.%s", "")
	}
}

// MarkNoSink sets the condition that the source does not have a sink configured.
func (kss *KafkaSourceStatus) MarkNoSink(reason, messageFormat string, messageA ...interface{}) {
	KafkaSourceCondSet.Manage(kss).MarkFalse(KafkaConditionSinkProvided, reason, messageFormat, messageA...)
}

func DeploymentIsAvailable(ds *appsv1.DeploymentStatus, def bool) bool {
	// Check if the Deployment is available.
	for _, cond := range ds.Conditions {
		if cond.Type == appsv1.DeploymentAvailable {
			return cond.Status == "True"
		}
	}
	return def
}

// MarkDeployed sets the condition that the source has been deployed.
func (kss *KafkaSourceStatus) MarkDeployed(d *appsv1.Deployment) {
	if duck.DeploymentIsAvailable(&d.Status, false) {
		KafkaSourceCondSet.Manage(kss).MarkTrue(KafkaConditionDeployed)

		// Propagate the number of consumers
		kss.Consumers = d.Status.Replicas
	} else {
		// I don't know how to propagate the status well, so just give the name of the Deployment
		// for now.
		KafkaSourceCondSet.Manage(kss).MarkFalse(KafkaConditionDeployed, "DeploymentUnavailable", "The Deployment '%s' is unavailable.", d.Name)
	}
}

// MarkDeploying sets the condition that the source is deploying.
func (kss *KafkaSourceStatus) MarkDeploying(reason, messageFormat string, messageA ...interface{}) {
	KafkaSourceCondSet.Manage(kss).MarkUnknown(KafkaConditionDeployed, reason, messageFormat, messageA...)
}

// MarkNotDeployed sets the condition that the source has not been deployed.
func (kss *KafkaSourceStatus) MarkNotDeployed(reason, messageFormat string, messageA ...interface{}) {
	KafkaSourceCondSet.Manage(kss).MarkFalse(KafkaConditionDeployed, reason, messageFormat, messageA...)
}

func (kss *KafkaSourceStatus) MarkKeyTypeCorrect() {
	KafkaSourceCondSet.Manage(kss).MarkTrue(KafkaConditionKeyType)
}

func (kss *KafkaSourceStatus) MarkKeyTypeIncorrect(reason, messageFormat string, messageA ...interface{}) {
	KafkaSourceCondSet.Manage(kss).MarkFalse(KafkaConditionKeyType, reason, messageFormat, messageA...)
}

func (kss *KafkaSourceStatus) MarkConnectionEstablished() {
	KafkaSourceCondSet.Manage(kss).MarkTrue(KafkaConditionConnectionEstablished)
}

func (kss *KafkaSourceStatus) MarkConnectionNotEstablished(reason, messageFormat string, messageA ...interface{}) {
	KafkaSourceCondSet.Manage(kss).MarkFalse(KafkaConditionConnectionEstablished, reason, messageFormat, messageA...)
}

func (kss *KafkaSourceStatus) MarkInitialOffsetCommitted() {
	KafkaSourceCondSet.Manage(kss).MarkTrue(KafkaConditionInitialOffsetsCommitted)
}

func (kss *KafkaSourceStatus) MarkInitialOffsetNotCommitted(reason, messageFormat string, messageA ...interface{}) {
	KafkaSourceCondSet.Manage(kss).MarkFalse(KafkaConditionInitialOffsetsCommitted, reason, messageFormat, messageA...)
}

func (kss *KafkaSourceStatus) MarkOIDCIdentityCreatedSucceeded() {
	KafkaSourceCondSet.Manage(kss).MarkTrue(KafkaConditionOIDCIdentityCreated)
}

func (kss *KafkaSourceStatus) MarkOIDCIdentityCreatedSucceededWithReason(reason, messageFormat string, messageA ...interface{}) {
	KafkaSourceCondSet.Manage(kss).MarkTrueWithReason(KafkaConditionOIDCIdentityCreated, reason, messageFormat, messageA...)
}

func (kss *KafkaSourceStatus) MarkOIDCIdentityCreatedFailed(reason, messageFormat string, messageA ...interface{}) {
	KafkaSourceCondSet.Manage(kss).MarkFalse(KafkaConditionOIDCIdentityCreated, reason, messageFormat, messageA...)
}

func (kss *KafkaSourceStatus) MarkOIDCIdentityCreatedUnknown(reason, messageFormat string, messageA ...interface{}) {
	KafkaSourceCondSet.Manage(kss).MarkUnknown(KafkaConditionOIDCIdentityCreated, reason, messageFormat, messageA...)
}

func (kss *KafkaSourceStatus) UpdateConsumerGroupStatus(status string) {
	kss.Claims = status
}
