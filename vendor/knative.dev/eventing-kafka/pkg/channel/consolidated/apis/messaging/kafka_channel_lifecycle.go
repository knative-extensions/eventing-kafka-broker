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

package messaging

import (
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"knative.dev/pkg/apis"

	messaging "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
)

/*
 * Each KafkaChannel implementation will require its own unique set of
 * Status Conditions in order to accurately reflect the state of its
 * runtime artifacts.  Therefore, these conditions are defined here
 * instead of the standard /apis/.../kafka_channel_lifecycle.go location.
 */

const (

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
)

// RegisterConsolidatedKafkaChannelConditionSet initializes the ConditionSet to those pertaining to the consolidated KafkaChannel.
func RegisterConsolidatedKafkaChannelConditionSet() {
	messaging.RegisterAlternateKafkaChannelConditionSet(
		apis.NewLivingConditionSet(
			messaging.KafkaChannelConditionAddressable,
			messaging.KafkaChannelConditionConfigReady,
			messaging.KafkaChannelConditionTopicReady,
			messaging.KafkaChannelConditionChannelServiceReady,
			KafkaChannelConditionDispatcherReady,
			KafkaChannelConditionServiceReady,
			KafkaChannelConditionEndpointsReady,
		),
	)
}

func MarkDispatcherFailed(kcs *messaging.KafkaChannelStatus, reason, messageFormat string, messageA ...interface{}) {
	kcs.GetConditionSet().Manage(kcs).MarkFalse(KafkaChannelConditionDispatcherReady, reason, messageFormat, messageA...)
}

func MarkDispatcherUnknown(kcs *messaging.KafkaChannelStatus, reason, messageFormat string, messageA ...interface{}) {
	kcs.GetConditionSet().Manage(kcs).MarkUnknown(KafkaChannelConditionDispatcherReady, reason, messageFormat, messageA...)
}

// TODO: Unify this with the ones from Eventing. Say: Broker, Trigger.
func PropagateDispatcherStatus(kcs *messaging.KafkaChannelStatus, ds *appsv1.DeploymentStatus) {
	for _, cond := range ds.Conditions {
		if cond.Type == appsv1.DeploymentAvailable {
			if cond.Status == corev1.ConditionTrue {
				kcs.GetConditionSet().Manage(kcs).MarkTrue(KafkaChannelConditionDispatcherReady)
			} else if cond.Status == corev1.ConditionFalse {
				MarkDispatcherFailed(kcs, "DispatcherDeploymentFalse", "The status of Dispatcher Deployment is False: %s : %s", cond.Reason, cond.Message)
			} else if cond.Status == corev1.ConditionUnknown {
				MarkDispatcherUnknown(kcs, "DispatcherDeploymentUnknown", "The status of Dispatcher Deployment is Unknown: %s : %s", cond.Reason, cond.Message)
			}
		}
	}
}

func MarkServiceFailed(kcs *messaging.KafkaChannelStatus, reason, messageFormat string, messageA ...interface{}) {
	kcs.GetConditionSet().Manage(kcs).MarkFalse(KafkaChannelConditionServiceReady, reason, messageFormat, messageA...)
}

func MarkServiceUnknown(kcs *messaging.KafkaChannelStatus, reason, messageFormat string, messageA ...interface{}) {
	kcs.GetConditionSet().Manage(kcs).MarkUnknown(KafkaChannelConditionServiceReady, reason, messageFormat, messageA...)
}

func MarkServiceTrue(kcs *messaging.KafkaChannelStatus) {
	kcs.GetConditionSet().Manage(kcs).MarkTrue(KafkaChannelConditionServiceReady)
}

func MarkEndpointsFailed(kcs *messaging.KafkaChannelStatus, reason, messageFormat string, messageA ...interface{}) {
	kcs.GetConditionSet().Manage(kcs).MarkFalse(KafkaChannelConditionEndpointsReady, reason, messageFormat, messageA...)
}

func MarkEndpointsTrue(kcs *messaging.KafkaChannelStatus) {
	kcs.GetConditionSet().Manage(kcs).MarkTrue(KafkaChannelConditionEndpointsReady)
}
