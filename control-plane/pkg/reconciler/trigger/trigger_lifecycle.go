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

package trigger

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1"
	"knative.dev/pkg/apis"
	"knative.dev/pkg/reconciler"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
)

type statusConditionManager struct {
	Trigger *eventing.Trigger

	Configs *config.Env

	Recorder record.EventRecorder
}

func (m *statusConditionManager) failedToGetBroker(err error) reconciler.Event {

	m.Trigger.Status.MarkBrokerFailed(
		"Failed to get broker",
		"%v",
		err,
	)

	return fmt.Errorf("failed to get broker: %w", err)
}

func (m *statusConditionManager) failedToGetDataPlaneConfigMap(err error) reconciler.Event {

	reason := fmt.Sprintf("Failed to get data plane config map %s", m.Configs.DataPlaneConfigMapAsString())
	m.Trigger.Status.MarkDependencyFailed(
		reason,
		"%v",
		err,
	)

	return fmt.Errorf(reason+": %w", err)
}

func (m *statusConditionManager) propagateBrokerCondition(broker *eventing.Broker) {
	m.Trigger.Status.PropagateBrokerCondition(broker.Status.GetTopLevelCondition())
}

func (m *statusConditionManager) failedToGetDataPlaneConfigFromConfigMap(err error) reconciler.Event {

	m.Trigger.Status.MarkDependencyFailed(
		"Failed to get data plane config from config map",
		"%v",
		err,
	)

	return fmt.Errorf("failed to get data plane config from config map: %w", err)
}

func (m *statusConditionManager) brokerNotFoundInDataPlaneConfigMap() reconciler.Event {

	m.Trigger.Status.MarkBrokerFailed(
		"Broker not found in data plane map",
		"config map: %s",
		m.Configs.DataPlaneConfigMapAsString(),
	)

	return fmt.Errorf("broker not found in data plane config map %s", m.Configs.DataPlaneConfigMapAsString())
}

func (m *statusConditionManager) reconciled() reconciler.Event {

	m.Trigger.Status.MarkDependencySucceeded()

	// TODO we don't have a subscription, consider register custom condition set for Triggers
	m.Trigger.Status.PropagateSubscriptionCondition(&apis.Condition{
		Status: corev1.ConditionTrue,
	})

	return nil
}

func (m *statusConditionManager) failedToResolveTriggerConfig(err error) reconciler.Event {

	m.Trigger.Status.MarkSubscriberResolvedFailed(
		"Failed to resolve trigger config",
		"%v",
		err,
	)

	return fmt.Errorf("failed to resolve trigger config: %w", err)
}

func (m *statusConditionManager) failedToUpdateDispatcherPodsAnnotation(err error) {

	// We don't set status conditions for dispatcher pods updates.

	// Record the event.
	m.Recorder.Eventf(
		m.Trigger,
		corev1.EventTypeWarning,
		"Failed to update dispatcher pods annotation",
		"%v",
		err,
	)
}

func (m *statusConditionManager) subscriberResolved(details string) {
	m.Trigger.GetConditionSet().Manage(&m.Trigger.Status).MarkTrueWithReason(
		eventing.TriggerConditionSubscriberResolved,
		string(eventing.TriggerConditionSubscriberResolved),
		details,
	)
}
