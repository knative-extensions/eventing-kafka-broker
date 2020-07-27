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

package broker

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1beta1"
	"knative.dev/eventing/pkg/reconciler/names"
	"knative.dev/pkg/apis"
	"knative.dev/pkg/reconciler"
)

const (
	ConditionAddressable      apis.ConditionType = "Addressable"
	ConditionTopicReady       apis.ConditionType = "TopicReady"
	ConditionConfigMapUpdated apis.ConditionType = "ConfigMapUpdated"
)

var ConditionSet = apis.NewLivingConditionSet(
	ConditionAddressable,
	ConditionTopicReady,
	ConditionConfigMapUpdated,
)

const (
	Broker     = "Broker"
	Reconciled = Broker + "Reconciled"
)

type statusConditionManager struct {
	Broker *eventing.Broker

	configs *Configs

	recorder record.EventRecorder
}

func (manager *statusConditionManager) failedToGetBrokersTriggersConfigMap(err error) reconciler.Event {

	manager.Broker.GetConditionSet().Manage(&manager.Broker.Status).MarkFalse(
		ConditionConfigMapUpdated,
		fmt.Sprintf(
			"Failed to get ConfigMap: %s",
			manager.configs.DataPlaneConfigMapAsString(),
		),
		"%v",
		err,
	)

	return fmt.Errorf("failed to get brokers and triggers config map %s: %w", manager.configs.DataPlaneConfigMapAsString(), err)
}

func (manager *statusConditionManager) failedToGetBrokersTriggersDataFromConfigMap(err error) reconciler.Event {

	manager.Broker.GetConditionSet().Manage(&manager.Broker.Status).MarkFalse(
		ConditionConfigMapUpdated,
		fmt.Sprintf(
			"Failed to get brokers and trigger data from ConfigMap: %s",
			manager.configs.DataPlaneConfigMapAsString(),
		),
		"%v",
		err,
	)

	return fmt.Errorf("failed to get broker and triggers data from config map %s: %w", manager.configs.DataPlaneConfigMapAsString(), err)
}

func (manager *statusConditionManager) failedToUpdateBrokersTriggersConfigMap(err error) reconciler.Event {

	manager.Broker.GetConditionSet().Manage(&manager.Broker.Status).MarkFalse(
		ConditionConfigMapUpdated,
		fmt.Sprintf("Failed to update ConfigMap: %s", manager.configs.DataPlaneConfigMapAsString()),
		"%s",
		err,
	)

	return fmt.Errorf("failed to update brokers and triggers config map %s: %w", manager.configs.DataPlaneConfigMapAsString(), err)
}

func (manager *statusConditionManager) brokersTriggersConfigMapUpdated() {

	manager.Broker.GetConditionSet().Manage(&manager.Broker.Status).MarkTrueWithReason(
		ConditionConfigMapUpdated,
		fmt.Sprintf("Config map %s updated", manager.configs.DataPlaneConfigMapAsString()),
		"",
	)
}

func (manager *statusConditionManager) failedToCreateTopic(topic string, err error) reconciler.Event {

	manager.Broker.GetConditionSet().Manage(&manager.Broker.Status).MarkFalse(
		ConditionTopicReady,
		fmt.Sprintf("Failed to create topic: %s", topic),
		"%v",
		err,
	)

	return fmt.Errorf("failed to create topic: %s: %w", topic, err)
}

func (manager *statusConditionManager) topicCreated(topic string) {

	manager.Broker.GetConditionSet().Manage(&manager.Broker.Status).MarkTrueWithReason(
		ConditionTopicReady,
		fmt.Sprintf("Topic %s created", topic),
		"",
	)
}

func (manager *statusConditionManager) reconciled() reconciler.Event {

	broker := manager.Broker

	broker.Status.Address.URL = &apis.URL{
		Scheme: "http",
		Host:   names.ServiceHostName(manager.configs.BrokerIngressName, manager.configs.SystemNamespace),
		Path:   fmt.Sprintf("/%s/%s", broker.Namespace, broker.Name),
	}
	broker.GetConditionSet().Manage(&broker.Status).MarkTrue(ConditionAddressable)

	return nil
}

func (manager *statusConditionManager) failedToUpdateDispatcherPodsAnnotation(err error) {

	// We don't set status conditions for dispatcher pods updates.

	// Record the event.
	manager.recorder.Eventf(
		manager.Broker,
		corev1.EventTypeWarning,
		"failed to update dispatcher pods annotation",
		"%v",
		err,
	)
}

func (manager *statusConditionManager) failedToUpdateReceiverPodsAnnotation(err error) reconciler.Event {

	return fmt.Errorf("failed to update receiver pods annotation: %w", err)
}

func (manager *statusConditionManager) failedToGetBrokerConfig(err error) reconciler.Event {

	return fmt.Errorf("failed to get broker configuration: %w", err)
}
