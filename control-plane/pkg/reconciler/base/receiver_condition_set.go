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

// receiver_condition_set.go contains Broker and Kafka Sink logic for status conditions handling.
package base

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"knative.dev/eventing/pkg/reconciler/names"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/reconciler"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
)

const (
	ConditionAddressable      apis.ConditionType = "Addressable"
	ConditionTopicReady       apis.ConditionType = "TopicReady"
	ConditionConfigMapUpdated apis.ConditionType = "ConfigMapUpdated"
	ConditionConfigParsed     apis.ConditionType = "ConfigParsed"
)

var ConditionSet = apis.NewLivingConditionSet(
	ConditionAddressable,
	ConditionTopicReady,
	ConditionConfigMapUpdated,
	ConditionConfigParsed,
)

type Object interface {
	duckv1.KRShaped
	runtime.Object
}

type StatusConditionManager struct {
	Object Object

	SetAddress func(u *apis.URL)

	Configs *config.Env

	Recorder record.EventRecorder
}

func (manager *StatusConditionManager) FailedToGetConfigMap(err error) reconciler.Event {

	manager.Object.GetConditionSet().Manage(manager.Object.GetStatus()).MarkFalse(
		ConditionConfigMapUpdated,
		fmt.Sprintf(
			"Failed to get ConfigMap: %s",
			manager.Configs.DataPlaneConfigMapAsString(),
		),
		"%v",
		err,
	)

	return fmt.Errorf("failed to get brokers and triggers config map %s: %w", manager.Configs.DataPlaneConfigMapAsString(), err)
}

func (manager *StatusConditionManager) FailedToGetDataFromConfigMap(err error) reconciler.Event {

	manager.Object.GetConditionSet().Manage(manager.Object.GetStatus()).MarkFalse(
		ConditionConfigMapUpdated,
		fmt.Sprintf(
			"Failed to get brokers and trigger data from ConfigMap: %s",
			manager.Configs.DataPlaneConfigMapAsString(),
		),
		"%v",
		err,
	)

	return fmt.Errorf("failed to get broker and triggers data from config map %s: %w", manager.Configs.DataPlaneConfigMapAsString(), err)
}

func (manager *StatusConditionManager) FailedToUpdateConfigMap(err error) reconciler.Event {

	manager.Object.GetConditionSet().Manage(manager.Object.GetStatus()).MarkFalse(
		ConditionConfigMapUpdated,
		fmt.Sprintf("Failed to update ConfigMap: %s", manager.Configs.DataPlaneConfigMapAsString()),
		"%s",
		err,
	)

	return fmt.Errorf("failed to update brokers and triggers config map %s: %w", manager.Configs.DataPlaneConfigMapAsString(), err)
}

func (manager *StatusConditionManager) ConfigMapUpdated() {

	manager.Object.GetConditionSet().Manage(manager.Object.GetStatus()).MarkTrueWithReason(
		ConditionConfigMapUpdated,
		fmt.Sprintf("Config map %s updated", manager.Configs.DataPlaneConfigMapAsString()),
		"",
	)
}

func (manager *StatusConditionManager) FailedToCreateTopic(topic string, err error) reconciler.Event {

	manager.Object.GetConditionSet().Manage(manager.Object.GetStatus()).MarkFalse(
		ConditionTopicReady,
		fmt.Sprintf("Failed to create topic: %s", topic),
		"%v",
		err,
	)

	return fmt.Errorf("failed to create topic: %s: %w", topic, err)
}

func (manager *StatusConditionManager) TopicCreated(topic string) {

	manager.Object.GetConditionSet().Manage(manager.Object.GetStatus()).MarkTrueWithReason(
		ConditionTopicReady,
		fmt.Sprintf("Topic %s created", topic),
		"",
	)
}

func (manager *StatusConditionManager) Reconciled() reconciler.Event {

	object := manager.Object

	manager.SetAddress(&apis.URL{
		Scheme: "http",
		Host:   names.ServiceHostName(manager.Configs.BrokerIngressName, manager.Configs.SystemNamespace),
		Path:   fmt.Sprintf("/%s/%s", object.GetNamespace(), object.GetName()),
	})
	object.GetConditionSet().Manage(object.GetStatus()).MarkTrue(ConditionAddressable)

	return nil
}

func (manager *StatusConditionManager) FailedToUpdateDispatcherPodsAnnotation(err error) {

	// We don't set status conditions for dispatcher pods updates.

	// Record the event.
	manager.Recorder.Eventf(
		manager.Object,
		corev1.EventTypeWarning,
		"failed to update dispatcher pods annotation",
		"%v",
		err,
	)
}

func (manager *StatusConditionManager) FailedToUpdateReceiverPodsAnnotation(err error) reconciler.Event {

	return fmt.Errorf("failed to update receiver pods annotation: %w", err)
}

func (manager *StatusConditionManager) FailedToGetConfig(err error) reconciler.Event {

	return fmt.Errorf("failed to get broker configuration: %w", err)
}

func (manager *StatusConditionManager) FailedToResolveConfig(err error) reconciler.Event {

	manager.Object.GetConditionSet().Manage(manager.Object.GetStatus()).MarkFalse(
		ConditionConfigParsed,
		fmt.Sprintf("%v", err),
		"",
	)

	return fmt.Errorf("failed to get broker configuration: %w", err)
}

func (manager *StatusConditionManager) ConfigResolved() {
	manager.Object.GetConditionSet().Manage(manager.Object.GetStatus()).MarkTrue(ConditionConfigParsed)
}
