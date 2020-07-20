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
	"context"
	"fmt"
	"math"

	"go.uber.org/zap"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1beta1"
	eventinglisters "knative.dev/eventing/pkg/client/listers/eventing/v1beta1"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/reconciler"
	"knative.dev/pkg/resolver"

	coreconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/core/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/log"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	brokerreconciler "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/broker"
)

const (
	trigger           = "Trigger"
	triggerReconciled = trigger + "Reconciled"

	noTrigger = brokerreconciler.NoBroker
)

type Reconciler struct {
	*base.Reconciler

	BrokerLister eventinglisters.BrokerLister
	Resolver     *resolver.URIResolver

	Configs *brokerreconciler.EnvConfigs
}

func (r *Reconciler) ReconcileKind(ctx context.Context, trigger *eventing.Trigger) reconciler.Event {

	logger := log.Logger(ctx, "trigger", trigger)

	logger.Debug("Reconciling Trigger", zap.Any("trigger", trigger))

	statusConditionManager := statusConditionManager{
		Trigger:  trigger,
		Configs:  r.Configs,
		Recorder: controller.GetEventRecorder(ctx),
	}

	broker, err := r.BrokerLister.Brokers(trigger.Namespace).Get(trigger.Spec.Broker)
	if err != nil && !apierrors.IsNotFound(err) {
		return statusConditionManager.failedToGetBroker(err)
	}

	if apierrors.IsNotFound(err) || !broker.GetDeletionTimestamp().IsZero() {
		// The associated broker doesn't exist anymore, so clean up Trigger resources.
		return r.FinalizeKind(ctx, trigger)
	}

	statusConditionManager.propagateBrokerCondition(broker)

	// Get data plane config map.
	dataPlaneConfigMap, err := r.GetDataPlaneConfigMap()
	if err != nil {
		return statusConditionManager.failedToGetDataPlaneConfigMap(err)
	}

	logger.Debug("Got brokers and triggers config map")

	// Get data plane config data.
	dataPlaneConfig, err := r.GetDataPlaneConfigMapData(logger, dataPlaneConfigMap)
	if err != nil || dataPlaneConfig == nil {
		return statusConditionManager.failedToGetDataPlaneConfigFromConfigMap(err)
	}

	logger.Debug(
		"Got brokers and triggers data from config map",
		zap.Any(base.BrokersTriggersDataLogKey, log.BrokersMarshaller{Brokers: dataPlaneConfig}),
	)

	brokerIndex := brokerreconciler.FindBroker(dataPlaneConfig, broker)
	if brokerIndex == brokerreconciler.NoBroker {
		return statusConditionManager.brokerNotFoundInDataPlaneConfigMap()
	}
	triggerIndex := findTrigger(dataPlaneConfig.Broker[brokerIndex].Triggers, trigger)

	triggerConfig, err := r.GetTriggerConfig(trigger)
	if err != nil {
		return statusConditionManager.failedToResolveTriggerConfig(err)
	}

	statusConditionManager.subscriberResolved()

	if triggerIndex == noTrigger {
		dataPlaneConfig.Broker[brokerIndex].Triggers = append(
			dataPlaneConfig.Broker[brokerIndex].Triggers,
			&triggerConfig,
		)
	} else {
		dataPlaneConfig.Broker[brokerIndex].Triggers[triggerIndex] = &triggerConfig
	}

	// Increment volumeGeneration
	dataPlaneConfig.VolumeGeneration = incrementVolumeGeneration(dataPlaneConfig.VolumeGeneration)

	// Update the configuration map with the new dataPlaneConfig data.
	if err := r.UpdateDataPlaneConfigMap(dataPlaneConfig, dataPlaneConfigMap); err != nil {
		return fmt.Errorf("failed to update configuration map: %w", err)
	}

	// Update volume generation annotation of dispatcher pods
	if err := r.UpdateDispatcherPodsAnnotation(logger, dataPlaneConfig.VolumeGeneration); err != nil {
		// Failing to update dispatcher pods annotation leads to config map refresh delayed by several seconds.
		// Since the dispatcher side is the consumer side, we don't lose availability, and we can consider the Trigger
		// ready. So, log out the error and move on to the next step.
		logger.Warn(
			"Failed to update dispatcher pod annotation to trigger an immediate config map refresh",
			zap.Error(err),
		)

		statusConditionManager.failedToUpdateDispatcherPodsAnnotation(err)
	} else {
		logger.Debug("Updated dispatcher pod annotation")
	}

	logger.Debug("Brokers and triggers config map updated")

	return statusConditionManager.reconciled()
}

func (r *Reconciler) FinalizeKind(ctx context.Context, trigger *eventing.Trigger) reconciler.Event {

	logger := log.Logger(ctx, "trigger", trigger)

	logger.Debug("Finalizing Trigger", zap.Any("trigger", trigger))

	return reconciledNormal(trigger.Namespace, trigger.Name)
}

func (r *Reconciler) GetTriggerConfig(trigger *eventing.Trigger) (coreconfig.Trigger, error) {

	var attributes map[string]string
	if trigger.Spec.Filter != nil {
		attributes = trigger.Spec.Filter.Attributes
	}

	destination, err := r.Resolver.URIFromDestinationV1(trigger.Spec.Subscriber, trigger)
	if err != nil {
		return coreconfig.Trigger{}, fmt.Errorf("failed to resolve Trigger.Spec.Subscriber: %w", err)
	}

	return coreconfig.Trigger{
		Attributes:  attributes,
		Destination: destination.String(),
		Id:          string(trigger.UID),
	}, nil
}

func findTrigger(triggers []*coreconfig.Trigger, trigger *eventing.Trigger) int {

	for i, t := range triggers {
		if t.Id == string(trigger.UID) {
			return i
		}
	}
	return noTrigger
}

func incrementVolumeGeneration(generation uint64) uint64 {
	return (generation + 1) % (math.MaxUint64 - 1)
}
