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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1"
	eventingclientset "knative.dev/eventing/pkg/client/clientset/versioned"
	eventinglisters "knative.dev/eventing/pkg/client/listers/eventing/v1"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/reconciler"
	"knative.dev/pkg/resolver"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	coreconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/core/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/log"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
)

type Reconciler struct {
	*base.Reconciler

	BrokerLister   eventinglisters.BrokerLister
	EventingClient eventingclientset.Interface
	Resolver       *resolver.URIResolver

	Configs *config.Env
}

func (r *Reconciler) ReconcileKind(ctx context.Context, trigger *eventing.Trigger) reconciler.Event {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		return r.reconcileKind(ctx, trigger)
	})
}

func (r *Reconciler) FinalizeKind(ctx context.Context, trigger *eventing.Trigger) reconciler.Event {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		return r.finalizeKind(ctx, trigger)
	})
}

func (r *Reconciler) finalizeKind(ctx context.Context, trigger *eventing.Trigger) reconciler.Event {

	logger := log.Logger(ctx, "finalize", trigger)

	broker, err := r.BrokerLister.Brokers(trigger.Namespace).Get(trigger.Spec.Broker)
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to get broker from lister: %w", err)
	}

	if apierrors.IsNotFound(err) {
		// If the broker is deleted, resources associated with the Trigger will be deleted.
		return nil
	}

	// Get data plane config map.
	dataPlaneConfigMap, err := r.GetOrCreateDataPlaneConfigMap()
	if err != nil {
		return fmt.Errorf("failed to get data plane config map %s: %w", r.Configs.DataPlaneConfigMapAsString(), err)
	}

	logger.Debug("Got data plane config map")

	// Get brokersTriggers data.
	brokersTriggers, err := r.GetDataPlaneConfigMapData(logger, dataPlaneConfigMap)
	if err != nil {
		return fmt.Errorf("failed to get brokers and triggers: %w", err)
	}

	logger.Debug(
		"Got brokers and triggers data from data plane config map",
		zap.Any(base.BrokersTriggersDataLogKey, log.BrokersMarshaller{Brokers: brokersTriggers}),
	)

	brokerIndex := coreconfig.FindBroker(brokersTriggers, broker.UID)
	if brokerIndex == coreconfig.NoBroker {
		// If the broker is not there, resources associated with the Trigger are deleted accordingly.
		return nil
	}

	logger.Debug("Found Broker", zap.Int("brokerIndex", brokerIndex))

	triggers := brokersTriggers.Brokers[brokerIndex].Triggers
	triggerIndex := coreconfig.FindTrigger(triggers, trigger.UID)
	if triggerIndex == coreconfig.NoTrigger {
		// The trigger is not there, resources associated with the Trigger are deleted accordingly.
		logger.Debug("trigger not found in config map")

		return nil
	}

	logger.Debug("Found Trigger", zap.Int("triggerIndex", brokerIndex))

	// Delete the Trigger from the config map data.
	brokersTriggers.Brokers[brokerIndex].Triggers = deleteTrigger(triggers, triggerIndex)

	// Increment volume generation
	brokersTriggers.VolumeGeneration = incrementVolumeGeneration(brokersTriggers.VolumeGeneration)

	// Update data plane config map.
	err = r.UpdateDataPlaneConfigMap(brokersTriggers, dataPlaneConfigMap)
	if err != nil {
		return err
	}

	logger.Debug("Updated data plane config map", zap.String("configmap", r.Configs.DataPlaneConfigMapAsString()))

	// Update volume generation annotation of dispatcher pods
	if err := r.UpdateDispatcherPodsAnnotation(logger, brokersTriggers.VolumeGeneration); err != nil {
		// Failing to update dispatcher pods annotation leads to config map refresh delayed by several seconds.
		// The delete trigger will eventually be seen by the data plane pods, so log out the error and move on to the
		// next step.
		logger.Warn(
			"Failed to update dispatcher pod annotation to trigger an immediate config map refresh",
			zap.Error(err),
		)
	} else {
		logger.Debug("Updated dispatcher pod annotation successfully")
	}

	return nil
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
	trigger.Status.SubscriberURI = destination

	return coreconfig.Trigger{
		Attributes:  attributes,
		Destination: destination.String(),
		Id:          string(trigger.UID),
	}, nil
}

func deleteTrigger(triggers []*coreconfig.Trigger, index int) []*coreconfig.Trigger {
	if len(triggers) == 1 {
		return nil
	}

	// replace the trigger to be deleted with the last one.
	triggers[index] = triggers[len(triggers)-1]
	// truncate the array.
	return triggers[:len(triggers)-1]
}

func (r *Reconciler) reconcileKind(ctx context.Context, trigger *eventing.Trigger) reconciler.Event {

	logger := log.Logger(ctx, "reconcile", trigger)

	statusConditionManager := statusConditionManager{
		Trigger:  trigger,
		Configs:  r.Configs,
		Recorder: controller.GetEventRecorder(ctx),
	}

	broker, err := r.BrokerLister.Brokers(trigger.Namespace).Get(trigger.Spec.Broker)
	if err != nil && !apierrors.IsNotFound(err) {
		return statusConditionManager.failedToGetBroker(err)
	}

	if apierrors.IsNotFound(err) {

		// Actually check if the broker doesn't exist.
		broker, err = r.EventingClient.EventingV1(). // Note: do not introduce another `broker` variable with `:`
								Brokers(trigger.Namespace).
								Get(trigger.Spec.Broker, metav1.GetOptions{})

		if apierrors.IsNotFound(err) {

			logger.Debug("broker not found", zap.String("finalizeDuringReconcile", "notFound"))
			// The associated broker doesn't exist anymore, so clean up Trigger resources.
			return r.FinalizeKind(ctx, trigger)
		}
	}

	if !broker.GetDeletionTimestamp().IsZero() {

		logger.Debug("broker deleted", zap.String("finalizeDuringReconcile", "deleted"))

		// The associated broker doesn't exist anymore, so clean up Trigger resources.
		return r.FinalizeKind(ctx, trigger)
	}

	statusConditionManager.propagateBrokerCondition(broker)

	// Get data plane config map.
	dataPlaneConfigMap, err := r.GetOrCreateDataPlaneConfigMap()
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

	brokerIndex := coreconfig.FindBroker(dataPlaneConfig, broker.UID)
	if brokerIndex == coreconfig.NoBroker {
		return statusConditionManager.brokerNotFoundInDataPlaneConfigMap()
	}
	triggerIndex := coreconfig.FindTrigger(dataPlaneConfig.Brokers[brokerIndex].Triggers, trigger.UID)

	triggerConfig, err := r.GetTriggerConfig(trigger)
	if err != nil {
		return statusConditionManager.failedToResolveTriggerConfig(err)
	}

	statusConditionManager.subscriberResolved()

	if triggerIndex == coreconfig.NoTrigger {
		dataPlaneConfig.Brokers[brokerIndex].Triggers = append(
			dataPlaneConfig.Brokers[brokerIndex].Triggers,
			&triggerConfig,
		)
	} else {
		dataPlaneConfig.Brokers[brokerIndex].Triggers[triggerIndex] = &triggerConfig
	}

	// Increment volumeGeneration
	dataPlaneConfig.VolumeGeneration = incrementVolumeGeneration(dataPlaneConfig.VolumeGeneration)

	// Update the configuration map with the new dataPlaneConfig data.
	if err := r.UpdateDataPlaneConfigMap(dataPlaneConfig, dataPlaneConfigMap); err != nil {
		trigger.Status.MarkDependencyFailed(string(brokerreconciler.ConditionConfigMapUpdated), err.Error())
		return err
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

func incrementVolumeGeneration(generation uint64) uint64 {
	return (generation + 1) % (math.MaxUint64 - 1)
}
