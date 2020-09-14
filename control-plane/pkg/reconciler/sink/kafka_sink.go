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

package sink

import (
	"context"
	"math"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/util/retry"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/reconciler"

	eventing "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/eventing/v1alpha1"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	coreconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/core/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/log"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/receiver"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka"
)

type Reconciler struct {
	*base.Reconciler

	ConfigMapLister corelisters.ConfigMapLister

	// ClusterAdmin creates new sarama ClusterAdmin. It's convenient to add this as Reconciler field so that we can
	// mock the function used during the reconciliation loop.
	ClusterAdmin kafka.NewClusterAdminFunc

	Configs *config.Env
}

func (r *Reconciler) ReconcileKind(ctx context.Context, ks *eventing.KafkaSink) reconciler.Event {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		return r.reconcileKind(ctx, ks)
	})
}

func (r *Reconciler) reconcileKind(ctx context.Context, ks *eventing.KafkaSink) error {

	logger := log.Logger(ctx, "reconcile", ks)

	statusConditionManager := base.StatusConditionManager{
		Object:     ks,
		SetAddress: ks.Status.SetAddress,
		Configs:    r.Configs,
		Recorder:   controller.GetEventRecorder(ctx),
	}

	if !r.IsReceiverRunning() {
		return statusConditionManager.DataPlaneNotAvailable()
	}
	statusConditionManager.DataPlaneAvailable()

	if ks.Spec.NumPartitions != nil && ks.Spec.ReplicationFactor != nil {
		topicConfig := &kafka.TopicConfig{
			TopicDetail: sarama.TopicDetail{
				NumPartitions:     *ks.Spec.NumPartitions,
				ReplicationFactor: *ks.Spec.ReplicationFactor,
			},
			BootstrapServers: ks.Spec.BootstrapServers,
		}

		topic, err := r.ClusterAdmin.CreateTopic(logger, ks.Spec.Topic, topicConfig)
		if err != nil {
			return statusConditionManager.FailedToCreateTopic(topic, err)
		}
	}
	statusConditionManager.TopicCreated(ks.Spec.Topic)

	logger.Debug("Topic created", zap.Any("topic", ks.Spec.Topic))

	// Get sinks config map.
	sinksConfigMap, err := r.GetOrCreateDataPlaneConfigMap(ctx)
	if err != nil {
		return statusConditionManager.FailedToGetConfigMap(err)
	}

	logger.Debug("Got sinks config map")

	// Get sinks data.
	sinks, err := r.GetDataPlaneConfigMapData(logger, sinksConfigMap)
	if err != nil && sinks == nil {
		return statusConditionManager.FailedToGetDataFromConfigMap(err)
	}
	if sinks == nil {
		sinks = &coreconfig.Sinks{}
	}

	logger.Debug(
		"Got sinks data from config map",
		zap.Any("sinks", log.NewSinksMarshaller(sinks)),
	)

	// Get sink configuration.
	sinkConfig := &coreconfig.Sink{
		Id:               string(ks.UID),
		Topic:            ks.Spec.Topic,
		Path:             receiver.PathFromObject(ks),
		BootstrapServers: kafka.BootstrapServersCommaSeparated(ks.Spec.BootstrapServers),
		ContentMode:      coreconfig.ContentModeFromString(*ks.Spec.ContentMode),
	}

	sinkIndex := coreconfig.FindSink(sinks, ks.UID)
	// Update sinks data with the new sink configuration.
	coreconfig.AddOrUpdateSinksConfig(sinks, sinkConfig, sinkIndex, logger)

	// Increment volumeGeneration
	sinks.VolumeGeneration = incrementVolumeGeneration(sinks.VolumeGeneration)

	// Update the configuration map with the new sinks data.
	if err := r.UpdateDataPlaneConfigMap(ctx, sinks, sinksConfigMap); err != nil {
		logger.Error("failed to update data plane config map", zap.Error(
			statusConditionManager.FailedToUpdateConfigMap(err),
		))
		return err
	}
	statusConditionManager.ConfigMapUpdated()

	logger.Debug("Config map updated")

	// After #37 we reject events to a non-existing Sink, which means that we cannot consider a Sink Ready if all
	// receivers haven't got the Sink, so update failures to receiver pods is a hard failure.

	// Update volume generation annotation of receiver pods
	if err := r.UpdateReceiverPodsAnnotation(ctx, logger, sinks.VolumeGeneration); err != nil {
		return err
	}

	logger.Debug("Updated receiver pod annotation")

	return statusConditionManager.Reconciled()
}

func (r *Reconciler) FinalizeKind(ctx context.Context, ks *eventing.KafkaSink) reconciler.Event {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		return r.finalizeKind(ctx, ks)
	})
}

func (r *Reconciler) finalizeKind(ctx context.Context, ks *eventing.KafkaSink) error {

	// logger := log.Logger(ctx, "finalize", ks)

	// TODO implement finalizer
	return nil
}

func incrementVolumeGeneration(generation uint64) uint64 {
	return (generation + 1) % (math.MaxUint64 - 1)
}
