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
	"fmt"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/util/retry"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/reconciler"

	eventing "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/eventing/v1alpha1"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
	coreconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/core/config"
	kafkabrokerlogging "knative.dev/eventing-kafka-broker/control-plane/pkg/logging"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/receiver"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/security"
)

const (
	ExternalTopicOwner   = "external"
	ControllerTopicOwner = "kafkasink-controller"
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
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		return r.reconcileKind(ctx, ks)
	})
}

func (r *Reconciler) reconcileKind(ctx context.Context, ks *eventing.KafkaSink) error {
	logger := kafkabrokerlogging.CreateReconcileMethodLogger(ctx, ks)

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

	if ks.GetStatus().Annotations == nil {
		ks.GetStatus().Annotations = make(map[string]string, 1)
	}

	securityOption, secret, err := security.NewOptionFromSecret(ctx, &SecretLocator{KafkaSink: ks}, r.SecretProviderFunc())
	if err != nil {
		return fmt.Errorf("failed to create auth option: %w", err)
	}

	if err := r.TrackSecret(secret, ks); err != nil {
		return fmt.Errorf("failed to track secret: %w", err)
	}

	if ks.Spec.NumPartitions != nil && ks.Spec.ReplicationFactor != nil {

		ks.GetStatus().Annotations[base.TopicOwnerAnnotation] = ControllerTopicOwner

		topicConfig := topicConfigFromSinkSpec(&ks.Spec)

		topic, err := r.ClusterAdmin.CreateTopic(logger, ks.Spec.Topic, topicConfig, securityOption)
		if err != nil {
			return statusConditionManager.FailedToCreateTopic(topic, err)
		}
	} else {

		// If the topic is externally managed, we need to make sure that the topic exists and it's valid.

		ks.GetStatus().Annotations[base.TopicOwnerAnnotation] = ExternalTopicOwner

		isPresentAndValid, err := r.ClusterAdmin.IsTopicPresentAndValid(ks.Spec.Topic, ks.Spec.BootstrapServers, securityOption)
		if err != nil {
			return statusConditionManager.TopicNotPresentOrInvalidErr(err)
		}
		if !isPresentAndValid {
			// The topic might be invalid.
			return statusConditionManager.TopicNotPresentOrInvalid()
		}
	}
	statusConditionManager.TopicReady(ks.Spec.Topic)

	logger.Debug("Topic created", zap.Any("topic", ks.Spec.Topic))

	// Get contract config map.
	contractConfigMap, err := r.GetOrCreateDataPlaneConfigMap(ctx)
	if err != nil {
		return statusConditionManager.FailedToGetConfigMap(err)
	}

	logger.Debug("Got contract config map")

	// Get contract data.
	ct, err := r.GetDataPlaneConfigMapData(logger, contractConfigMap)
	if err != nil && ct == nil {
		return statusConditionManager.FailedToGetDataFromConfigMap(err)
	}
	if ct == nil {
		ct = &contract.Contract{}
	}

	logger.Debug(
		"Got contract data from config map",
		zap.Any("contract", ct),
	)

	// Get sink configuration.
	sinkConfig := &contract.Resource{
		Uid:    string(ks.UID),
		Topics: []string{ks.Spec.Topic},
		Ingress: &contract.Ingress{
			ContentMode: coreconfig.ContentModeFromString(*ks.Spec.ContentMode),
			IngressType: &contract.Ingress_Path{Path: receiver.PathFromObject(ks)},
		},
		BootstrapServers: kafka.BootstrapServersCommaSeparated(ks.Spec.BootstrapServers),
	}
	if ks.Spec.HasAuthConfig() {
		sinkConfig.Auth = &contract.Resource_AuthSecret{
			AuthSecret: &contract.Reference{
				Uuid:      string(secret.UID),
				Namespace: secret.Namespace,
				Name:      secret.Name,
				Version:   secret.ResourceVersion,
			},
		}
	}
	statusConditionManager.ConfigResolved()

	sinkIndex := coreconfig.FindResource(ct, ks.UID)
	// Update contract data with the new sink configuration.
	changed := coreconfig.AddOrUpdateResourceConfig(ct, sinkConfig, sinkIndex, logger)

	coreconfig.IncrementContractGeneration(ct)

	if changed == coreconfig.ResourceChanged {
		// Update the configuration map with the new contract data.
		if err := r.UpdateDataPlaneConfigMap(ctx, ct, contractConfigMap); err != nil {
			logger.Error("failed to update data plane config map", zap.Error(
				statusConditionManager.FailedToUpdateConfigMap(err),
			))
			return err
		}
	}
	statusConditionManager.ConfigMapUpdated()

	logger.Debug("Config map updated")

	if changed == coreconfig.ResourceChanged {
		// After #37 we reject events to a non-existing Sink, which means that we cannot consider a Sink Ready if all
		// receivers haven't got the Sink, so update failures to receiver pods is a hard failure.

		// Update volume generation annotation of receiver pods
		if err := r.UpdateReceiverPodsAnnotation(ctx, logger, ct.Generation); err != nil {
			return err
		}

		logger.Debug("Updated receiver pod annotation")
	}

	return statusConditionManager.Reconciled()
}

func (r *Reconciler) FinalizeKind(ctx context.Context, ks *eventing.KafkaSink) reconciler.Event {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		return r.finalizeKind(ctx, ks)
	})
}

func (r *Reconciler) finalizeKind(ctx context.Context, ks *eventing.KafkaSink) error {
	logger := kafkabrokerlogging.CreateFinalizeMethodLogger(ctx, ks)

	// Get contract config map.
	contractConfigMap, err := r.GetOrCreateDataPlaneConfigMap(ctx)
	if err != nil {
		return fmt.Errorf("failed to get contract config map %s: %w", r.Configs.DataPlaneConfigMapAsString(), err)
	}

	logger.Debug("Got contract config map")

	// Get contract data.
	ct, err := r.GetDataPlaneConfigMapData(logger, contractConfigMap)
	if err != nil {
		return fmt.Errorf("failed to get contract: %w", err)
	}

	logger.Debug(
		"Got contract data from config map",
		zap.Any("contract", ct),
	)

	sinkIndex := coreconfig.FindResource(ct, ks.UID)
	if sinkIndex != coreconfig.NoResource {
		coreconfig.DeleteResource(ct, sinkIndex)

		logger.Debug("Sink deleted", zap.Int("index", sinkIndex))

		coreconfig.IncrementContractGeneration(ct)

		// Update the configuration map with the new contract data.
		if err := r.UpdateDataPlaneConfigMap(ctx, ct, contractConfigMap); err != nil {
			return err
		}

		logger.Debug("Sinks config map updated")
	}

	if ks.GetStatus().Annotations[base.TopicOwnerAnnotation] == ControllerTopicOwner {
		securityOption, _, err := security.NewOptionFromSecret(ctx, &SecretLocator{KafkaSink: ks}, r.SecretProviderFunc())
		if err != nil {
			return fmt.Errorf("failed to create security (auth) option: %w", err)
		}
		topic, err := r.ClusterAdmin.DeleteTopic(ks.Spec.Topic, ks.Spec.BootstrapServers, securityOption)
		if err != nil {
			return err
		}
		logger.Debug("Topic deleted", zap.String("topic", topic))
	}

	return nil
}

func topicConfigFromSinkSpec(kss *eventing.KafkaSinkSpec) *kafka.TopicConfig {
	return &kafka.TopicConfig{
		TopicDetail: sarama.TopicDetail{
			NumPartitions:     *kss.NumPartitions,
			ReplicationFactor: *kss.ReplicationFactor,
		},
		BootstrapServers: kss.BootstrapServers,
	}
}
