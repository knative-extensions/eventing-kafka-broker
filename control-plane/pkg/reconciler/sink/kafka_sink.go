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
	"time"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/types"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/util/retry"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/reconciler"

	eventing "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/eventing/v1alpha1"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
	coreconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/core/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	kafkalogging "knative.dev/eventing-kafka-broker/control-plane/pkg/logging"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/prober"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/receiver"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/security"
)

const (
	ExternalTopicOwner   = "external"
	ControllerTopicOwner = "kafkasink-controller"
)

type Reconciler struct {
	*base.Reconciler
	*config.Env

	ConfigMapLister corelisters.ConfigMapLister

	// NewKafkaClusterAdminClient creates new sarama ClusterAdmin. It's convenient to add this as Reconciler field so that we can
	// mock the function used during the reconciliation loop.
	NewKafkaClusterAdminClient kafka.NewClusterAdminClientFunc

	Prober prober.Prober

	IngressHost string
}

func (r *Reconciler) ReconcileKind(ctx context.Context, ks *eventing.KafkaSink) reconciler.Event {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		return r.reconcileKind(ctx, ks)
	})
}

func (r *Reconciler) reconcileKind(ctx context.Context, ks *eventing.KafkaSink) error {
	logger := kafkalogging.CreateReconcileMethodLogger(ctx, ks)

	statusConditionManager := base.StatusConditionManager{
		Object:     ks,
		SetAddress: ks.Status.SetAddress,
		Env:        r.Env,
		Recorder:   controller.GetEventRecorder(ctx),
	}

	if !r.IsReceiverRunning(r.Env.SystemNamespace) {
		return statusConditionManager.DataPlaneNotAvailable()
	}
	statusConditionManager.DataPlaneAvailable()

	if ks.GetStatus().Annotations == nil {
		ks.GetStatus().Annotations = make(map[string]string, 1)
	}

	secret, err := security.Secret(ctx, &SecretLocator{KafkaSink: ks}, r.SecretProviderFunc())
	if err != nil {
		return fmt.Errorf("failed to get secret: %w", err)
	}
	if secret != nil {
		logger.Debug("Secret reference",
			zap.String("apiVersion", secret.APIVersion),
			zap.String("name", secret.Name),
			zap.String("namespace", secret.Namespace),
			zap.String("kind", secret.Kind),
		)
	}

	// get security option for Sarama with secret info in it
	securityOption := security.NewSaramaSecurityOptionFromSecret(secret)

	if err := r.TrackSecret(secret, ks); err != nil {
		return fmt.Errorf("failed to track secret: %w", err)
	}

	saramaConfig, err := kafka.GetSaramaConfig(securityOption)
	if err != nil {
		return fmt.Errorf("error getting cluster admin sarama config: %w", err)
	}

	kafkaClusterAdminClient, err := r.NewKafkaClusterAdminClient(ks.Spec.BootstrapServers, saramaConfig)
	if err != nil {
		return fmt.Errorf("cannot obtain Kafka cluster admin, %w", err)
	}
	defer kafkaClusterAdminClient.Close()

	if ks.Spec.NumPartitions != nil && ks.Spec.ReplicationFactor != nil {

		ks.GetStatus().Annotations[base.TopicOwnerAnnotation] = ControllerTopicOwner

		topicConfig := topicConfigFromSinkSpec(&ks.Spec)

		topic, err := kafka.CreateTopicIfDoesntExist(kafkaClusterAdminClient, logger, ks.Spec.Topic, topicConfig)
		if err != nil {
			return statusConditionManager.FailedToCreateTopic(topic, err)
		}
	} else {

		// If the topic is externally managed, we need to make sure that the topic exists and it's valid.
		ks.GetStatus().Annotations[base.TopicOwnerAnnotation] = ExternalTopicOwner

		isPresentAndValid, err := kafka.AreTopicsPresentAndValid(kafkaClusterAdminClient, ks.Spec.Topic)
		if err != nil {
			return statusConditionManager.TopicsNotPresentOrInvalidErr([]string{ks.Spec.Topic}, err)
		}
		if !isPresentAndValid {
			// The topic might be invalid.
			return statusConditionManager.TopicsNotPresentOrInvalid([]string{ks.Spec.Topic})
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
			Path:        receiver.PathFromObject(ks),
		},
		BootstrapServers: kafka.BootstrapServersCommaSeparated(ks.Spec.BootstrapServers),
		Reference: &contract.Reference{
			Uuid:      string(ks.GetUID()),
			Namespace: ks.GetNamespace(),
			Name:      ks.GetName(),
		},
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

	if changed == coreconfig.ResourceChanged {
		// Resource changed, increment contract generation.
		coreconfig.IncrementContractGeneration(ct)
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

	// We update receiver pods annotation regardless of our contract changed or not due to the fact  that in a previous
	// reconciliation we might have failed to update one of our data plane pod annotation, so we want to anyway update
	// remaining annotations with the contract generation that was saved in the CM.
	// Note: if there aren't changes to be done at the pod annotation level, we just skip the update.

	// Since we reject events to a non-existing Sink, which means that we cannot consider a Sink Ready if all
	// receivers haven't got the Sink, so update failures to receiver pods is a hard failure.

	// Update volume generation annotation of receiver pods
	if err := r.UpdateReceiverPodsAnnotation(ctx, logger, ct.Generation); err != nil {
		return err
	}

	logger.Debug("Updated receiver pod annotation")

	address := receiver.Address(r.IngressHost, ks)
	proberAddressable := prober.Addressable{
		Address: address,
		ResourceKey: types.NamespacedName{
			Namespace: ks.GetNamespace(),
			Name:      ks.GetName(),
		},
	}

	if status := r.Prober.Probe(ctx, proberAddressable, prober.StatusReady); status != prober.StatusReady {
		statusConditionManager.ProbesStatusNotReady(status)
		return nil // Object will get re-queued once probe status changes.
	}
	statusConditionManager.Addressable(address)

	return nil
}

func (r *Reconciler) FinalizeKind(ctx context.Context, ks *eventing.KafkaSink) reconciler.Event {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		return r.finalizeKind(ctx, ks)
	})
}

func (r *Reconciler) finalizeKind(ctx context.Context, ks *eventing.KafkaSink) error {
	logger := kafkalogging.CreateFinalizeMethodLogger(ctx, ks)

	// Get contract config map.
	contractConfigMap, err := r.GetOrCreateDataPlaneConfigMap(ctx)
	if err != nil {
		return fmt.Errorf("failed to get contract config map %s: %w", r.DataPlaneConfigMapAsString(), err)
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

	if err := r.DeleteResource(ctx, logger, ks.GetUID(), ct, contractConfigMap); err != nil {
		return err
	}

	ks.Status.Address.URL = nil

	// We update receiver pods annotation regardless of our contract changed or not due to the fact  that in a previous
	// reconciliation we might have failed to update one of our data plane pod annotation, so we want to anyway update
	// remaining annotations with the contract generation that was saved in the CM.
	// Note: if there aren't changes to be done at the pod annotation level, we just skip the update.

	// Update volume generation annotation of receiver pods
	if err := r.UpdateReceiverPodsAnnotation(ctx, logger, ct.Generation); err != nil {
		return err
	}

	//  Rationale: after deleting a topic closing a producer ends up blocking and requesting metadata for max.block.ms
	//  because topic metadata aren't available anymore.
	// 	See (under discussions KIPs, unlikely to be accepted as they are):
	// 	- https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=181306446
	// 	- https://cwiki.apache.org/confluence/display/KAFKA/KIP-286%3A+producer.send%28%29+should+not+block+on+metadata+update
	address := receiver.Address(r.IngressHost, ks)
	proberAddressable := prober.Addressable{
		Address: address,
		ResourceKey: types.NamespacedName{
			Namespace: ks.GetNamespace(),
			Name:      ks.GetName(),
		},
	}
	if status := r.Prober.Probe(ctx, proberAddressable, prober.StatusNotReady); status != prober.StatusNotReady {
		// Return a requeueKeyError that doesn't generate an event and it re-queues the object
		// for a new reconciliation.
		return controller.NewRequeueAfter(5 * time.Second)
	}

	if ks.GetStatus().Annotations[base.TopicOwnerAnnotation] == ControllerTopicOwner {
		secret, err := security.Secret(ctx, &SecretLocator{KafkaSink: ks}, r.SecretProviderFunc())
		if err != nil {
			return fmt.Errorf("failed to get secret: %w", err)
		}
		if secret != nil {
			logger.Debug("Secret reference",
				zap.String("apiVersion", secret.APIVersion),
				zap.String("name", secret.Name),
				zap.String("namespace", secret.Namespace),
				zap.String("kind", secret.Kind),
			)
		}

		// get security option for Sarama with secret info in it
		securityOption := security.NewSaramaSecurityOptionFromSecret(secret)

		saramaConfig, err := kafka.GetSaramaConfig(securityOption)
		if err != nil {
			// even in error case, we return `normal`, since we are fine with leaving the
			// topic undeleted e.g. when we lose connection
			return fmt.Errorf("error getting cluster admin sarama config: %w", err)
		}

		kafkaClusterAdminClient, err := r.NewKafkaClusterAdminClient(ks.Spec.BootstrapServers, saramaConfig)
		if err != nil {
			// even in error case, we return `normal`, since we are fine with leaving the
			// topic undeleted e.g. when we lose connection
			return fmt.Errorf("cannot obtain Kafka cluster admin, %w", err)
		}
		defer kafkaClusterAdminClient.Close()

		topic, err := kafka.DeleteTopic(kafkaClusterAdminClient, ks.Spec.Topic)
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
