/*
 * Copyright 2021 The Knative Authors
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

package source

import (
	"context"
	"fmt"
	"strings"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/util/retry"
	sources "knative.dev/eventing-kafka/pkg/apis/sources/v1beta1"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/reconciler"
	"knative.dev/pkg/resolver"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
	coreconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/core/config"
	kafkalogging "knative.dev/eventing-kafka-broker/control-plane/pkg/logging"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/security"
)

var (
	DefaultEgressConfig = contract.EgressConfig{
		Retry:         10,
		BackoffPolicy: contract.BackoffPolicy_Exponential,
		BackoffDelay:  10000, // 10 seconds
		Timeout:       0,
	}
)

const (
	DefaultDeliveryOrder = contract.DeliveryOrder_ORDERED
)

type Reconciler struct {
	*base.Reconciler
	*config.Env

	Resolver *resolver.URIResolver

	// NewKafkaClusterAdmin creates new sarama ClusterAdmin. It's convenient to add this as Reconciler field so that we can
	// mock the function used during the reconciliation loop.
	NewKafkaClusterAdmin kafka.NewClusterAdminFunc
}

func (r *Reconciler) ReconcileKind(ctx context.Context, ks *sources.KafkaSource) reconciler.Event {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		return r.reconcileKind(ctx, ks)
	})
}

func (r *Reconciler) reconcileKind(ctx context.Context, ks *sources.KafkaSource) reconciler.Event {

	logger := kafkalogging.CreateReconcileMethodLogger(ctx, ks)

	statusConditionManager := base.StatusConditionManager{
		Object:   ks,
		Configs:  r.Env,
		Recorder: controller.GetEventRecorder(ctx),
	}

	if !r.IsDispatcherRunning() {
		return statusConditionManager.DataPlaneNotAvailable()
	}
	statusConditionManager.DataPlaneAvailable()

	secret, err := security.Secret(ctx, &SecretLocator{KafkaSource: ks}, r.SecretProviderFunc())
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

	saramaConfig, err := kafka.GetClusterAdminSaramaConfig(securityOption)
	if err != nil {
		return fmt.Errorf("error getting cluster admin sarama config: %w", err)
	}

	kafkaClusterAdmin, err := r.NewKafkaClusterAdmin(ks.Spec.BootstrapServers, saramaConfig)
	if err != nil {
		return fmt.Errorf("cannot obtain Kafka cluster admin, %w", err)
	}
	defer kafkaClusterAdmin.Close()

	isValid, err := kafka.AreTopicsPresentAndValid(kafkaClusterAdmin, ks.Spec.Topics...)
	if err != nil {
		return statusConditionManager.TopicsNotPresentOrInvalidErr(ks.Spec.Topics, err)
	}
	if !isValid {
		return statusConditionManager.TopicsNotPresentOrInvalid(ks.Spec.Topics)
	}
	statusConditionManager.TopicReady(strings.Join(ks.Spec.Topics, ", "))

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

	logger.Debug("Got contract data from config map", zap.Any(base.ContractLogKey, ct))

	// Get resource configuration.
	resource, err := r.reconcileKafkaSourceResource(ctx, ks, secret)
	if err != nil {
		return statusConditionManager.FailedToGetConfig(err)
	}

	brokerIndex := coreconfig.FindResource(ct, ks.GetUID())
	// Update contract data with the new contract configuration
	changed := coreconfig.AddOrUpdateResourceConfig(ct, resource, brokerIndex, logger)

	logger.Debug("Change detector", zap.Int("changed", changed))

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
		logger.Debug("Contract config map updated")
	}
	statusConditionManager.ConfigMapUpdated()

	// We update dispatcher pods annotation regardless of our contract changed or not due to the fact
	// that in a previous reconciliation we might have failed to update one of our data plane pod annotation, so we want
	// to anyway update remaining annotations with the contract generation that was saved in the CM.

	// Update volume generation annotation of dispatcher pods
	if err := r.UpdateDispatcherPodsAnnotation(ctx, logger, ct.Generation); err != nil {
		// Failing to update dispatcher pods annotation leads to config map refresh delayed by several seconds.
		// Since the dispatcher side is the consumer side, we don't lose availability, and we can consider the Broker
		// ready. So, log out the error and move on to the next step.
		logger.Warn(
			"Failed to update dispatcher pod annotation to trigger an immediate config map refresh",
			zap.Error(err),
		)

		statusConditionManager.FailedToUpdateDispatcherPodsAnnotation(err)
	} else {
		logger.Debug("Updated dispatcher pod annotation")
	}

	return statusConditionManager.Reconciled()
}

func (r *Reconciler) FinalizeKind(ctx context.Context, ks *sources.KafkaSource) reconciler.Event {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		return r.finalizeKind(ctx, ks)
	})
}

func (r *Reconciler) finalizeKind(ctx context.Context, ks *sources.KafkaSource) reconciler.Event {
	logger := kafkalogging.CreateFinalizeMethodLogger(ctx, ks)

	// Get contract config map.
	contractConfigMap, err := r.GetOrCreateDataPlaneConfigMap(ctx)
	if err != nil {
		return fmt.Errorf("failed to get contract config map %s: %w", r.Env.DataPlaneConfigMapAsString(), err)
	}

	logger.Debug("Got contract config map")

	// Get contract data.
	ct, err := r.GetDataPlaneConfigMapData(logger, contractConfigMap)
	if err != nil {
		return fmt.Errorf("failed to get contract: %w", err)
	}

	logger.Debug("Got contract data from config map", zap.Any(base.ContractLogKey, ct))

	if err := r.DeleteResource(ctx, logger, ks.GetUID(), ct, contractConfigMap); err != nil {
		return err
	}

	// We update dispatcher pods annotation regardless of our contract changed or not due to the fact
	// that in a previous reconciliation we might have failed to update one of our data plane pod annotation, so we want
	// to update anyway remaining annotations with the contract generation that was saved in the CM.
	// Note: if there aren't changes to be done at the pod annotation level, we just skip the update.

	// Update volume generation annotation of dispatcher pods
	if err := r.UpdateDispatcherPodsAnnotation(ctx, logger, ct.Generation); err != nil {
		return err
	}

	return nil
}

func (r *Reconciler) reconcileKafkaSourceResource(ctx context.Context, ks *sources.KafkaSource, secret *corev1.Secret) (*contract.Resource, error) {
	destination, err := r.Resolver.URIFromDestinationV1(ctx, ks.Spec.Sink, ks)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve destination: %w", err)
	}

	egressConfig := proto.Clone(&DefaultEgressConfig).(*contract.EgressConfig)

	egress := &contract.Egress{
		ConsumerGroup: ks.Spec.ConsumerGroup,
		Destination:   destination.String(),
		ReplyStrategy: &contract.Egress_DiscardReply{},
		Uid:           string(ks.GetUID()),
		EgressConfig:  egressConfig,
		DeliveryOrder: DefaultDeliveryOrder,
	}
	// Set key type hint (if any).
	if keyType, ok := ks.Labels[sources.KafkaKeyTypeLabel]; ok {
		egress.KeyType = coreconfig.KeyTypeFromString(keyType)
	}
	resource := &contract.Resource{
		Uid:              string(ks.GetUID()),
		Topics:           ks.Spec.Topics,
		BootstrapServers: strings.Join(ks.Spec.BootstrapServers, ","),
		Egresses:         []*contract.Egress{egress},
		Auth:             &contract.Resource_AbsentAuth{},
		Reference: &contract.Reference{
			Namespace: ks.GetNamespace(),
			Name:      ks.GetName(),
		},
	}
	if ks.Spec.CloudEventOverrides != nil {
		resource.CloudEventOverrides = &contract.CloudEventOverrides{
			Extensions: ks.Spec.CloudEventOverrides.Extensions,
		}
	}
	if secret != nil {
		resource.Auth = &contract.Resource_AuthSecret{
			AuthSecret: &contract.Reference{
				Uuid:      string(secret.GetUID()),
				Namespace: secret.GetNamespace(),
				Name:      secret.GetName(),
				Version:   secret.GetResourceVersion(),
			},
		}
	}
	return resource, nil
}
