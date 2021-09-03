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

package channel

import (
	"context"
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/util/retry"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/receiver"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/security"
	messagingv1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	commonconfig "knative.dev/eventing-kafka/pkg/common/config"
	"knative.dev/eventing-kafka/pkg/common/constants"
	commonsarama "knative.dev/eventing-kafka/pkg/common/kafka/sarama"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/reconciler"
	"knative.dev/pkg/resolver"
	"knative.dev/pkg/system"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	coreconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/core/config"
	kafkalogging "knative.dev/eventing-kafka-broker/control-plane/pkg/logging"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka"
)

const (
	// TopicPrefix is the Kafka Channel topic prefix - (topic name: knative-messaging-kafka.<channel-namespace>.<channel-name>).
	TopicPrefix = "knative-messaging-kafka"
)

type Configs struct {
	config.Env

	BootstrapServers string
}

type Reconciler struct {
	*base.Reconciler

	Resolver *resolver.URIResolver

	// ClusterAdmin creates new sarama ClusterAdmin. It's convenient to add this as Reconciler field so that we can
	// mock the function used during the reconciliation loop.
	ClusterAdmin kafka.NewClusterAdminFunc

	ConfigMapLister corelisters.ConfigMapLister

	Configs *Configs
}

func (r *Reconciler) ReconcileKind(ctx context.Context, channel *messagingv1beta1.KafkaChannel) reconciler.Event {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		return r.reconcileKind(ctx, channel)
	})
}

func (r *Reconciler) reconcileKind(ctx context.Context, channel *messagingv1beta1.KafkaChannel) reconciler.Event {
	logger := kafkalogging.CreateReconcileMethodLogger(ctx, channel)

	statusConditionManager := base.StatusConditionManager{
		Object:     channel,
		SetAddress: channel.Status.SetAddress,
		Configs:    &r.Configs.Env,
		Recorder:   controller.GetEventRecorder(ctx),
	}

	// do not proceed, if data plane is not available
	if !r.IsReceiverRunning() || !r.IsDispatcherRunning() {
		return statusConditionManager.DataPlaneNotAvailable()
	}
	statusConditionManager.DataPlaneAvailable()

	// get the channel configmap
	channelConfigMap, err := r.channelConfigMap()
	if err != nil {
		return statusConditionManager.FailedToResolveConfig(err)
	}
	logger.Debug("configmap read", zap.Any("configmap", channelConfigMap))

	// TODO: configmap tracking

	// parse the config
	eventingKafkaSettings, err := commonsarama.LoadEventingKafkaSettings(channelConfigMap.Data)
	if err != nil {
		return statusConditionManager.FailedToResolveConfig(err)
	}
	logger.Debug("config parsed", zap.Any("eventingKafkaSettings", eventingKafkaSettings))

	// get topic config
	topicConfig, err := r.topicConfig(eventingKafkaSettings)
	if err != nil {
		return statusConditionManager.FailedToResolveConfig(err)
	}
	logger.Debug("config resolved", zap.Any("config", topicConfig))
	statusConditionManager.ConfigResolved()

	// get the secret to access Kafka
	secret, err := r.secret(ctx, channelConfigMap)
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
	saramaSecurityOption := security.NewOptionFromSecret(secret)

	// TODO: secret tracking

	// create the topic
	topic, err := r.ClusterAdmin.CreateTopicIfDoesntExist(logger, topic(TopicPrefix, channel), topicConfig, saramaSecurityOption)
	if err != nil {
		return statusConditionManager.FailedToCreateTopic(topic, err)
	}
	logger.Debug("Topic created", zap.Any("topic", topic))
	statusConditionManager.TopicReady(topic)

	// Get contract config map to write into it.
	contractConfigMap, err := r.GetOrCreateDataPlaneConfigMap(ctx)
	if err != nil {
		return statusConditionManager.FailedToGetConfigMap(err)
	}
	logger.Debug("Got contract config map")

	// Get contract data
	ct, err := r.GetDataPlaneConfigMapData(logger, contractConfigMap)
	if err != nil && ct == nil {
		return statusConditionManager.FailedToGetDataFromConfigMap(err)
	}
	logger.Debug("Got contract data from config map", zap.Any(base.ContractLogKey, ct))

	// Get resource configuration
	channelResource, err := r.getChannelContractResource(ctx, topic, channel, secret, topicConfig)
	if err != nil {
		return statusConditionManager.FailedToGetConfig(err)
	}

	// Update contract data with the new contract configuration
	channelIndex := coreconfig.FindResource(ct, channel.UID)
	changed := coreconfig.AddOrUpdateResourceConfig(ct, channelResource, channelIndex, logger)
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

	// We update receiver and dispatcher pods annotation regardless of our contract changed or not due to the fact
	// that in a previous reconciliation we might have failed to update one of our data plane pod annotation, so we want
	// to anyway update remaining annotations with the contract generation that was saved in the CM.

	// We reject events to a non-existing Channel, which means that we cannot consider a Channel Ready if all
	// receivers haven't got the Channel, so update failures to receiver pods is a hard failure.
	// On the other side, dispatcher pods care about Subscriptions, and the Channel object is used as a configuration
	// prototype for all associated Subscriptions, so we consider that it's fine on the dispatcher side to receive eventually
	// the update even if here eventually means seconds or minutes after the actual update.

	// Update volume generation annotation of receiver pods
	if err := r.UpdateReceiverPodsAnnotation(ctx, logger, ct.Generation); err != nil {
		logger.Error("Failed to update receiver pod annotation", zap.Error(
			statusConditionManager.FailedToUpdateReceiverPodsAnnotation(err),
		))
		return err
	}
	logger.Debug("Updated receiver pod annotation")

	// Update volume generation annotation of dispatcher pods
	if err := r.UpdateDispatcherPodsAnnotation(ctx, logger, ct.Generation); err != nil {
		// Failing to update dispatcher pods annotation leads to config map refresh delayed by several seconds.
		// Since the dispatcher side is the consumer side, we don't lose availability, and we can consider the Channel
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

//nolint
func (r *Reconciler) FinalizeKind(ctx context.Context, channel *messagingv1beta1.KafkaChannel) reconciler.Event {
	// TODO: noop for now
	return nil
}

func (r *Reconciler) channelConfigMap() (*corev1.ConfigMap, error) {
	// TODO: do we want to support namespaced channels? they're not supported at the moment.

	namespace := system.Namespace()
	cm, err := r.ConfigMapLister.ConfigMaps(namespace).Get(constants.SettingsConfigMapName)
	if err != nil {
		return nil, fmt.Errorf("failed to get configmap %s/%s: %w", namespace, constants.SettingsConfigMapName, err)
	}

	return cm, nil
}

//nolint
func (r *Reconciler) topicConfig(eventingKafkaConfig *commonconfig.EventingKafkaConfig) (*kafka.TopicConfig, error) {
	// TODO: all hardcoded for now
	return &kafka.TopicConfig{
		TopicDetail: sarama.TopicDetail{
			NumPartitions:     1,
			ReplicationFactor: 1,
			ReplicaAssignment: nil,
			ConfigEntries:     nil,
		},
		BootstrapServers: strings.Split(eventingKafkaConfig.Kafka.Brokers, ","),
	}, nil
}

func (r *Reconciler) secret(ctx context.Context, channelConfig *corev1.ConfigMap) (*corev1.Secret, error) {
	return security.Secret(ctx, &security.MTConfigMapSecretLocator{ConfigMap: channelConfig}, r.SecretProviderFunc())
}

func (r *Reconciler) getChannelContractResource(ctx context.Context, topic string, channel *messagingv1beta1.KafkaChannel, secret *corev1.Secret, config *kafka.TopicConfig) (*contract.Resource, error) {
	resource := &contract.Resource{
		Uid:    string(channel.UID),
		Topics: []string{topic},
		Ingress: &contract.Ingress{
			IngressType: &contract.Ingress_Path{
				Path: receiver.PathFromObject(channel),
			},
		},
		BootstrapServers: config.GetBootstrapServers(),
	}

	if secret != nil {
		resource.Auth = &contract.Resource_AuthSecret{
			AuthSecret: &contract.Reference{
				Uuid:      string(secret.UID),
				Namespace: secret.Namespace,
				Name:      secret.Name,
				Version:   secret.ResourceVersion,
			},
		}
	}

	egressConfig, err := coreconfig.EgressConfigFromDelivery(ctx, r.Resolver, channel, channel.Spec.Delivery, r.Configs.DefaultBackoffDelayMs)
	if err != nil {
		return nil, err
	}
	resource.EgressConfig = egressConfig

	return resource, nil
}

// Topic returns a topic name given a topic prefix and a generic object.
// This function uses a different format than the kafkatopic.Topic function
func topic(prefix string, obj metav1.Object) string {
	return fmt.Sprintf("%s.%s.%s", prefix, obj.GetNamespace(), obj.GetName())
}
