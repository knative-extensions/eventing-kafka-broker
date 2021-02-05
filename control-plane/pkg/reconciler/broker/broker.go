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
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/util/retry"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/reconciler"
	"knative.dev/pkg/resolver"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
	coreconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/core/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/log"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/receiver"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/security"
)

const (
	// TopicPrefix is the Kafka Broker topic prefix - (topic name: knative-broker-<broker-namespace>-<broker-name>).
	TopicPrefix = "knative-broker-"
)

type Configs struct {
	config.Env

	BootstrapServers string
}

type Reconciler struct {
	*base.Reconciler

	Resolver *resolver.URIResolver

	KafkaDefaultTopicDetails     sarama.TopicDetail
	KafkaDefaultTopicDetailsLock sync.RWMutex
	bootstrapServers             []string
	bootstrapServersLock         sync.RWMutex
	ConfigMapLister              corelisters.ConfigMapLister

	// ClusterAdmin creates new sarama ClusterAdmin. It's convenient to add this as Reconciler field so that we can
	// mock the function used during the reconciliation loop.
	ClusterAdmin kafka.NewClusterAdminFunc

	Configs *Configs
}

func (r *Reconciler) ReconcileKind(ctx context.Context, broker *eventing.Broker) reconciler.Event {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		return r.reconcileKind(ctx, broker)
	})
}

func (r *Reconciler) reconcileKind(ctx context.Context, broker *eventing.Broker) reconciler.Event {

	logger := log.Logger(ctx, "reconcile", broker)

	statusConditionManager := base.StatusConditionManager{
		Object:     broker,
		SetAddress: broker.Status.SetAddress,
		Configs:    &r.Configs.Env,
		Recorder:   controller.GetEventRecorder(ctx),
	}

	if !r.IsReceiverRunning() || !r.IsDispatcherRunning() {
		return statusConditionManager.DataPlaneNotAvailable()
	}
	statusConditionManager.DataPlaneAvailable()

	topicConfig, brokerConfig, err := r.topicConfig(logger, broker)
	if err != nil {
		return statusConditionManager.FailedToResolveConfig(err)
	}
	statusConditionManager.ConfigResolved()

	if err := r.TrackConfigMap(brokerConfig, broker); err != nil {
		return fmt.Errorf("failed to track broker config: %w", err)
	}

	logger.Debug("config resolved", zap.Any("config", topicConfig))

	securityOption, secret, err := security.NewOptionFromSecret(ctx, &security.MTConfigMapSecretLocator{ConfigMap: brokerConfig}, r.SecretProviderFunc())
	if err != nil {
		return fmt.Errorf("failed to create security (auth) option: %w", err)
	}

	if secret != nil {
		logger.Debug("Secret reference",
			zap.String("apiVersion", secret.APIVersion),
			zap.String("name", secret.Name),
			zap.String("namespace", secret.Namespace),
			zap.String("kind", secret.Kind),
		)
	}

	if err := r.TrackSecret(secret, broker); err != nil {
		return fmt.Errorf("failed to track secret: %w", err)
	}

	topic, err := r.ClusterAdmin.CreateTopic(logger, kafka.Topic(TopicPrefix, broker), topicConfig, securityOption)
	if err != nil {
		return statusConditionManager.FailedToCreateTopic(topic, err)
	}
	statusConditionManager.TopicReady(topic)

	logger.Debug("Topic created", zap.Any("topic", topic))

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

	logger.Debug(
		"Got contract data from config map",
		zap.Any(base.ContractLogKey, (*log.ContractMarshaller)(ct)),
	)

	// Get resource configuration.
	brokerResource, err := r.getBrokerResource(ctx, topic, broker, secret, topicConfig)
	if err != nil {
		return statusConditionManager.FailedToGetConfig(err)
	}

	brokerIndex := coreconfig.FindResource(ct, broker.UID)
	// Update contract data with the new contract configuration
	changed := coreconfig.AddOrUpdateResourceConfig(ct, brokerResource, brokerIndex, logger)

	logger.Debug("Change detector", zap.Int("changed", changed))

	coreconfig.IncrementContractGeneration(ct)

	if changed == coreconfig.ResourceChanged {
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

	if changed == coreconfig.ResourceChanged {

		// After #37 we reject events to a non-existing Broker, which means that we cannot consider a Broker Ready if all
		// receivers haven't got the Broker, so update failures to receiver pods is a hard failure.
		// On the other side, dispatcher pods care about Triggers, and the Broker object is used as a configuration
		// prototype for all associated Triggers, so we consider that it's fine on the dispatcher side to receive eventually
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
	}

	return statusConditionManager.Reconciled()
}

func (r *Reconciler) FinalizeKind(ctx context.Context, broker *eventing.Broker) reconciler.Event {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		return r.finalizeKind(ctx, broker)
	})
}

func (r *Reconciler) finalizeKind(ctx context.Context, broker *eventing.Broker) reconciler.Event {

	logger := log.Logger(ctx, "finalize", broker)

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
		zap.Any(base.ContractLogKey, (*log.ContractMarshaller)(ct)),
	)

	brokerIndex := coreconfig.FindResource(ct, broker.UID)
	if brokerIndex != coreconfig.NoResource {
		coreconfig.DeleteResource(ct, brokerIndex)

		logger.Debug("Broker deleted", zap.Int("index", brokerIndex))

		coreconfig.IncrementContractGeneration(ct)

		// Update the configuration map with the new contract data.
		if err := r.UpdateDataPlaneConfigMap(ctx, ct, contractConfigMap); err != nil {
			return err
		}

		logger.Debug("Contract config map updated")

		// There is no need to update volume generation and dispatcher pod annotation, updates to the config map will
		// eventually be seen by the dispatcher pod and resources will be deleted accordingly.
	}

	topicConfig, brokerConfig, err := r.topicConfig(logger, broker)
	if err != nil {
		return fmt.Errorf("failed to resolve broker config: %w", err)
	}

	authProvider := &security.MTConfigMapSecretLocator{ConfigMap: brokerConfig}
	securityOption, _, err := security.NewOptionFromSecret(ctx, authProvider, r.SecretProviderFunc())
	if err != nil {
		return fmt.Errorf("failed to create security (auth) option: %w", err)
	}

	topic, err := r.ClusterAdmin.DeleteTopic(kafka.Topic(TopicPrefix, broker), topicConfig.BootstrapServers, securityOption)
	if err != nil {
		return err
	}

	logger.Debug("Topic deleted", zap.String("topic", topic))

	return nil
}

func (r *Reconciler) topicConfig(logger *zap.Logger, broker *eventing.Broker) (*kafka.TopicConfig, *corev1.ConfigMap, error) {

	logger.Debug("broker config", zap.Any("broker.spec.config", broker.Spec.Config))

	if broker.Spec.Config == nil {
		tc, err := r.defaultConfig()
		return tc, nil, err
	}

	if strings.ToLower(broker.Spec.Config.Kind) != "configmap" { // TODO: is there any constant?
		return nil, nil, fmt.Errorf("supported config Kind: ConfigMap - got %s", broker.Spec.Config.Kind)
	}

	namespace := broker.Spec.Config.Namespace
	if namespace == "" {
		// Namespace not specified, use broker namespace.
		namespace = broker.Namespace
	}
	cm, err := r.ConfigMapLister.ConfigMaps(namespace).Get(broker.Spec.Config.Name)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get configmap %s/%s: %w", namespace, broker.Spec.Config.Name, err)
	}

	brokerConfig, err := configFromConfigMap(logger, cm)
	if err != nil {
		return nil, cm, err
	}

	return brokerConfig, cm, nil
}

func (r *Reconciler) defaultTopicDetail() sarama.TopicDetail {
	r.KafkaDefaultTopicDetailsLock.RLock()
	defer r.KafkaDefaultTopicDetailsLock.RUnlock()

	// copy the default topic details
	topicDetail := r.KafkaDefaultTopicDetails
	return topicDetail
}

func (r *Reconciler) defaultConfig() (*kafka.TopicConfig, error) {
	bootstrapServers, err := r.getDefaultBootstrapServersOrFail()
	if err != nil {
		return nil, err
	}

	return &kafka.TopicConfig{
		TopicDetail:      r.defaultTopicDetail(),
		BootstrapServers: bootstrapServers,
	}, nil
}

func (r *Reconciler) getBrokerResource(ctx context.Context, topic string, broker *eventing.Broker, secret *corev1.Secret, config *kafka.TopicConfig) (*contract.Resource, error) {
	res := &contract.Resource{
		Uid:    string(broker.UID),
		Topics: []string{topic},
		Ingress: &contract.Ingress{
			IngressType: &contract.Ingress_Path{
				Path: receiver.PathFromObject(broker),
			},
		},
		BootstrapServers: config.GetBootstrapServers(),
	}

	if secret != nil {
		res.Auth = &contract.Resource_AuthSecret{
			AuthSecret: &contract.Reference{
				Uuid:      string(secret.UID),
				Namespace: secret.Namespace,
				Name:      secret.Name,
				Version:   secret.ResourceVersion,
			},
		}
	}

	delivery := broker.Spec.Delivery
	if delivery != nil {

		if delivery.DeadLetterSink != nil {

			deadLetterSinkURL, err := r.Resolver.URIFromDestinationV1(ctx, *delivery.DeadLetterSink, broker)
			if err != nil {
				return nil, fmt.Errorf("failed to resolve broker.Spec.Deliver.DeadLetterSink: %w", err)
			}

			ensureEgressConfig(res)
			res.EgressConfig.DeadLetter = deadLetterSinkURL.String()
		}

		if delivery.Retry != nil {
			ensureEgressConfig(res)
			res.EgressConfig.Retry = uint32(*delivery.Retry)
			var err error
			delay, err := coreconfig.BackoffDelayFromISO8601String(delivery.BackoffDelay, r.Configs.DefaultBackoffDelayMs)
			if err != nil {
				return nil, fmt.Errorf("failed to parse backoff delay: %w", err)
			}
			res.EgressConfig.BackoffDelay = delay
			res.EgressConfig.BackoffPolicy = coreconfig.BackoffPolicyFromString(delivery.BackoffPolicy)
		}
	}

	return res, nil
}

func ensureEgressConfig(res *contract.Resource) {
	if res.EgressConfig == nil {
		res.EgressConfig = &contract.EgressConfig{}
	}
}

func (r *Reconciler) ConfigMapUpdated(ctx context.Context) func(configMap *corev1.ConfigMap) {

	logger := logging.FromContext(ctx).Desugar()

	return func(configMap *corev1.ConfigMap) {

		topicConfig, err := configFromConfigMap(logger, configMap)
		if err != nil {
			return
		}

		logger.Debug("new defaults",
			zap.Any("topicDetail", topicConfig.TopicDetail),
			zap.String("BootstrapServers", topicConfig.GetBootstrapServers()),
		)

		r.SetDefaultTopicDetails(topicConfig.TopicDetail)
		r.SetBootstrapServers(topicConfig.GetBootstrapServers())
	}
}

// SetBootstrapServers change kafka bootstrap brokers addresses.
// servers: a comma separated list of brokers to connect to.
func (r *Reconciler) SetBootstrapServers(servers string) {
	if servers == "" {
		return
	}

	addrs := kafka.BootstrapServersArray(servers)

	r.bootstrapServersLock.Lock()
	r.bootstrapServers = addrs
	r.bootstrapServersLock.Unlock()
}

func (r *Reconciler) SetDefaultTopicDetails(topicDetail sarama.TopicDetail) {
	r.KafkaDefaultTopicDetailsLock.Lock()
	defer r.KafkaDefaultTopicDetailsLock.Unlock()

	r.KafkaDefaultTopicDetails = topicDetail
}

func (r *Reconciler) getDefaultBootstrapServersOrFail() ([]string, error) {
	r.bootstrapServersLock.RLock()
	defer r.bootstrapServersLock.RUnlock()

	if len(r.bootstrapServers) == 0 {
		return nil, fmt.Errorf("no %s provided", BootstrapServersConfigMapKey)
	}

	return r.bootstrapServers, nil
}
