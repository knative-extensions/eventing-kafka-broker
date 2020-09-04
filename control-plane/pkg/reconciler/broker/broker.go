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
	"math"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/util/retry"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1"
	"knative.dev/eventing/pkg/logging"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/reconciler"
	"knative.dev/pkg/resolver"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	coreconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/core/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/log"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/receiver"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka"
)

const (
	// TopicPrefix is the Kafka Broker topic prefix - (topic name: knative-broker-<broker-namespace>-<broker-name>)
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
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
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

	topicConfig, err := r.topicConfig(logger, broker)
	if err != nil {
		return statusConditionManager.FailedToResolveConfig(err)
	}
	statusConditionManager.ConfigResolved()

	logger.Debug("config resolved", zap.Any("config", topicConfig))

	topic, err := r.ClusterAdmin.CreateTopic(logger, kafka.Topic(TopicPrefix, broker), topicConfig)
	if err != nil {
		return statusConditionManager.FailedToCreateTopic(topic, err)
	}
	statusConditionManager.TopicCreated(topic)

	logger.Debug("Topic created", zap.Any("topic", topic))

	// Get brokers and triggers config map.
	brokersTriggersConfigMap, err := r.GetOrCreateDataPlaneConfigMap()
	if err != nil {
		return statusConditionManager.FailedToGetConfigMap(err)
	}

	logger.Debug("Got brokers and triggers config map")

	// Get brokersTriggers data.
	brokersTriggers, err := r.GetDataPlaneConfigMapData(logger, brokersTriggersConfigMap)
	if err != nil && brokersTriggers == nil {
		return statusConditionManager.FailedToGetDataFromConfigMap(err)
	}

	logger.Debug(
		"Got brokers and triggers data from config map",
		zap.Any(base.BrokersTriggersDataLogKey, log.BrokersMarshaller{Brokers: brokersTriggers}),
	)

	// Get broker configuration.
	brokerConfig, err := r.getBrokerConfig(topic, broker, topicConfig)
	if err != nil {
		return statusConditionManager.FailedToGetConfig(err)
	}

	brokerIndex := coreconfig.FindBroker(brokersTriggers, broker.UID)
	// Update brokersTriggers data with the new broker configuration
	coreconfig.AddOrUpdateBrokersConfig(brokersTriggers, brokerConfig, brokerIndex, logger)

	// Increment volumeGeneration
	brokersTriggers.VolumeGeneration = incrementVolumeGeneration(brokersTriggers.VolumeGeneration)

	// Update the configuration map with the new brokersTriggers data.
	if err := r.UpdateDataPlaneConfigMap(brokersTriggers, brokersTriggersConfigMap); err != nil {
		logger.Error("failed to update data plane config map", zap.Error(
			statusConditionManager.failedToUpdateBrokersTriggersConfigMap(err),
		))
		return err
	}
	statusConditionManager.ConfigMapUpdated()

	logger.Debug("Brokers and triggers config map updated")

	// After #37 we reject events to a non-existing Broker, which means that we cannot consider a Broker Ready if all
	// receivers haven't got the Broker, so update failures to receiver pods is a hard failure.
	// On the other side, dispatcher pods care about Triggers, and the Broker object is used as a configuration
	// prototype for all associated Triggers, so we consider that it's fine on the dispatcher side to receive eventually
	// the update even if here eventually means seconds or minutes after the actual update.

	// Update volume generation annotation of receiver pods
	if err := r.UpdateReceiverPodsAnnotation(logger, brokersTriggers.VolumeGeneration); err != nil {
		logger.Error("Failed to update receiver pod annotation", zap.Error(
			statusConditionManager.failedToUpdateReceiverPodsAnnotation(err),
		))
		return err
	}

	logger.Debug("Updated receiver pod annotation")

	// Update volume generation annotation of dispatcher pods
	if err := r.UpdateDispatcherPodsAnnotation(logger, brokersTriggers.VolumeGeneration); err != nil {
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

func (r *Reconciler) FinalizeKind(ctx context.Context, broker *eventing.Broker) reconciler.Event {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		return r.finalizeKind(ctx, broker)
	})
}

func (r *Reconciler) finalizeKind(ctx context.Context, broker *eventing.Broker) reconciler.Event {

	logger := log.Logger(ctx, "finalize", broker)

	// Get brokers and triggers config map.
	brokersTriggersConfigMap, err := r.GetOrCreateDataPlaneConfigMap()
	if err != nil {
		return fmt.Errorf("failed to get brokers and triggers config map %s: %w", r.Configs.DataPlaneConfigMapAsString(), err)
	}

	logger.Debug("Got brokers and triggers config map")

	// Get brokersTriggers data.
	brokersTriggers, err := r.GetDataPlaneConfigMapData(logger, brokersTriggersConfigMap)
	if err != nil {
		return fmt.Errorf("failed to get brokers and triggers: %w", err)
	}

	logger.Debug(
		"Got brokers and triggers data from config map",
		zap.Any(base.BrokersTriggersDataLogKey, log.BrokersMarshaller{Brokers: brokersTriggers}),
	)

	brokerIndex := coreconfig.FindBroker(brokersTriggers, broker.UID)
	if brokerIndex != coreconfig.NoBroker {
		deleteBroker(brokersTriggers, brokerIndex)

		logger.Debug("Broker deleted", zap.Int("index", brokerIndex))

		// Update the configuration map with the new brokersTriggers data.
		if err := r.UpdateDataPlaneConfigMap(brokersTriggers, brokersTriggersConfigMap); err != nil {
			return err
		}

		logger.Debug("Brokers and triggers config map updated")

		// There is no need to update volume generation and dispatcher pod annotation, updates to the config map will
		// eventually be seen by the dispatcher pod and resources will be deleted accordingly.
	}

	topicConfig, err := r.topicConfig(logger, broker)
	if err != nil {
		return fmt.Errorf("failed to resolve broker config: %w", err)
	}

	bootstrapServers := topicConfig.BootstrapServers
	topic, err := r.ClusterAdmin.DeleteTopic(kafka.Topic(TopicPrefix, broker), bootstrapServers)
	if err != nil {
		return err
	}

	logger.Debug("Topic deleted", zap.String("topic", topic))

	return nil
}

func incrementVolumeGeneration(generation uint64) uint64 {
	return (generation + 1) % (math.MaxUint64 - 1)
}

func (r *Reconciler) topicConfig(logger *zap.Logger, broker *eventing.Broker) (*kafka.TopicConfig, error) {

	logger.Debug("broker config", zap.Any("broker.spec.config", broker.Spec.Config))

	if broker.Spec.Config == nil {
		return r.defaultConfig()
	}

	if strings.ToLower(broker.Spec.Config.Kind) != "configmap" { // TODO: is there any constant?
		return nil, fmt.Errorf("supported config Kind: ConfigMap - got %s", broker.Spec.Config.Kind)
	}

	namespace := broker.Spec.Config.Namespace
	if namespace == "" {
		// Namespace not specified, use broker namespace.
		namespace = broker.Namespace
	}
	cm, err := r.ConfigMapLister.ConfigMaps(namespace).Get(broker.Spec.Config.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to get configmap %s/%s: %w", namespace, broker.Spec.Config.Name, err)
	}

	brokerConfig, err := configFromConfigMap(logger, cm)
	if err != nil {
		return nil, err
	}

	return brokerConfig, nil
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

func (r *Reconciler) getBrokerConfig(topic string, broker *eventing.Broker, config *kafka.TopicConfig) (*coreconfig.Broker, error) {

	brokerConfig := &coreconfig.Broker{
		Id:               string(broker.UID),
		Topic:            topic,
		Path:             receiver.PathFromObject(broker),
		BootstrapServers: config.GetBootstrapServers(),
	}

	if broker.Spec.Delivery == nil || broker.Spec.Delivery.DeadLetterSink == nil {
		return brokerConfig, nil
	}

	deadLetterSinkURL, err := r.Resolver.URIFromDestinationV1(*broker.Spec.Delivery.DeadLetterSink, broker)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve broker.Spec.Deliver.DeadLetterSink: %w", err)
	}

	brokerConfig.DeadLetterSink = deadLetterSinkURL.String()

	return brokerConfig, nil
}

func (r *Reconciler) ConfigMapUpdated(ctx context.Context) func(configMap *corev1.ConfigMap) {

	return func(configMap *corev1.ConfigMap) {

		logger := logging.FromContext(ctx)

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

func deleteBroker(brokersTriggers *coreconfig.Brokers, index int) {
	if len(brokersTriggers.Brokers) == 1 {
		*brokersTriggers = coreconfig.Brokers{
			VolumeGeneration: brokersTriggers.VolumeGeneration,
		}
		return
	}

	// replace the broker to be deleted with the last one.
	brokersTriggers.Brokers[index] = brokersTriggers.Brokers[len(brokersTriggers.Brokers)-1]
	// truncate the array.
	brokersTriggers.Brokers = brokersTriggers.Brokers[:len(brokersTriggers.Brokers)-1]
}

func (r *Reconciler) getDefaultBootstrapServersOrFail() ([]string, error) {
	r.bootstrapServersLock.RLock()
	defer r.bootstrapServersLock.RUnlock()

	if len(r.bootstrapServers) == 0 {
		return nil, fmt.Errorf("no %s provided", BootstrapServersConfigMapKey)
	}

	return r.bootstrapServers, nil
}
