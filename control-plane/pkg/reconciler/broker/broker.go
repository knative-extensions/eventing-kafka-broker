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
	"k8s.io/client-go/tools/record"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1beta1"
	"knative.dev/eventing/pkg/logging"
	"knative.dev/pkg/reconciler"
	"knative.dev/pkg/resolver"

	coreconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/core/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/log"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
)

const (
	// topic prefix - (topic name: knative-<broker-namespace>-<broker-name>)
	TopicPrefix = "knative-"

	// signal that the broker hasn't been added to the config map yet.
	noBroker = -1
)

type Reconciler struct {
	*base.Reconciler

	Resolver *resolver.URIResolver

	KafkaClusterAdmin            sarama.ClusterAdmin
	KafkaClusterAdminLock        sync.RWMutex
	KafkaDefaultTopicDetails     sarama.TopicDetail
	KafkaDefaultTopicDetailsLock sync.RWMutex

	Configs *Configs

	Recorder record.EventRecorder
}

func (r *Reconciler) ReconcileKind(ctx context.Context, broker *eventing.Broker) reconciler.Event {

	logger := logger(ctx, broker)

	logger.Debug("reconciling broker")

	statusConditionManager := statusConditionManager{
		Broker:   broker,
		configs:  r.Configs,
		recorder: r.Recorder,
	}

	topic, err := r.CreateTopic(broker)
	if err != nil {
		return statusConditionManager.failedToCreateTopic(topic, err)
	}
	statusConditionManager.topicCreated(topic)

	logger.Debug("topic created", zap.Any("topic", topic))

	// Get brokers and triggers config map.
	brokersTriggersConfigMap, err := r.GetBrokersTriggersConfigMap()
	if err != nil {
		return statusConditionManager.failedToGetBrokersTriggersConfigMap(err)
	}

	logger.Debug("got brokers and triggers config map")

	// Get brokersTriggers data.
	brokersTriggers, err := r.GetBrokersTriggers(logger, brokersTriggersConfigMap)
	if err != nil && brokersTriggers == nil {
		return statusConditionManager.failedToGetBrokersTriggersDataFromConfigMap(err)
	}

	logger.Debug(
		"got brokers and triggers data from config map",
		zap.Any(base.BrokersTriggersDataLogKey, log.BrokersMarshaller{Brokers: brokersTriggers}),
	)

	brokerIndex := findBroker(brokersTriggers, broker)

	// Get broker configuration.
	brokerConfig, err := r.getBrokerConfig(topic, broker)
	if err != nil {
		return statusConditionManager.failedToGetBrokerConfig(broker, err)
	}
	// Update brokersTriggers data with the new broker configuration
	if brokerIndex != noBroker {
		brokersTriggers.Broker[brokerIndex] = brokerConfig

		logger.Debug("broker exists", zap.Int("index", brokerIndex))

	} else {
		brokersTriggers.Broker = append(brokersTriggers.Broker, brokerConfig)

		logger.Debug("broker doesn't exist")
	}

	// Increment volumeGeneration
	brokersTriggers.VolumeGeneration = incrementVolumeGeneration(brokersTriggers.VolumeGeneration)

	// Update the configuration map with the new brokersTriggers data.
	if err := r.UpdateBrokersTriggersConfigMap(brokersTriggers, brokersTriggersConfigMap); err != nil {
		return statusConditionManager.failedToUpdateBrokersTriggersConfigMap(err)
	}
	statusConditionManager.brokersTriggersConfigMapUpdated()

	logger.Debug("brokers and triggers config map updated")

	// Update volume generation annotation of dispatcher pods
	if err := r.UpdateDispatcherPodsAnnotation(logger, brokersTriggers.VolumeGeneration); err != nil {
		// Failing to update dispatcher pods annotation leads to config map refresh delayed by several seconds.
		// For now, we use the configuration map only on the dispatcher side, which means we don't lose availability,
		// since the receiver will accept events. So, log out the error and move on to the next step.
		logger.Warn(
			"failed to update dispatcher pod annotation to trigger an immediate config map refresh",
			zap.Error(err),
		)

		statusConditionManager.failedToUpdateDispatcherPodsAnnotation(broker, err)
	} else {
		logger.Debug("updated dispatcher pod annotation")
	}

	return statusConditionManager.reconciled()
}

func (r *Reconciler) FinalizeKind(ctx context.Context, broker *eventing.Broker) reconciler.Event {
	logger := logger(ctx, broker)

	logger.Debug("finalizing broker")

	// Get brokers and triggers config map.
	brokersTriggersConfigMap, err := r.GetBrokersTriggersConfigMap()
	if err != nil {
		return fmt.Errorf("failed to get brokers and triggers config map %s: %w", r.Configs.BrokersTriggersConfigMapAsString(), err)
	}

	logger.Debug("got brokers and triggers config map")

	// Get brokersTriggers data.
	brokersTriggers, err := r.GetBrokersTriggers(logger, brokersTriggersConfigMap)
	if err != nil {
		return fmt.Errorf("failed to get brokers and triggers: %w", err)
	}

	logger.Debug(
		"got brokers and triggers data from config map",
		zap.Any(base.BrokersTriggersDataLogKey, log.BrokersMarshaller{Brokers: brokersTriggers}),
	)

	brokerIndex := findBroker(brokersTriggers, broker)
	if brokerIndex == noBroker {
		return fmt.Errorf("broker %s/%s not found in config map", broker.Namespace, broker.Name)
	}
	deleteBroker(brokersTriggers, brokerIndex)

	logger.Debug("broker deleted", zap.Int("index", brokerIndex))

	// Update the configuration map with the new brokersTriggers data.
	if err := r.UpdateBrokersTriggersConfigMap(brokersTriggers, brokersTriggersConfigMap); err != nil {
		return fmt.Errorf("failed to update configuration map: %w", err)
	}

	logger.Debug("brokers and triggers config map updated")

	// There is no need to update volume generation and dispatcher pod annotation, updates to the config map will
	// eventually be seen by the dispatcher pod and resources will be deleted accordingly.

	topic, err := r.deleteTopic(broker)
	if err != nil {
		return fmt.Errorf("failed to delete topic %s: %w", topic, err)
	}

	logger.Debug("topic deleted", zap.String("topic", topic))

	return reconciledNormal(broker.Namespace, broker.Name)
}

func incrementVolumeGeneration(generation uint64) uint64 {
	return (generation + 1) % (math.MaxUint64 - 1)
}

func (r *Reconciler) topicDetailFromBrokerConfig(broker *eventing.Broker) *sarama.TopicDetail {
	// TODO use broker configurations - see https://github.com/knative-sandbox/eventing-kafka-broker/issues/9

	r.KafkaDefaultTopicDetailsLock.RLock()
	defer r.KafkaDefaultTopicDetailsLock.RUnlock()

	// copy the default topic details
	topicDetail := r.KafkaDefaultTopicDetails
	return &topicDetail
}

func (r *Reconciler) getBrokerConfig(topic string, broker *eventing.Broker) (*coreconfig.Broker, error) {
	brokerConfig := &coreconfig.Broker{
		Id:    string(broker.UID),
		Topic: topic,
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

// SetBootstrapServers change kafka bootstrap brokers addresses.
// servers: a comma separated list of brokers to connect to.
func (r *Reconciler) SetBootstrapServers(servers string) error {
	addrs := strings.Split(servers, ",")

	kafkaClusterAdmin, err := NewClusterAdmin(addrs, sarama.NewConfig())
	if err != nil {
		return fmt.Errorf("failed to create kafka cluster admin: %w", err)
	}

	r.KafkaClusterAdminLock.Lock()
	oldKafkaClusterAdmin := r.KafkaClusterAdmin

	// first unlock and then close the old one.
	defer func() {
		if oldKafkaClusterAdmin != nil {
			_ = oldKafkaClusterAdmin.Close()
		}
	}()
	defer r.KafkaClusterAdminLock.Unlock()

	r.KafkaClusterAdmin = kafkaClusterAdmin
	return nil
}

func (r *Reconciler) SetDefaultTopicDetails(topicDetail sarama.TopicDetail) {
	r.KafkaDefaultTopicDetailsLock.Lock()
	defer r.KafkaDefaultTopicDetailsLock.Unlock()

	r.KafkaDefaultTopicDetails = topicDetail
}

func findBroker(brokersTriggers *coreconfig.Brokers, broker *eventing.Broker) int {
	// Find broker in brokersTriggers.
	brokerIndex := noBroker
	for i, b := range brokersTriggers.Broker {
		if b.Id == string(broker.UID) {
			brokerIndex = i
			break
		}
	}
	return brokerIndex
}

func deleteBroker(brokersTriggers *coreconfig.Brokers, index int) {
	if len(brokersTriggers.Broker) == 1 {
		*brokersTriggers = coreconfig.Brokers{
			VolumeGeneration: brokersTriggers.VolumeGeneration,
		}
		return
	}

	// replace the broker to be deleted with the last one.
	brokersTriggers.Broker[index] = brokersTriggers.Broker[len(brokersTriggers.Broker)-1]
	// truncate the array.
	brokersTriggers.Broker = brokersTriggers.Broker[:len(brokersTriggers.Broker)-1]
}

func logger(ctx context.Context, broker *eventing.Broker) *zap.Logger {

	return logging.FromContext(ctx).With(zap.String(
		"broker",
		fmt.Sprintf("%s/%s", broker.Namespace, broker.Name),
	))
}
