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

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/util/retry"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/reconciler"
	"knative.dev/pkg/resolver"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
	coreconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/core/config"
	kafkalogging "knative.dev/eventing-kafka-broker/control-plane/pkg/logging"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/prober"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/receiver"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/security"
)

const (
	// TopicPrefix is the Kafka Broker topic prefix - (topic name: knative-broker-<broker-namespace>-<broker-name>).
	TopicPrefix = "knative-broker-"
)

type Reconciler struct {
	*base.Reconciler
	*config.Env

	Resolver *resolver.URIResolver

	ConfigMapLister corelisters.ConfigMapLister

	// NewKafkaClusterAdminClient creates new sarama ClusterAdmin. It's convenient to add this as Reconciler field so that we can
	// mock the function used during the reconciliation loop.
	NewKafkaClusterAdminClient kafka.NewClusterAdminClientFunc

	BootstrapServers string

	Prober prober.Prober

	IngressHost string
}

func (r *Reconciler) ReconcileKind(ctx context.Context, broker *eventing.Broker) reconciler.Event {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		return r.reconcileKind(ctx, broker)
	})
}

func (r *Reconciler) reconcileKind(ctx context.Context, broker *eventing.Broker) reconciler.Event {
	logger := kafkalogging.CreateReconcileMethodLogger(ctx, broker)

	statusConditionManager := base.StatusConditionManager{
		Object:     broker,
		SetAddress: broker.Status.SetAddress,
		Env:        r.Env,
		Recorder:   controller.GetEventRecorder(ctx),
	}

	if !r.IsReceiverRunning() || !r.IsDispatcherRunning() {
		return statusConditionManager.DataPlaneNotAvailable()
	}
	statusConditionManager.DataPlaneAvailable()

	topicConfig, brokerConfig, err := r.topicConfig(ctx, logger, broker)
	if err != nil {
		return statusConditionManager.FailedToResolveConfig(err)
	}
	statusConditionManager.ConfigResolved()

	if err := r.TrackConfigMap(brokerConfig, broker); err != nil {
		return fmt.Errorf("failed to track broker config: %w", err)
	}

	logger.Debug("config resolved", zap.Any("config", topicConfig))

	secret, err := security.Secret(ctx, &security.MTConfigMapSecretLocator{ConfigMap: brokerConfig}, r.SecretProviderFunc())
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

	if err := r.TrackSecret(secret, broker); err != nil {
		return fmt.Errorf("failed to track secret: %w", err)
	}

	topicName := kafka.BrokerTopic(TopicPrefix, broker)

	saramaConfig, err := kafka.GetSaramaConfig(securityOption)
	if err != nil {
		return statusConditionManager.FailedToCreateTopic(topicName, fmt.Errorf("error getting cluster admin config: %w", err))
	}

	kafkaClusterAdminClient, err := r.NewKafkaClusterAdminClient(topicConfig.BootstrapServers, saramaConfig)
	if err != nil {
		return statusConditionManager.FailedToCreateTopic(topicName, fmt.Errorf("cannot obtain Kafka cluster admin, %w", err))
	}
	defer kafkaClusterAdminClient.Close()

	topic, err := kafka.CreateTopicIfDoesntExist(kafkaClusterAdminClient, logger, topicName, topicConfig)
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

	logger.Debug("Got contract data from config map", zap.Any(base.ContractLogKey, ct))

	// Get resource configuration.
	brokerResource, err := r.reconcilerBrokerResource(ctx, topic, broker, secret, topicConfig)
	if err != nil {
		return statusConditionManager.FailedToGetConfig(err)
	}
	coreconfig.SetDeadLetterSinkURIFromEgressConfig(&broker.Status.DeliveryStatus, brokerResource.EgressConfig)

	brokerIndex := coreconfig.FindResource(ct, broker.UID)
	// Update contract data with the new contract configuration
	coreconfig.SetResourceEgressesFromContract(ct, brokerResource, brokerIndex)
	changed := coreconfig.AddOrUpdateResourceConfig(ct, brokerResource, brokerIndex, logger)

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

	// We reject events to a non-existing Broker, which means that we cannot consider a Broker Ready if all
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

	address := receiver.Address(r.IngressHost, broker)
	proberAddressable := prober.Addressable{
		Address: address,
		ResourceKey: types.NamespacedName{
			Namespace: broker.GetNamespace(),
			Name:      broker.GetName(),
		},
	}

	if status := r.Prober.Probe(ctx, proberAddressable); status != prober.StatusReady {
		statusConditionManager.ProbesStatusNotReady(status)
		return nil // Object will get re-queued once probe status changes.
	}
	statusConditionManager.Addressable(address)

	return nil
}

func (r *Reconciler) FinalizeKind(ctx context.Context, broker *eventing.Broker) reconciler.Event {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		return r.finalizeKind(ctx, broker)
	})
}

func (r *Reconciler) finalizeKind(ctx context.Context, broker *eventing.Broker) reconciler.Event {
	logger := kafkalogging.CreateFinalizeMethodLogger(ctx, broker)

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

	logger.Debug("Got contract data from config map", zap.Any(base.ContractLogKey, ct))

	if err := r.DeleteResource(ctx, logger, broker.GetUID(), ct, contractConfigMap); err != nil {
		return err
	}

	// We update receiver and dispatcher pods annotation regardless of our contract changed or not due to the fact
	// that in a previous reconciliation we might have failed to update one of our data plane pod annotation, so we want
	// to update anyway remaining annotations with the contract generation that was saved in the CM.
	// Note: if there aren't changes to be done at the pod annotation level, we just skip the update.

	// Update volume generation annotation of receiver pods
	if err := r.UpdateReceiverPodsAnnotation(ctx, logger, ct.Generation); err != nil {
		return err
	}
	// Update volume generation annotation of dispatcher pods
	if err := r.UpdateDispatcherPodsAnnotation(ctx, logger, ct.Generation); err != nil {
		return err
	}

	// TODO probe (as in #974) and check if status code is 404 otherwise requeue and return.
	//  Rationale: after deleting a topic closing a producer ends up blocking and requesting metadata for max.block.ms
	//  because topic metadata aren't available anymore.
	// 	See (under discussions KIPs, unlikely to be accepted as they are):
	// 	- https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=181306446
	// 	- https://cwiki.apache.org/confluence/display/KAFKA/KIP-286%3A+producer.send%28%29+should+not+block+on+metadata+update

	topicConfig, brokerConfig, err := r.topicConfig(ctx, logger, broker)
	if err != nil {
		return fmt.Errorf("failed to resolve broker config: %w", err)
	}

	secret, err := security.Secret(ctx, &security.MTConfigMapSecretLocator{ConfigMap: brokerConfig}, r.SecretProviderFunc())
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

	kafkaClusterAdminClient, err := r.NewKafkaClusterAdminClient(topicConfig.BootstrapServers, saramaConfig)
	if err != nil {
		// even in error case, we return `normal`, since we are fine with leaving the
		// topic undeleted e.g. when we lose connection
		return fmt.Errorf("cannot obtain Kafka cluster admin, %w", err)
	}
	defer kafkaClusterAdminClient.Close()

	topic, err := kafka.DeleteTopic(kafkaClusterAdminClient, kafka.BrokerTopic(TopicPrefix, broker))
	if err != nil {
		return err
	}

	logger.Debug("Topic deleted", zap.String("topic", topic))

	if err := r.removeFinalizerCM(ctx, finalizerCM(broker), brokerConfig); err != nil {
		return err
	}

	return nil
}

func (r *Reconciler) topicConfig(ctx context.Context, logger *zap.Logger, broker *eventing.Broker) (*kafka.TopicConfig, *corev1.ConfigMap, error) {

	logger.Debug("broker config", zap.Any("broker.spec.config", broker.Spec.Config))

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

	if err := r.addFinalizerCM(ctx, finalizerCM(broker), cm); err != nil {
		return nil, nil, err
	}

	topicConfig, err := kafka.TopicConfigFromConfigMap(logger, cm)
	if err != nil {
		return nil, cm, fmt.Errorf("unable to build topic config from configmap: %w - ConfigMap data: %v", err, cm.Data)
	}

	return topicConfig, cm, nil
}

func (r *Reconciler) reconcilerBrokerResource(ctx context.Context, topic string, broker *eventing.Broker, secret *corev1.Secret, config *kafka.TopicConfig) (*contract.Resource, error) {
	resource := &contract.Resource{
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
		resource.Auth = &contract.Resource_AuthSecret{
			AuthSecret: &contract.Reference{
				Uuid:      string(secret.UID),
				Namespace: secret.Namespace,
				Name:      secret.Name,
				Version:   secret.ResourceVersion,
			},
		}
	}

	egressConfig, err := coreconfig.EgressConfigFromDelivery(ctx, r.Resolver, broker, broker.Spec.Delivery, r.DefaultBackoffDelayMs)
	if err != nil {
		return nil, err
	}
	resource.EgressConfig = egressConfig

	return resource, nil
}

func (r *Reconciler) addFinalizerCM(ctx context.Context, finalizer string, cm *corev1.ConfigMap) error {
	if !containsFinalizerCM(cm, finalizer) {
		cm := cm.DeepCopy() // Do not modify informer copy.
		cm.Finalizers = append(cm.Finalizers, finalizer)
		_, err := r.KubeClient.CoreV1().ConfigMaps(cm.GetNamespace()).Update(ctx, cm, metav1.UpdateOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to add finalizer to ConfigMap %s/%s: %w", cm.GetNamespace(), cm.GetName(), err)
		}
	}
	return nil
}

func containsFinalizerCM(cm *corev1.ConfigMap, finalizer string) bool {
	if cm == nil {
		return false
	}
	for _, f := range cm.Finalizers {
		if f == finalizer {
			return true
		}
	}
	return false
}

func (r *Reconciler) removeFinalizerCM(ctx context.Context, finalizer string, cm *corev1.ConfigMap) error {
	newFinalizers := make([]string, 0, len(cm.Finalizers))
	for _, f := range cm.Finalizers {
		if f != finalizer {
			newFinalizers = append(newFinalizers, f)
		}
	}
	if len(newFinalizers) != len(cm.Finalizers) {
		cm := cm.DeepCopy() // Do not modify informer copy.
		cm.Finalizers = newFinalizers
		_, err := r.KubeClient.CoreV1().ConfigMaps(cm.GetNamespace()).Update(ctx, cm, metav1.UpdateOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to remove finalizer %s to ConfigMap %s/%s: %w", finalizer, cm.GetNamespace(), cm.GetName(), err)
		}
	}
	return nil
}

func finalizerCM(object metav1.Object) string {
	return fmt.Sprintf("%s/%s-%s", "kafka.brokers.eventing.knative.dev", object.GetNamespace(), object.GetName())
}
