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
	"time"

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

	// private annotation for changing the topic
	// NOTE: this may go away in a future release
	externalTopicAnnotation = "x-kafka.eventing.knative.dev/external.topic"
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

	topicConfig, brokerConfig, err := r.topicConfig(logger, broker)
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

	topic, err := r.reconcileBrokerTopic(broker, securityOption, statusConditionManager, topicConfig, logger)
	if err != nil {
		return err
	}

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
		return statusConditionManager.FailedToResolveConfig(err)
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

	if status := r.Prober.Probe(ctx, proberAddressable, prober.StatusReady); status != prober.StatusReady {
		statusConditionManager.ProbesStatusNotReady(status)
		return nil // Object will get re-queued once probe status changes.
	}
	statusConditionManager.Addressable(address)

	// TODO(pierDipi) remove after some releases (released in 1.4)
	if err := r.removeFinalizerCM(ctx, finalizerCM(broker), brokerConfig); err != nil {
		return err
	}

	return nil
}

func (r *Reconciler) reconcileBrokerTopic(broker *eventing.Broker, securityOption kafka.ConfigOption, statusConditionManager base.StatusConditionManager, topicConfig *kafka.TopicConfig, logger *zap.Logger) (string, reconciler.Event) {

	saramaConfig, err := kafka.GetSaramaConfig(securityOption)
	if err != nil {
		return "", statusConditionManager.FailedToResolveConfig(fmt.Errorf("error getting cluster admin config: %w", err))
	}

	kafkaClusterAdminClient, err := r.NewKafkaClusterAdminClient(topicConfig.BootstrapServers, saramaConfig)
	if err != nil {
		return "", statusConditionManager.FailedToResolveConfig(fmt.Errorf("cannot obtain Kafka cluster admin, %w", err))
	}
	defer kafkaClusterAdminClient.Close()

	// if we have a custom topic annotation
	// the topic is externally manged and we do NOT need to create it
	topicName, externalTopic := isExternalTopic(broker)
	if externalTopic {
		isPresentAndValid, err := kafka.AreTopicsPresentAndValid(kafkaClusterAdminClient, topicName)
		if err != nil {
			return "", statusConditionManager.TopicsNotPresentOrInvalidErr([]string{topicName}, err)
		}
		if !isPresentAndValid {
			// The topic might be invalid.
			return "", statusConditionManager.TopicsNotPresentOrInvalid([]string{topicName})
		}
	} else {
		// no external topic, we create it
		topicName = kafka.BrokerTopic(TopicPrefix, broker)

		topic, err := kafka.CreateTopicIfDoesntExist(kafkaClusterAdminClient, logger, topicName, topicConfig)
		if err != nil {
			return "", statusConditionManager.FailedToCreateTopic(topic, err)
		}
	}

	statusConditionManager.TopicReady(topicName)
	logger.Debug("Topic created", zap.Any("topic", topicName))
	return topicName, nil
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

	broker.Status.Address.URL = nil

	//  Rationale: after deleting a topic closing a producer ends up blocking and requesting metadata for max.block.ms
	//  because topic metadata aren't available anymore.
	// 	See (under discussions KIPs, unlikely to be accepted as they are):
	// 	- https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=181306446
	// 	- https://cwiki.apache.org/confluence/display/KAFKA/KIP-286%3A+producer.send%28%29+should+not+block+on+metadata+update
	address := receiver.Address(r.IngressHost, broker)
	proberAddressable := prober.Addressable{
		Address: address,
		ResourceKey: types.NamespacedName{
			Namespace: broker.GetNamespace(),
			Name:      broker.GetName(),
		},
	}
	if status := r.Prober.Probe(ctx, proberAddressable, prober.StatusNotReady); status != prober.StatusNotReady {
		// Return a requeueKeyError that doesn't generate an event and it re-queues the object
		// for a new reconciliation.
		return controller.NewRequeueAfter(5 * time.Second)
	}

	topicConfig, brokerConfig, err := r.topicConfig(logger, broker)
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
	if err := r.finalizeBrokerTopic(broker, securityOption, topicConfig, logger); err != nil {
		return err
	}

	// TODO(pierDipi) remove after some releases (released in 1.4)
	if err := r.removeFinalizerCM(ctx, finalizerCM(broker), brokerConfig); err != nil {
		return err
	}

	return nil
}

func (r *Reconciler) finalizeBrokerTopic(broker *eventing.Broker, securityOption kafka.ConfigOption, topicConfig *kafka.TopicConfig, logger *zap.Logger) reconciler.Event {

	// External topics are not managed by the broker,
	// therefore we do not delete them
	_, externalTopic := isExternalTopic(broker)
	if !externalTopic {
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
		return nil
	}
	return nil
}

func (r *Reconciler) topicConfig(logger *zap.Logger, broker *eventing.Broker) (*kafka.TopicConfig, *corev1.ConfigMap, error) {

	logger.Debug("broker config", zap.Any("broker.spec.config", broker.Spec.Config))

	if strings.ToLower(broker.Spec.Config.Kind) != "configmap" { // TODO: is there any constant?
		return nil, nil, fmt.Errorf("supported config Kind: ConfigMap - got %s", broker.Spec.Config.Kind)
	}

	namespace := broker.Spec.Config.Namespace
	if namespace == "" {
		// Namespace not specified, use broker namespace.
		namespace = broker.Namespace
	}

	// There might be cases where the ConfigMap is deleted before the Broker.
	// In these cases, we rebuild the ConfigMap from broker status annotations.
	//
	// These annotations aren't guaranteed to be there or valid since there might
	// be other actions messing with them, so when we try to rebuild the ConfigMap's
	// data but the guess is wrong or data are invalid we return the
	// `StatusReasonNotFound` error instead of the error generated from the fake
	// "re-built" ConfigMap.

	isRebuilt := false
	cm, getCmError := r.ConfigMapLister.ConfigMaps(namespace).Get(broker.Spec.Config.Name)
	if getCmError != nil && !apierrors.IsNotFound(getCmError) {
		return nil, nil, fmt.Errorf("failed to get configmap %s/%s: %w", namespace, broker.Spec.Config.Name, getCmError)
	}
	if apierrors.IsNotFound(getCmError) {
		cm = rebuildCMFromAnnotations(broker)
		isRebuilt = true
	}

	topicConfig, err := kafka.TopicConfigFromConfigMap(logger, cm)
	if err != nil {
		if isRebuilt {
			return nil, cm, fmt.Errorf("failed to get configmap %s/%s: %w", namespace, broker.Spec.Config.Name, getCmError)
		}
		return nil, cm, fmt.Errorf("unable to build topic config from configmap: %w - ConfigMap data: %v", err, cm.Data)
	}

	if broker.Status.Annotations == nil {
		broker.Status.Annotations = make(map[string]string, len(cm.Data))
	}

	// Save ConfigMap's data into broker annotations
	for k, v := range cm.Data {
		broker.Status.Annotations[k] = v
	}

	return topicConfig, cm, nil
}

func rebuildCMFromAnnotations(br *eventing.Broker) *corev1.ConfigMap {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: br.Spec.Config.Namespace,
			Name:      br.Spec.Config.Name,
		},
	}
	for k, v := range br.Status.Annotations {
		if cm.Data == nil {
			cm.Data = make(map[string]string, len(br.Status.Annotations))
		}
		cm.Data[k] = v
	}
	return cm
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
		Reference: &contract.Reference{
			Uuid:      string(broker.GetUID()),
			Namespace: broker.GetNamespace(),
			Name:      broker.GetName(),
		},
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

func isExternalTopic(broker *eventing.Broker) (string, bool) {
	topicAnnotationValue, ok := broker.Annotations[externalTopicAnnotation]
	return topicAnnotationValue, ok
}
