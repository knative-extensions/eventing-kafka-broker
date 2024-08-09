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

	"knative.dev/eventing/pkg/auth"
	"knative.dev/pkg/logging"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/pointer"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1"
	"knative.dev/eventing/pkg/apis/feature"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/network"
	"knative.dev/pkg/reconciler"
	"knative.dev/pkg/resolver"

	apisconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
	coreconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/core/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/counter"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka/clientpool"
	kafkalogging "knative.dev/eventing-kafka-broker/control-plane/pkg/logging"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/prober"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/receiver"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/security"
)

const (
	// ExternalTopicAnnotation for using external kafka topic for the broker
	ExternalTopicAnnotation = "kafka.eventing.knative.dev/external.topic"

	// ConsumerConfigKey is the key for Kafka Broker consumer configurations
	ConsumerConfigKey = "config-kafka-broker-consumer.properties"

	// brokerIngressTLSSecretName is the TLS Secret Name for the Cert-Manager resource
	brokerIngressTLSSecretName = "kafka-broker-ingress-server-tls"

	// caCertsSecretKey is the name of the CA Cert in the secret
	caCertsSecretKey = "ca.crt"
)

type Reconciler struct {
	*base.Reconciler
	*config.Env

	Resolver *resolver.URIResolver

	ConfigMapLister corelisters.ConfigMapLister

	// GetKafkaClusterAdmin creates new sarama ClusterAdmin. It's convenient to add this as Reconciler field so that we can
	// mock the function used during the reconciliation loop.
	GetKafkaClusterAdmin clientpool.GetKafkaClusterAdminFunc

	BootstrapServers string

	Prober            prober.NewProber
	Counter           *counter.Counter
	KafkaFeatureFlags *apisconfig.KafkaFeatureFlags
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

	// Get contract config map. Do this in advance, otherwise
	// the dataplane pods that need volume mounts to the contract configmap
	// will get stuck and will never be ready.
	contractConfigMap, err := r.GetOrCreateDataPlaneConfigMap(ctx)
	if err != nil {
		return statusConditionManager.FailedToGetConfigMap(err)
	}

	logger.Debug("Got contract config map")

	if !r.IsReceiverRunning() {
		return statusConditionManager.DataPlaneNotAvailable()
	}
	statusConditionManager.DataPlaneAvailable()

	brokerConfig, err := r.brokerConfigMap(logger, broker)
	if err != nil && !apierrors.IsNotFound(err) {
		return statusConditionManager.FailedToResolveConfig(err)
	}

	topicConfig, err := r.topicConfig(logger, broker, brokerConfig)
	if err != nil {
		return statusConditionManager.FailedToResolveConfig(err)
	}
	statusConditionManager.ConfigResolved()

	if err := r.TrackConfigMap(brokerConfig, broker); err != nil {
		return fmt.Errorf("failed to track broker config: %w", err)
	}

	logger.Debug("config resolved", zap.Any("config", topicConfig))

	secret, err := security.Secret(ctx, &security.MTConfigMapSecretLocator{ConfigMap: brokerConfig, UseNamespaceInConfigmap: false}, r.SecretProviderFunc())
	if err != nil {
		return statusConditionManager.FailedToGetBrokerAuthSecret(err)
	}
	if secret != nil {
		logger.Debug("Secret reference",
			zap.String("apiVersion", secret.APIVersion),
			zap.String("name", secret.Name),
			zap.String("namespace", secret.Namespace),
			zap.String("kind", secret.Kind),
		)

		if err := r.addFinalizerSecret(ctx, finalizerSecret(broker), secret); err != nil {
			return err
		}
	}

	if err := r.TrackSecret(secret, broker); err != nil {
		return fmt.Errorf("failed to track secret: %w", err)
	}

	topic, err := r.reconcileBrokerTopic(ctx, broker, secret, statusConditionManager, topicConfig, logger)
	if err != nil {
		return err
	}

	// Get contract data.
	ct, err := r.GetDataPlaneConfigMapData(logger, contractConfigMap)
	if err != nil && ct == nil {
		return statusConditionManager.FailedToGetDataFromConfigMap(err)
	}

	logger.Debug("Got contract data from config map", zap.Any(base.ContractLogKey, ct))

	if err := r.setTrustBundles(ct); err != nil {
		return statusConditionManager.FailedToResolveConfig(err)
	}

	// Get resource configuration.
	brokerResource, err := r.reconcilerBrokerResource(ctx, topic, broker, secret, topicConfig)
	if err != nil {
		return statusConditionManager.FailedToResolveConfig(err)
	}
	coreconfig.SetDeadLetterSinkURIFromEgressConfig(&broker.Status.DeliveryStatus, brokerResource.EgressConfig)

	brokerIndex := coreconfig.FindResource(ct, broker.UID)
	// Update contract data with the new contract configuration
	coreconfig.SetResourceEgressesFromContract(ct, brokerResource, brokerIndex)
	coreconfig.AddOrUpdateResourceConfig(ct, brokerResource, brokerIndex, logger)

	// Update the configuration map with the new contract data.
	if err := r.UpdateDataPlaneConfigMap(ctx, ct, contractConfigMap); err != nil {
		logger.Error("failed to update data plane config map", zap.Error(
			statusConditionManager.FailedToUpdateConfigMap(err),
		))
		return err
	}
	logger.Debug("Contract config map updated")
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
	if err := r.UpdateReceiverPodsContractGenerationAnnotation(ctx, logger, ct.Generation); err != nil {
		logger.Error("Failed to update receiver pod annotation", zap.Error(
			statusConditionManager.FailedToUpdateReceiverPodsAnnotation(err),
		))
		return err
	}

	logger.Debug("Updated receiver pod annotation")

	// Update volume generation annotation of dispatcher pods
	if err := r.UpdateDispatcherPodsContractGenerationAnnotation(ctx, logger, ct.Generation); err != nil {
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

	ingressHost := network.GetServiceHostname(r.Env.IngressName, r.DataPlaneNamespace)

	transportEncryptionFlags := feature.FromContext(ctx)
	var addressableStatus duckv1.AddressStatus
	if transportEncryptionFlags.IsPermissiveTransportEncryption() {
		caCerts, err := r.getCaCerts()
		if err != nil {
			return err
		}

		httpAddress := receiver.HTTPAddress(ingressHost, nil, broker)
		httpsAddress := receiver.HTTPSAddress(ingressHost, nil, broker, caCerts)
		addressableStatus.Address = &httpAddress
		addressableStatus.Addresses = []duckv1.Addressable{httpAddress, httpsAddress}
	} else if transportEncryptionFlags.IsStrictTransportEncryption() {
		caCerts, err := r.getCaCerts()
		if err != nil {
			return err
		}

		httpsAddress := receiver.HTTPSAddress(ingressHost, nil, broker, caCerts)
		addressableStatus.Address = &httpsAddress
		addressableStatus.Addresses = []duckv1.Addressable{httpsAddress}
	} else {
		httpAddress := receiver.HTTPAddress(ingressHost, nil, broker)
		addressableStatus.Address = &httpAddress
		addressableStatus.Addresses = []duckv1.Addressable{httpAddress}
	}
	proberAddressable := prober.ProberAddressable{
		AddressStatus: &addressableStatus,
		ResourceKey: types.NamespacedName{
			Namespace: broker.GetNamespace(),
			Name:      broker.GetName(),
		},
	}

	if status := r.Prober.Probe(ctx, proberAddressable, prober.StatusReady); status != prober.StatusReady {
		statusConditionManager.ProbesStatusNotReady(status)
		return nil // Object will get re-queued once probe status changes.
	}
	statusConditionManager.ProbesStatusReady()

	broker.Status.Address = addressableStatus.Address
	broker.Status.Addresses = addressableStatus.Addresses

	if feature.FromContext(ctx).IsOIDCAuthentication() {
		audience := auth.GetAudience(eventing.SchemeGroupVersion.WithKind("Broker"), broker.ObjectMeta)
		logging.FromContext(ctx).Debugw("Setting the brokers audience", zap.String("audience", audience))
		broker.Status.Address.Audience = &audience

		for i := range broker.Status.Addresses {
			broker.Status.Addresses[i].Audience = &audience
		}
	} else {
		logging.FromContext(ctx).Debug("Clearing the brokers audience as OIDC is not enabled")
		if broker.Status.Address != nil {
			broker.Status.Address.Audience = nil
		}

		for i := range broker.Status.Addresses {
			broker.Status.Addresses[i].Audience = nil
		}
	}

	broker.GetConditionSet().Manage(broker.GetStatus()).MarkTrue(base.ConditionAddressable)

	return nil
}

func (r *Reconciler) reconcileBrokerTopic(ctx context.Context, broker *eventing.Broker, secret *corev1.Secret, statusConditionManager base.StatusConditionManager, topicConfig *kafka.TopicConfig, logger *zap.Logger) (string, reconciler.Event) {

	kafkaClusterAdminClient, err := r.GetKafkaClusterAdmin(ctx, topicConfig.BootstrapServers, secret)
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
		var existingTopic bool
		// check if the broker already has reconciled with a topic name and use that if it exists
		// otherwise, we create a new topic name from the broker topic template
		if topicName, existingTopic = broker.Status.Annotations[kafka.TopicAnnotation]; !existingTopic {
			topicName, err = r.KafkaFeatureFlags.ExecuteBrokersTopicTemplate(broker.ObjectMeta)
			if err != nil {
				return "", statusConditionManager.TopicsNotPresentOrInvalidErr([]string{topicName}, err)
			}
		}

		topic, err := kafka.CreateTopicIfDoesntExist(kafkaClusterAdminClient, logger, topicName, topicConfig)
		if err != nil {
			return "", statusConditionManager.FailedToCreateTopic(topic, err)
		}
	}

	statusConditionManager.TopicReady(topicName)
	logger.Debug("Topic created", zap.Any("topic", topicName))

	broker.Status.Annotations[kafka.TopicAnnotation] = topicName

	return topicName, nil
}

func (r *Reconciler) FinalizeKind(ctx context.Context, broker *eventing.Broker) reconciler.Event {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		return r.finalizeKind(ctx, broker)
	})
}

func (r *Reconciler) finalizeKind(ctx context.Context, broker *eventing.Broker) reconciler.Event {
	logger := kafkalogging.CreateFinalizeMethodLogger(ctx, broker)

	if err := r.deleteResourceFromContractConfigMap(ctx, logger, broker); err != nil {
		return err
	}

	broker.Status.Address = nil

	ingressHost := network.GetServiceHostname(r.Env.IngressName, r.Reconciler.DataPlaneNamespace)

	//  Rationale: after deleting a topic closing a producer ends up blocking and requesting metadata for max.block.ms
	//  because topic metadata aren't available anymore.
	// 	See (under discussions KIPs, unlikely to be accepted as they are):
	// 	- https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=181306446
	// 	- https://cwiki.apache.org/confluence/display/KAFKA/KIP-286%3A+producer.send%28%29+should+not+block+on+metadata+update
	address := receiver.HTTPAddress(ingressHost, nil, broker)
	proberAddressable := prober.ProberAddressable{
		AddressStatus: &duckv1.AddressStatus{
			Address:   &address,
			Addresses: []duckv1.Addressable{address},
		},
		ResourceKey: types.NamespacedName{
			Namespace: broker.GetNamespace(),
			Name:      broker.GetName(),
		},
	}
	status := r.Prober.Probe(ctx, proberAddressable, prober.StatusNotReady)
	if status != prober.StatusNotReady && status != prober.StatusUnknownErr {
		// Return a requeueKeyError that doesn't generate an event and it re-queues the object
		// for a new reconciliation.
		return controller.NewRequeueAfter(5 * time.Second)
	}

	brokerConfig, err := r.brokerConfigMap(logger, broker)
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	// If the broker config data is empty we simply return,
	// as the configuration may already be gone
	if len(brokerConfig.Data) == 0 {
		return nil
	}

	secret, err := security.Secret(ctx, &security.MTConfigMapSecretLocator{ConfigMap: brokerConfig, UseNamespaceInConfigmap: false}, r.SecretProviderFunc())
	if err != nil {
		// If we can not get the referenced secret,
		// let us try for a bit before we give up.
		brokerUUID := string(broker.GetUID())
		if r.Counter.Inc(brokerUUID) <= 5 {
			return controller.NewRequeueAfter(5 * time.Second)
		}
	}

	if secret != nil {
		logger.Debug("Secret reference",
			zap.String("apiVersion", secret.APIVersion),
			zap.String("name", secret.Name),
			zap.String("namespace", secret.Namespace),
			zap.String("kind", secret.Kind),
		)
	}

	// External topics are not managed by the broker,
	// therefore we do not delete them
	_, externalTopic := isExternalTopic(broker)
	if !externalTopic {
		topicConfig, err := r.topicConfig(logger, broker, brokerConfig)
		if err != nil {

			// On finalize, we fail to get a valid config, we can safely ignore and return nil
			// no further actions are needed since we are also not putting the finalizer on given secret
			// if we are not having a valid topic config
			if strings.Contains(err.Error(), "validating topic config") {
				return nil
			} else {
				return fmt.Errorf("failed to resolve broker config: %w", err)
			}
		}

		err = r.finalizeNonExternalBrokerTopic(ctx, broker, secret, topicConfig, logger)

		// if finalizeNonExternalBrokerTopic returns error that kafka is not reachable!
		if err != nil {
			if strings.Contains(err.Error(), "cannot obtain Kafka") {

				// If the kafka cluster is not reachable we give it a few more retries, to see if there was
				// some temporary network issue and requeue the finalization.
				// If we tried often enough and the kafka cluster is still not reachable,
				// we return nil and delete the broker
				brokerUUID := string(broker.GetUID())
				if r.Counter.Inc(brokerUUID) <= 5 {
					return controller.NewRequeueAfter(5 * time.Second)
				} else {
					r.Counter.Del(brokerUUID) // clean up the reference from the counter
					logger.Error("Error reaching Kafka cluster", zap.Error(err))
					return nil
				}
			}
			return err
		}
	}

	if err := r.removeFinalizerSecret(ctx, finalizerSecret(broker), secret); err != nil {
		return err
	}

	return nil
}

func (r *Reconciler) deleteResourceFromContractConfigMap(ctx context.Context, logger *zap.Logger, broker *eventing.Broker) error {
	// Get contract config map.
	contractConfigMap, err := r.GetOrCreateDataPlaneConfigMap(ctx)
	// Handles https://github.com/knative-extensions/eventing-kafka-broker/issues/2893
	// When the system namespace is deleted while we're running there is no point in
	// trying to delete the resource from the ConfigMap since the entire ConfigMap
	// is gone.
	if apierrors.IsForbidden(err) {
		return nil
	}
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
	if err := r.UpdateReceiverPodsContractGenerationAnnotation(ctx, logger, ct.Generation); err != nil {
		return err
	}
	// Update volume generation annotation of dispatcher pods
	if err := r.UpdateDispatcherPodsContractGenerationAnnotation(ctx, logger, ct.Generation); err != nil {
		return err
	}

	return nil
}

func (r *Reconciler) finalizeNonExternalBrokerTopic(ctx context.Context, broker *eventing.Broker, secret *corev1.Secret, topicConfig *kafka.TopicConfig, logger *zap.Logger) reconciler.Event {

	kafkaClusterAdminClient, err := r.GetKafkaClusterAdmin(ctx, topicConfig.BootstrapServers, secret)
	if err != nil {
		// even in error case, we return `normal`, since we are fine with leaving the
		// topic undeleted e.g. when we lose connection
		return fmt.Errorf("cannot obtain Kafka cluster admin, %w", err)
	}
	defer kafkaClusterAdminClient.Close()

	topicName, ok := broker.Status.Annotations[kafka.TopicAnnotation]
	if !ok {
		return fmt.Errorf("no topic annotated on broker")
	}
	topic, err := kafka.DeleteTopic(kafkaClusterAdminClient, topicName)
	if err != nil {
		return err
	}

	logger.Debug("Topic deleted", zap.String("topic", topic))
	return nil
}

func (r *Reconciler) brokerNamespace(broker *eventing.Broker) string {
	namespace := broker.Spec.Config.Namespace
	if namespace == "" {
		// Namespace not specified, use broker namespace.
		namespace = broker.Namespace
	}
	return namespace
}

func (r *Reconciler) brokerConfigMap(logger *zap.Logger, broker *eventing.Broker) (*corev1.ConfigMap, error) {
	logger.Debug("broker config", zap.Any("broker.spec.config", broker.Spec.Config))

	if strings.ToLower(broker.Spec.Config.Kind) != "configmap" {
		return nil, fmt.Errorf("supported config Kind: ConfigMap - got %s", broker.Spec.Config.Kind)
	}

	namespace := r.brokerNamespace(broker)

	// There might be cases where the ConfigMap is deleted before the Broker.
	// In these cases, we rebuild the ConfigMap from broker status annotations.
	//
	// These annotations aren't guaranteed to be there or valid since there might
	// be other actions messing with them, so when we try to rebuild the ConfigMap's
	// data but the guess is wrong or data are invalid we return the
	// `StatusReasonNotFound` error instead of the error generated from the fake
	// "re-built" ConfigMap.

	cm, getCmError := r.ConfigMapLister.ConfigMaps(namespace).Get(broker.Spec.Config.Name)
	if getCmError != nil && !apierrors.IsNotFound(getCmError) {
		return cm, fmt.Errorf("failed to get configmap %s/%s: %w", namespace, broker.Spec.Config.Name, getCmError)
	}
	if apierrors.IsNotFound(getCmError) {
		// will at least return an empty CM
		cm = rebuildCMFromStatusAnnotations(broker)
	}

	return cm, getCmError
}

func (r *Reconciler) topicConfig(logger *zap.Logger, broker *eventing.Broker, brokerConfig *corev1.ConfigMap) (*kafka.TopicConfig, error) {
	topicConfig, err := kafka.TopicConfigFromConfigMap(logger, brokerConfig)
	if err != nil {
		// Check if the rebuilt CM is empty
		if brokerConfig != nil && len(brokerConfig.Data) == 0 {
			return nil, fmt.Errorf("unable to rebuild topic config, failed to get configmap %s/%s", r.brokerNamespace(broker), broker.Spec.Config.Name)
		}
		return nil, fmt.Errorf("unable to build topic config from configmap: %w - ConfigMap data: %v", err, brokerConfig.Data)
	}

	storeConfigMapAsStatusAnnotation(broker, brokerConfig)

	return topicConfig, nil
}

// Save ConfigMap's data into broker annotations, to prevent issue when the ConfigMap itself is being deleted
func storeConfigMapAsStatusAnnotation(broker *eventing.Broker, cm *corev1.ConfigMap) {
	if broker.Status.Annotations == nil {
		broker.Status.Annotations = make(map[string]string, len(cm.Data))
	}

	keysToStore := map[string]bool{
		kafka.DefaultTopicNumPartitionConfigMapKey:      true,
		kafka.DefaultTopicReplicationFactorConfigMapKey: true,
		kafka.BootstrapServersConfigMapKey:              true,
		security.AuthSecretNameKey:                      true,
	}

	for k, v := range cm.Data {
		if keysToStore[k] {
			broker.Status.Annotations[k] = v
		}
	}
}

// Creates the Broker ConfigMap from the status annotation, if any present
func rebuildCMFromStatusAnnotations(br *eventing.Broker) *corev1.ConfigMap {
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
			Path:                       receiver.PathFromObject(broker),
			EnableAutoCreateEventTypes: feature.FromContext(ctx).IsEnabled(feature.EvenTypeAutoCreate),
		},
		BootstrapServers: config.GetBootstrapServers(),
		Reference: &contract.Reference{
			Uuid:         string(broker.GetUID()),
			Namespace:    broker.GetNamespace(),
			Name:         broker.GetName(),
			Kind:         "Broker",
			GroupVersion: eventing.SchemeGroupVersion.String(),
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

	if broker.Status.Address != nil && broker.Status.Address.Audience != nil {
		resource.Ingress.Audience = *broker.Status.Address.Audience
	}

	egressConfig, err := coreconfig.EgressConfigFromDelivery(ctx, r.Resolver, broker, broker.Spec.Delivery, r.DefaultBackoffDelayMs)
	if err != nil {
		return nil, err
	}
	resource.EgressConfig = egressConfig

	return resource, nil
}

func isExternalTopic(broker *eventing.Broker) (string, bool) {
	topicAnnotationValue, ok := broker.Annotations[ExternalTopicAnnotation]
	return topicAnnotationValue, ok
}

func (r *Reconciler) addFinalizerSecret(ctx context.Context, finalizer string, secret *corev1.Secret) error {
	if !containsFinalizerSecret(secret, finalizer) {
		secret := secret.DeepCopy() // Do not modify informer copy.
		secret.Finalizers = append(secret.Finalizers, finalizer)
		_, err := r.KubeClient.CoreV1().Secrets(secret.GetNamespace()).Update(ctx, secret, metav1.UpdateOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to add finalizer to Secret %s/%s: %w", secret.GetNamespace(), secret.GetName(), err)
		}
	}
	return nil
}

func (r *Reconciler) removeFinalizerSecret(ctx context.Context, finalizer string, secret *corev1.Secret) error {
	if secret != nil {
		newFinalizers := make([]string, 0, len(secret.Finalizers))
		for _, f := range secret.Finalizers {
			if f != finalizer {
				newFinalizers = append(newFinalizers, f)
			}
		}
		if len(newFinalizers) != len(secret.Finalizers) {
			secret := secret.DeepCopy() // Do not modify informer copy.
			secret.Finalizers = newFinalizers
			_, err := r.KubeClient.CoreV1().Secrets(secret.GetNamespace()).Update(ctx, secret, metav1.UpdateOptions{})
			if err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf("failed to remove finalizer %s from Secret %s/%s: %w", finalizer, secret.GetNamespace(), secret.GetName(), err)
			}
		}
	}
	return nil
}

func containsFinalizerSecret(secret *corev1.Secret, finalizer string) bool {
	if secret == nil {
		return false
	}
	for _, f := range secret.Finalizers {
		if f == finalizer {
			return true
		}
	}
	return false
}

func finalizerSecret(object metav1.Object) string {
	return fmt.Sprintf("%s/%s", "kafka.eventing", object.GetUID())
}

func (r *Reconciler) getCaCerts() (*string, error) {
	secret, err := r.SecretLister.Secrets(r.SystemNamespace).Get(brokerIngressTLSSecretName)
	if err != nil {
		return nil, fmt.Errorf("failed to get CA certs from %s/%s: %w", r.SystemNamespace, brokerIngressTLSSecretName, err)
	}
	caCerts, ok := secret.Data[caCertsSecretKey]
	if !ok {
		return nil, nil
	}
	return pointer.String(string(caCerts)), nil
}

func (r *Reconciler) setTrustBundles(ct *contract.Contract) error {
	tb, err := coreconfig.TrustBundles(r.ConfigMapLister.ConfigMaps(r.SystemNamespace))
	if err != nil {
		return fmt.Errorf("failed to get trust bundles: %w", err)
	}
	ct.TrustBundles = tb
	return nil
}
