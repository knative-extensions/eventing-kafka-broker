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
	"strconv"
	"time"

	"github.com/Shopify/sarama"

	"go.uber.org/multierr"
	"go.uber.org/zap"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/util/retry"

	"knative.dev/pkg/controller"
	"knative.dev/pkg/reconciler"
	"knative.dev/pkg/resolver"
	"knative.dev/pkg/system"

	v1 "knative.dev/eventing/pkg/apis/duck/v1"

	messagingv1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	"knative.dev/eventing-kafka/pkg/common/constants"

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
	// TopicPrefix is the Kafka Channel topic prefix - (topic name: knative-messaging-kafka.<channel-namespace>.<channel-name>).
	TopicPrefix          = "knative-messaging-kafka"
	DefaultDeliveryOrder = contract.DeliveryOrder_ORDERED
)

type Reconciler struct {
	*base.Reconciler
	*config.Env

	Resolver *resolver.URIResolver

	// NewKafkaClusterAdminClient creates new sarama ClusterAdmin. It's convenient to add this as Reconciler field so that we can
	// mock the function used during the reconciliation loop.
	NewKafkaClusterAdminClient kafka.NewClusterAdminClientFunc

	// NewKafkaClient creates new sarama Client. It's convenient to add this as Reconciler field so that we can
	// mock the function used during the reconciliation loop.
	NewKafkaClient kafka.NewClientFunc

	// InitOffsetsFunc initialize offsets for a provided set of topics and a provided consumer group id.
	// It's convenient to add this as Reconciler field so that we can mock the function used during the
	// reconciliation loop.
	InitOffsetsFunc kafka.InitOffsetsFunc

	ConfigMapLister corelisters.ConfigMapLister

	Prober prober.Prober

	IngressHost string
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
		Env:        r.Env,
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

	if err := r.TrackConfigMap(channelConfigMap, channel); err != nil {
		return fmt.Errorf("failed to track broker config: %w", err)
	}

	// get topic config
	topicConfig, err := r.topicConfig(logger, channelConfigMap, channel)
	if err != nil {
		return statusConditionManager.FailedToResolveConfig(err)
	}
	logger.Debug("topic config resolved", zap.Any("config", topicConfig))
	statusConditionManager.ConfigResolved()

	// get the secret to access Kafka
	secret, err := security.Secret(ctx, &security.MTConfigMapSecretLocator{ConfigMap: channelConfigMap}, r.SecretProviderFunc())
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
	saramaSecurityOption := security.NewSaramaSecurityOptionFromSecret(secret)

	if err := r.TrackSecret(secret, channel); err != nil {
		return fmt.Errorf("failed to track secret: %w", err)
	}

	topicName := kafka.ChannelTopic(TopicPrefix, channel)

	kafkaClusterAdminSaramaConfig, err := kafka.GetSaramaConfig(saramaSecurityOption)
	if err != nil {
		return statusConditionManager.FailedToCreateTopic(topicName, fmt.Errorf("error getting cluster admin sarama config: %w", err))
	}

	// Manually commit the offsets in KafkaChannel controller.
	// That's because we want to make sure we initialize the offsets within the controller
	// before dispatcher actually starts consuming messages.
	kafkaClientSaramaConfig, err := kafka.GetSaramaConfig(saramaSecurityOption, kafka.DisableOffsetAutoCommitConfigOption)
	if err != nil {
		return statusConditionManager.FailedToCreateTopic(topicName, fmt.Errorf("error getting cluster admin sarama config: %w", err))
	}

	kafkaClusterAdminClient, err := r.NewKafkaClusterAdminClient(topicConfig.BootstrapServers, kafkaClusterAdminSaramaConfig)
	if err != nil {
		return statusConditionManager.FailedToCreateTopic(topicName, fmt.Errorf("cannot obtain Kafka cluster admin, %w", err))
	}
	defer kafkaClusterAdminClient.Close()

	kafkaClient, err := r.NewKafkaClient(topicConfig.BootstrapServers, kafkaClientSaramaConfig)
	if err != nil {
		return statusConditionManager.FailedToCreateTopic(topicName, fmt.Errorf("error getting sarama config: %w", err))
	}
	defer kafkaClient.Close()

	// create the topic
	topic, err := kafka.CreateTopicIfDoesntExist(kafkaClusterAdminClient, logger, topicName, topicConfig)
	if err != nil {
		return statusConditionManager.FailedToCreateTopic(topic, err)
	}
	logger.Debug("Topic created", zap.Any("topic", topic))
	statusConditionManager.TopicReady(topic)

	// Get data plane config map.
	contractConfigMap, err := r.GetOrCreateDataPlaneConfigMap(ctx)
	if err != nil {
		return statusConditionManager.FailedToResolveConfig(err)
	}
	logger.Debug("Got contract config map")

	// Get data plane config data.
	ct, err := r.GetDataPlaneConfigMapData(logger, contractConfigMap)
	if err != nil || ct == nil {
		return statusConditionManager.FailedToGetDataFromConfigMap(err)
	}
	logger.Debug("Got contract data from config map", zap.Any(base.ContractLogKey, ct))

	// Get resource configuration
	channelResource, err := r.getChannelContractResource(ctx, topic, channel, secret, topicConfig)
	if err != nil {
		return statusConditionManager.FailedToResolveConfig(err)
	}
	coreconfig.SetDeadLetterSinkURIFromEgressConfig(&channel.Status.DeliveryStatus, channelResource.EgressConfig)

	// we still update the contract configMap even though there's an error.
	// however, we record this error to make the reconciler try again.
	subscriptionError := r.reconcileSubscribers(ctx, kafkaClient, kafkaClusterAdminClient, channel, channelResource)

	// Update contract data with the new contract configuration (add/update channel resource)
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

	if subscriptionError != nil {
		logger.Error("Error reconciling subscriptions. Going to try again.", zap.Error(subscriptionError))
		return subscriptionError
	}

	address := receiver.Address(r.IngressHost, channel)
	proberAddressable := prober.Addressable{
		Address: address,
		ResourceKey: types.NamespacedName{
			Namespace: channel.GetNamespace(),
			Name:      channel.GetName(),
		},
	}

	if status := r.Prober.Probe(ctx, proberAddressable, prober.StatusReady); status != prober.StatusReady {
		statusConditionManager.ProbesStatusNotReady(status)
		return nil // Object will get re-queued once probe status changes.
	}
	statusConditionManager.Addressable(address)

	return nil
}

func (r *Reconciler) FinalizeKind(ctx context.Context, channel *messagingv1beta1.KafkaChannel) reconciler.Event {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		return r.finalizeKind(ctx, channel)
	})
}

func (r *Reconciler) finalizeKind(ctx context.Context, channel *messagingv1beta1.KafkaChannel) reconciler.Event {
	logger := kafkalogging.CreateFinalizeMethodLogger(ctx, channel)

	// Get contract config map.
	contractConfigMap, err := r.GetOrCreateDataPlaneConfigMap(ctx)
	if err != nil {
		return fmt.Errorf("failed to get contract config map %s: %w", r.DataPlaneConfigMapAsString(), err)
	}
	logger.Debug("Got contract config map")

	// Get contract data.
	ct, err := r.GetDataPlaneConfigMapData(logger, contractConfigMap)
	if err != nil && ct == nil {
		return fmt.Errorf("failed to get contract: %w", err)
	}
	logger.Debug("Got contract data from config map", zap.Any(base.ContractLogKey, ct))

	channelIndex := coreconfig.FindResource(ct, channel.UID)
	if channelIndex != coreconfig.NoResource {
		coreconfig.DeleteResource(ct, channelIndex)

		logger.Debug("Channel deleted", zap.Int("index", channelIndex))

		// Resource changed, increment contract generation.
		coreconfig.IncrementContractGeneration(ct)

		// Update the configuration map with the new contract data.
		if err := r.UpdateDataPlaneConfigMap(ctx, ct, contractConfigMap); err != nil {
			return err
		}
		logger.Debug("Contract config map updated")
	}

	channel.Status.Address = nil

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

	//  Rationale: after deleting a topic closing a producer ends up blocking and requesting metadata for max.block.ms
	//  because topic metadata aren't available anymore.
	// 	See (under discussions KIPs, unlikely to be accepted as they are):
	// 	- https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=181306446
	// 	- https://cwiki.apache.org/confluence/display/KAFKA/KIP-286%3A+producer.send%28%29+should+not+block+on+metadata+update
	address := receiver.Address(r.IngressHost, channel)
	proberAddressable := prober.Addressable{
		Address: address,
		ResourceKey: types.NamespacedName{
			Namespace: channel.GetNamespace(),
			Name:      channel.GetName(),
		},
	}
	if status := r.Prober.Probe(ctx, proberAddressable, prober.StatusNotReady); status != prober.StatusNotReady {
		// Return a requeueKeyError that doesn't generate an event and it re-queues the object
		// for a new reconciliation.
		return controller.NewRequeueAfter(5 * time.Second)
	}

	// get the channel configmap
	channelConfigMap, err := r.channelConfigMap()
	if err != nil {
		return err
	}
	logger.Debug("configmap read", zap.Any("configmap", channelConfigMap))

	// get topic config
	topicConfig, err := r.topicConfig(logger, channelConfigMap, channel)
	if err != nil {
		return fmt.Errorf("failed to resolve channel config: %w", err)
	}

	logger.Debug("topic config resolved", zap.Any("config", topicConfig))

	// get the secret to access Kafka
	secret, err := security.Secret(ctx, &security.MTConfigMapSecretLocator{ConfigMap: channelConfigMap}, r.SecretProviderFunc())
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
	saramaSecurityOption := security.NewSaramaSecurityOptionFromSecret(secret)

	saramaConfig, err := kafka.GetSaramaConfig(saramaSecurityOption)
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

	topic, err := kafka.DeleteTopic(kafkaClusterAdminClient, kafka.ChannelTopic(TopicPrefix, channel))
	if err != nil {
		return err
	}

	logger.Debug("Topic deleted", zap.String("topic", topic))

	return nil
}

func (r *Reconciler) reconcileSubscribers(ctx context.Context, kafkaClient sarama.Client, kafkaClusterAdmin sarama.ClusterAdmin, channel *messagingv1beta1.KafkaChannel, channelContractResource *contract.Resource) error {
	logger := kafkalogging.CreateReconcileMethodLogger(ctx, channel)

	channel.Status.Subscribers = make([]v1.SubscriberStatus, 0)
	var globalErr error
	for i := range channel.Spec.Subscribers {
		s := &channel.Spec.Subscribers[i]
		logger = logger.With(zap.Any("subscription", s))
		err := r.reconcileSubscriber(ctx, kafkaClient, kafkaClusterAdmin, channel, s, channelContractResource)
		if err != nil {
			logger.Error("error reconciling subscription. marking subscription as not ready", zap.Error(err))
			channel.Status.Subscribers = append(channel.Status.Subscribers, v1.SubscriberStatus{
				UID:                s.UID,
				ObservedGeneration: s.Generation,
				Ready:              corev1.ConditionFalse,
				Message:            fmt.Sprintf("Subscription not ready: %v", err),
			})
			globalErr = multierr.Append(globalErr, err)
		} else {
			logger.Debug("marking subscription as ready")
			channel.Status.Subscribers = append(channel.Status.Subscribers, v1.SubscriberStatus{
				UID:                s.UID,
				ObservedGeneration: s.Generation,
				Ready:              corev1.ConditionTrue,
			})
		}
	}
	return globalErr
}

func (r *Reconciler) reconcileSubscriber(ctx context.Context, kafkaClient sarama.Client, kafkaClusterAdmin sarama.ClusterAdmin, channel *messagingv1beta1.KafkaChannel, subscriberSpec *v1.SubscriberSpec, channelContractResource *contract.Resource) error {
	logger := kafkalogging.CreateReconcileMethodLogger(ctx, channel)

	logger.Debug("Reconciling initial offset for subscription", zap.Any("subscription", subscriberSpec), zap.Any("channel", channel))
	err := r.reconcileInitialOffset(ctx, channel, subscriberSpec, kafkaClient, kafkaClusterAdmin)
	if err != nil {
		return fmt.Errorf("initial offset cannot be committed: %v", err)
	}
	logger.Debug("Reconciled initial offset for subscription. ", zap.Any("subscription", subscriberSpec))

	subscriberIndex := coreconfig.FindEgress(channelContractResource.Egresses, subscriberSpec.UID)
	subscriberConfig, err := r.getSubscriberConfig(ctx, channel, subscriberSpec)
	if err != nil {
		return fmt.Errorf("failed to resolve subscriber config: %w", err)
	}

	coreconfig.AddOrUpdateEgressConfigForResource(channelContractResource, subscriberConfig, subscriberIndex)
	return nil
}

func (r *Reconciler) reconcileInitialOffset(ctx context.Context, channel *messagingv1beta1.KafkaChannel, sub *v1.SubscriberSpec, kafkaClient sarama.Client, kafkaClusterAdmin sarama.ClusterAdmin) error {
	subscriptionStatus := findSubscriptionStatus(channel, sub.UID)
	if subscriptionStatus != nil && subscriptionStatus.Ready == corev1.ConditionTrue {
		// subscription is ready, the offsets must have been initialized already
		return nil
	}

	topicName := kafka.ChannelTopic(TopicPrefix, channel)
	groupID := consumerGroup(channel, sub)
	_, err := r.InitOffsetsFunc(ctx, kafkaClient, kafkaClusterAdmin, []string{topicName}, groupID)
	return err
}

func (r *Reconciler) getSubscriberConfig(ctx context.Context, channel *messagingv1beta1.KafkaChannel, subscriber *v1.SubscriberSpec) (*contract.Egress, error) {
	subscriberURI := subscriber.SubscriberURI.String()
	if subscriberURI == "" {
		return nil, fmt.Errorf("failed to resolve Subscription.Spec.Subscriber: empty subscriber URI")
	}

	egress := &contract.Egress{
		Destination:   subscriberURI,
		ConsumerGroup: consumerGroup(channel, subscriber),
		DeliveryOrder: DefaultDeliveryOrder,
		Uid:           string(subscriber.UID),
		ReplyStrategy: &contract.Egress_DiscardReply{},
	}

	if subscriber.ReplyURI != nil {
		egress.ReplyStrategy = &contract.Egress_ReplyUrl{
			ReplyUrl: subscriber.ReplyURI.String(),
		}
	}

	subscriptionEgressConfig, err := coreconfig.EgressConfigFromDelivery(ctx, r.Resolver, channel, subscriber.Delivery, r.DefaultBackoffDelayMs)
	if err != nil {
		return nil, err
	}
	channelEgressConfig, err := coreconfig.EgressConfigFromDelivery(ctx, r.Resolver, channel, channel.Spec.Delivery, r.DefaultBackoffDelayMs)
	if err != nil {
		return nil, err
	}

	// Merge Channel and Subscription egress configuration prioritizing the Subscription configuration.
	egress.EgressConfig = coreconfig.MergeEgressConfig(subscriptionEgressConfig, channelEgressConfig)

	return egress, nil
}

func (r *Reconciler) channelConfigMap() (*corev1.ConfigMap, error) {
	// TODO: do we want to support namespaced channels? they're not supported at the moment.

	namespace := system.Namespace()
	cm, err := r.ConfigMapLister.ConfigMaps(namespace).Get(r.Env.GeneralConfigMapName)
	if err != nil {
		return nil, fmt.Errorf("failed to get configmap %s/%s: %w", namespace, r.Env.GeneralConfigMapName, err)
	}

	return cm, nil
}

func (r *Reconciler) topicConfig(logger *zap.Logger, cm *corev1.ConfigMap, channel *messagingv1beta1.KafkaChannel) (*kafka.TopicConfig, error) {
	bootstrapServers, err := kafka.BootstrapServersFromConfigMap(logger, cm)
	if err != nil {
		return nil, fmt.Errorf("unable to get bootstrapServers from configmap: %w - ConfigMap data: %v", err, cm.Data)
	}

	// Parse & Format the RetentionDuration into Sarama retention.ms string
	retentionDuration, err := channel.Spec.ParseRetentionDuration()
	if err != nil {
		// Should never happen with webhook defaulting and validation in place.
		logger.Error("Error parsing RetentionDuration, using default instead", zap.String("RetentionDuration", channel.Spec.RetentionDuration), zap.Error(err))
		retentionDuration = constants.DefaultRetentionDuration
	}
	retentionMillisString := strconv.FormatInt(retentionDuration.Milliseconds(), 10)

	return &kafka.TopicConfig{
		TopicDetail: sarama.TopicDetail{
			NumPartitions:     channel.Spec.NumPartitions,
			ReplicationFactor: channel.Spec.ReplicationFactor,
			ConfigEntries: map[string]*string{
				constants.KafkaTopicConfigRetentionMs: &retentionMillisString,
			},
		},
		BootstrapServers: bootstrapServers,
	}, nil
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
		Reference: &contract.Reference{
			Uuid:      string(channel.GetUID()),
			Namespace: channel.GetNamespace(),
			Name:      channel.GetName(),
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

	egressConfig, err := coreconfig.EgressConfigFromDelivery(ctx, r.Resolver, channel, channel.Spec.Delivery, r.DefaultBackoffDelayMs)
	if err != nil {
		return nil, err
	}
	resource.EgressConfig = egressConfig

	return resource, nil
}

// consumerGroup returns a consumerGroup name for the given channel and subscription
func consumerGroup(channel *messagingv1beta1.KafkaChannel, sub *v1.SubscriberSpec) string {
	return fmt.Sprintf("kafka.%s.%s.%s", channel.Namespace, channel.Name, string(sub.UID))
}

func findSubscriptionStatus(kc *messagingv1beta1.KafkaChannel, subUID types.UID) *v1.SubscriberStatus {
	for _, subStatus := range kc.Status.Subscribers {
		if subStatus.UID == subUID {
			return &subStatus
		}
	}
	return nil
}
