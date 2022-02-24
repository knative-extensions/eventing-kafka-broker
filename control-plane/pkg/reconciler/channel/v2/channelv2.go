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

package v2

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	corelisters "k8s.io/client-go/listers/core/v1"

	v1 "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/kmeta"
	"knative.dev/pkg/reconciler"
	"knative.dev/pkg/system"

	messagingv1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	"knative.dev/eventing-kafka/pkg/common/constants"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/prober"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/receiver"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/security"

	internals "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing"
	internalscg "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing/v1alpha1"
	internalsclient "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/clientset/versioned"
	internalslst "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/listers/eventing/v1alpha1"
	coreconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/core/config"
	kafkalogging "knative.dev/eventing-kafka-broker/control-plane/pkg/logging"
)

const (
	// TopicPrefix is the Kafka Channel topic prefix - (topic name: knative-messaging-kafka.<channel-namespace>.<channel-name>).
	TopicPrefix          = "knative-messaging-kafka"
	DefaultDeliveryOrder = internals.Ordered

	KafkaChannelConditionSubscribersReady apis.ConditionType = "SubscribersReady" //condition is registered by controller
)

var (
	conditionSet = apis.NewLivingConditionSet(
		KafkaChannelConditionSubscribersReady,
		base.ConditionTopicReady,
		base.ConditionConfigMapUpdated,
		base.ConditionConfigParsed,
		base.ConditionProbeSucceeded,
	)
)

type Reconciler struct {
	*base.Reconciler
	*config.Env

	// NewKafkaClusterAdminClient creates new sarama ClusterAdmin. It's convenient to add this as Reconciler field so that we can
	// mock the function used during the reconciliation loop.
	NewKafkaClusterAdminClient kafka.NewClusterAdminClientFunc

	// NewKafkaClient creates new sarama Client. It's convenient to add this as Reconciler field so that we can
	// mock the function used during the reconciliation loop.
	NewKafkaClient kafka.NewClientFunc

	ConfigMapLister corelisters.ConfigMapLister

	Prober prober.Prober

	IngressHost string

	ConsumerGroupLister internalslst.ConsumerGroupLister
	InternalsClient     internalsclient.Interface
}

func (r *Reconciler) ReconcileKind(ctx context.Context, channel *messagingv1beta1.KafkaChannel) reconciler.Event {
	logger := kafkalogging.CreateReconcileMethodLogger(ctx, channel)

	statusConditionManager := base.StatusConditionManager{
		Object:     channel,
		SetAddress: channel.Status.SetAddress,
		Env:        r.Env,
		Recorder:   controller.GetEventRecorder(ctx),
	}

	// Get the channel configmap
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
	channelConfig := r.getChannelContractResource(topic, channel, secret, topicConfig)

	allReady, subscribersError := r.reconcileSubscribers(ctx, channel, topicName, topicConfig.BootstrapServers)
	if subscribersError != nil {
		channel.GetConditionSet().Manage(&channel.Status).MarkFalse(KafkaChannelConditionSubscribersReady, "failed to reconcile all subscribers", subscribersError.Error())
		return subscribersError
	}
	if !allReady { //no need to return error. Not ready because of consumer group status. Will be ok once consumer group is reconciled
		channel.GetConditionSet().Manage(&channel.Status).MarkUnknown(KafkaChannelConditionSubscribersReady, "all subscribers not ready", "failed to reconcile consumer group")
	} else {
		channel.GetConditionSet().Manage(&channel.Status).MarkTrue(KafkaChannelConditionSubscribersReady)
	}

	// Update contract data with the new contract configuration (add/update channel resource)
	channelIndex := coreconfig.FindResource(ct, channel.UID)
	changed := coreconfig.AddOrUpdateResourceConfig(ct, channelConfig, channelIndex, logger)
	logger.Debug("Change detector", zap.Int("changed", changed))

	if changed == coreconfig.ResourceChanged {
		logger.Debug("Contract changed", zap.Int("changed", changed))

		ct.IncrementGeneration()

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
		ct.IncrementGeneration()

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

func (r *Reconciler) reconcileSubscribers(ctx context.Context, channel *messagingv1beta1.KafkaChannel, topicName string, bootstrapServers []string) (bool, error) {
	logger := kafkalogging.CreateReconcileMethodLogger(ctx, channel)

	channel.Status.Subscribers = make([]v1.SubscriberStatus, 0)
	var globalErr error
	currentCgs := make(map[string]*internalscg.ConsumerGroup, len(channel.Spec.Subscribers))
	allReady := true
	for i := range channel.Spec.Subscribers {
		s := &channel.Spec.Subscribers[i]
		logger = logger.With(zap.Any("subscriber", s))
		cg, err := r.reconcileConsumerGroup(ctx, channel, s, topicName, bootstrapServers)
		if err != nil {
			logger.Error("error reconciling subscriber. marking subscriber as not ready", zap.Error(err))
			msg := fmt.Sprintf("Subscriber %v not ready: %v", s.UID, err)
			channel.Status.Subscribers = append(channel.Status.Subscribers, v1.SubscriberStatus{
				UID:                s.UID,
				ObservedGeneration: s.Generation,
				Ready:              corev1.ConditionFalse,
				Message:            msg,
			})
			allReady = false
			globalErr = multierr.Append(globalErr, errors.New(msg))
		} else {
			currentCgs[cg.Name] = cg // Adding reconciled consumer group to map
			if cg.IsReady() {
				logger.Debug("marking subscriber as ready")
				channel.Status.Subscribers = append(channel.Status.Subscribers, v1.SubscriberStatus{
					UID:                s.UID,
					ObservedGeneration: s.Generation,
					Ready:              corev1.ConditionTrue,
				})
			} else {
				topLevelCondition := cg.GetConditionSet().Manage(cg.GetStatus()).GetTopLevelCondition()
				if topLevelCondition == nil {
					msg := fmt.Sprintf("Subscriber %v not ready: %v", s.UID, "consumer group status unknown")
					channel.Status.Subscribers = append(channel.Status.Subscribers, v1.SubscriberStatus{
						UID:                s.UID,
						ObservedGeneration: s.Generation,
						Ready:              corev1.ConditionUnknown,
						Message:            msg,
					})
					allReady = false
				} else {
					msg := fmt.Sprintf("Subscriber %v not ready: %v %v", s.UID, topLevelCondition.Reason, topLevelCondition.Message)
					channel.Status.Subscribers = append(channel.Status.Subscribers, v1.SubscriberStatus{
						UID:                s.UID,
						ObservedGeneration: s.Generation,
						Ready:              corev1.ConditionFalse,
						Message:            msg,
					})
					allReady = false
				}
			}
		}
	}

	// Get all consumer groups associated with this Channel
	selector := labels.SelectorFromSet(map[string]string{internalscg.KafkaChannelNameLabel: channel.Name})
	channelCgs, err := r.ConsumerGroupLister.ConsumerGroups(channel.GetNamespace()).List(selector)
	if err != nil {
		globalErr = multierr.Append(globalErr, err)
	}
	for _, cg := range channelCgs {
		_, found := currentCgs[cg.Name]
		if !found { // ConsumerGroup needs to be deleted since it isn't associated with an existing subscriber (subscriber may have been deleted)
			err := r.finalizeConsumerGroup(ctx, cg)
			if err != nil {
				globalErr = multierr.Append(globalErr, err)
			}
		}
	}

	return allReady, globalErr
}

func (r Reconciler) reconcileConsumerGroup(ctx context.Context, channel *messagingv1beta1.KafkaChannel, s *v1.SubscriberSpec, topicName string, bootstrapServers []string) (*internalscg.ConsumerGroup, error) {
	newcg := &internalscg.ConsumerGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      string(s.UID),
			Namespace: channel.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*kmeta.NewControllerRef(channel),
			},
			Labels: map[string]string{
				internalscg.KafkaChannelNameLabel:           channel.Name, // Identifies the new ConsumerGroup as associated with this channel (same namespace)
				internalscg.UserFacingResourceLabelSelector: strings.ToLower(channel.GetGroupVersionKind().Kind),
			},
		},
		Spec: internalscg.ConsumerGroupSpec{
			Template: internalscg.ConsumerTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						internalscg.ConsumerLabelSelector: string(s.UID),
					},
				},
				Spec: internalscg.ConsumerSpec{
					Topics: []string{topicName},
					Configs: internalscg.ConsumerConfigs{Configs: map[string]string{
						"group.id":          consumerGroup(channel, s),
						"bootstrap.servers": strings.Join(bootstrapServers, ","),
					}},
					Delivery: &internalscg.DeliverySpec{
						DeliverySpec: channel.Spec.Delivery,
						Ordering:     DefaultDeliveryOrder,
					},
					Subscriber: duckv1.Destination{
						URI: s.SubscriberURI,
					},
				},
			},
		},
	}

	cg, err := r.ConsumerGroupLister.ConsumerGroups(channel.GetNamespace()).Get(string(s.UID)) // Get by consumer group id
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, err
	}

	if apierrors.IsNotFound(err) {
		cg, err = r.InternalsClient.InternalV1alpha1().ConsumerGroups(newcg.GetNamespace()).Create(ctx, newcg, metav1.CreateOptions{})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("failed to create consumer group %s/%s: %w", newcg.GetNamespace(), newcg.GetName(), err)
		}
		if apierrors.IsAlreadyExists(err) {
			return newcg, nil
		}
		return cg, nil
	}

	if equality.Semantic.DeepDerivative(newcg.Spec, cg.Spec) {
		return cg, nil
	}

	newCg := &internalscg.ConsumerGroup{
		TypeMeta:   cg.TypeMeta,
		ObjectMeta: cg.ObjectMeta,
		Spec:       newcg.Spec,
		Status:     cg.Status,
	}
	if cg, err = r.InternalsClient.InternalV1alpha1().ConsumerGroups(cg.GetNamespace()).Update(ctx, newCg, metav1.UpdateOptions{}); err != nil {
		return nil, fmt.Errorf("failed to update consumer group %s/%s: %w", newCg.GetNamespace(), newCg.GetName(), err)
	}

	return cg, nil
}

func (r *Reconciler) getChannelContractResource(topic string, channel *messagingv1beta1.KafkaChannel, secret *corev1.Secret, config *kafka.TopicConfig) *contract.Resource {
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

	return resource
}

// consumerGroup returns a consumerGroup name for the given channel and subscription
func consumerGroup(channel *messagingv1beta1.KafkaChannel, s *v1.SubscriberSpec) string {
	return fmt.Sprintf("kafka.%s.%s.%s", channel.Namespace, channel.Name, string(s.UID))
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

func (r Reconciler) finalizeConsumerGroup(ctx context.Context, cg *internalscg.ConsumerGroup) error {
	dOpts := metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{UID: &cg.UID},
	}
	err := r.InternalsClient.InternalV1alpha1().ConsumerGroups(cg.GetNamespace()).Delete(ctx, cg.GetName(), dOpts)
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to remove consumer group %s/%s: %w", cg.GetNamespace(), cg.GetName(), err)
	}
	return nil
}
