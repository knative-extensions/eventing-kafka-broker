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
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/channel/resources"
	"knative.dev/eventing/pkg/apis/feature"
	"knative.dev/pkg/network"
	"knative.dev/pkg/resolver"
	"knative.dev/pkg/system"

	messagingv1beta1 "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/messaging/v1beta1"
	v1 "knative.dev/eventing/pkg/apis/duck/v1"
	messaging "knative.dev/eventing/pkg/apis/messaging/v1"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/kmeta"
	"knative.dev/pkg/reconciler"

	kafkasource "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/sources/v1beta1"
	kedafunc "knative.dev/eventing-kafka-broker/control-plane/pkg/autoscaler/keda"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/prober"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/receiver"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	channelreconciler "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/channel"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/security"

	apisconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/config"
	internalscg "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing/v1alpha1"
	internalsclient "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/clientset/versioned"
	internalslst "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/listers/eventing/v1alpha1"
	coreconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/core/config"
	kafkalogging "knative.dev/eventing-kafka-broker/control-plane/pkg/logging"

	messaginglisters "knative.dev/eventing/pkg/client/listers/messaging/v1"
)

const (
	DefaultDeliveryOrder = kafkasource.Ordered

	KafkaChannelConditionSubscribersReady apis.ConditionType = "Subscribers" // condition is registered by controller

	kafkaChannelTLSSecretName = "kafka-channel-ingress-server-tls" //nolint:gosec // This is not a hardcoded credential
	caCertsSecretKey          = "ca.crt"
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

	Resolver *resolver.URIResolver

	// NewKafkaClusterAdminClient creates new sarama ClusterAdmin. It's convenient to add this as Reconciler field so that we can
	// mock the function used during the reconciliation loop.
	NewKafkaClusterAdminClient kafka.NewClusterAdminClientFunc

	ConfigMapLister    corelisters.ConfigMapLister
	ServiceLister      corelisters.ServiceLister
	SubscriptionLister messaginglisters.SubscriptionLister

	Prober prober.Prober

	IngressHost string

	ConsumerGroupLister internalslst.ConsumerGroupLister
	InternalsClient     internalsclient.Interface
	KafkaFeatureFlags   *apisconfig.KafkaFeatureFlags
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
	secret, err := security.Secret(ctx, &security.MTConfigMapSecretLocator{ConfigMap: channelConfigMap, UseNamespaceInConfigmap: true}, r.SecretProviderFunc())
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

	authContext, err := security.ResolveAuthContextFromLegacySecret(secret)
	if err != nil {
		return statusConditionManager.FailedToResolveConfig(fmt.Errorf("failed to resolve auth context: %w", err))
	}

	// get security option for Sarama with secret info in it
	saramaSecurityOption := security.NewSaramaSecurityOptionFromSecret(authContext.VirtualSecret)

	if channel.Status.Annotations == nil {
		channel.Status.Annotations = make(map[string]string)
	}
	// Check if there is an existing topic name for this channel. If there is, reconcile the channel with the existing name.
	// If not, create a new topic name from the channel topic name template.
	var topicName string
	var existingTopic bool
	if topicName, existingTopic = channel.Status.Annotations[kafka.TopicAnnotation]; !existingTopic {
		topicName, err = r.KafkaFeatureFlags.ExecuteChannelsTopicTemplate(channel.ObjectMeta)
		if err != nil {
			return err
		}
	}
	channel.Status.Annotations[kafka.TopicAnnotation] = topicName

	kafkaClusterAdminSaramaConfig, err := kafka.GetSaramaConfig(saramaSecurityOption)
	if err != nil {
		return statusConditionManager.FailedToCreateTopic(topicName, fmt.Errorf("error getting cluster admin sarama config: %w", err))
	}

	kafkaClusterAdminClient, err := r.NewKafkaClusterAdminClient(topicConfig.BootstrapServers, kafkaClusterAdminSaramaConfig)
	if err != nil {
		return statusConditionManager.FailedToCreateTopic(topicName, fmt.Errorf("cannot obtain Kafka cluster admin, %w", err))
	}
	defer kafkaClusterAdminClient.Close()

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
	channelResource, err := r.getChannelContractResource(ctx, topic, channel, authContext, topicConfig)
	if err != nil {
		return statusConditionManager.FailedToResolveConfig(err)
	}
	coreconfig.SetDeadLetterSinkURIFromEgressConfig(&channel.Status.DeliveryStatus, channelResource.EgressConfig)

	allReady, subscribersError := r.reconcileSubscribers(ctx, channel, topicName, topicConfig.BootstrapServers, secret)
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
	changed := coreconfig.AddOrUpdateResourceConfig(ct, channelResource, channelIndex, logger)
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

	// We update receiver pods annotation regardless of our contract changed or not due to the fact
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

	channelService, err := r.reconcileChannelService(ctx, channel)
	if err != nil {
		logger.Error("Error reconciling the backwards compatibility channel service.", zap.Error(err))
		return err
	}

	var addressableStatus duckv1.AddressStatus
	channelHttpsHost := network.GetServiceHostname(r.Env.IngressName, r.SystemNamespace)
	channelHttpHost := network.GetServiceHostname(channelService.Name, channel.Namespace)
	transportEncryptionFlags := feature.FromContext(ctx)
	if transportEncryptionFlags.IsPermissiveTransportEncryption() {
		caCerts, err := r.getCaCerts()
		if err != nil {
			return err
		}

		httpAddress := receiver.ChannelHTTPAddress(channelHttpHost)
		httpsAddress := receiver.HTTPSAddress(channelHttpsHost, channelService, caCerts)
		// Permissive mode:
		// - status.address http address with path-based routing
		// - status.addresses:
		//   - https address with path-based routing
		//   - http address with path-based routing
		addressableStatus.Addresses = []duckv1.Addressable{httpsAddress, httpAddress}
		addressableStatus.Address = &httpAddress
	} else if transportEncryptionFlags.IsStrictTransportEncryption() {
		// Strict mode: (only https addresses)
		// - status.address https address with path-based routing
		// - status.addresses:
		//   - https address with path-based routing
		caCerts, err := r.getCaCerts()
		if err != nil {
			return err
		}

		httpsAddress := receiver.HTTPSAddress(channelHttpsHost, channelService, caCerts)
		addressableStatus.Addresses = []duckv1.Addressable{httpsAddress}
		addressableStatus.Address = &httpsAddress
	} else {
		httpAddress := receiver.ChannelHTTPAddress(channelHttpHost)
		addressableStatus.Address = &httpAddress
		addressableStatus.Addresses = []duckv1.Addressable{httpAddress}
	}

	address := addressableStatus.Address.URL.URL()

	proberAddressable := prober.Addressable{
		Address: address,
		ResourceKey: types.NamespacedName{
			Namespace: channel.GetNamespace(),
			Name:      channel.GetName(),
		},
	}

	logger.Debug("Going to probe address to check ingress readiness for the channel.", zap.Any("proberAddressable", proberAddressable))
	if status := r.Prober.Probe(ctx, proberAddressable, prober.StatusReady); status != prober.StatusReady {
		logger.Debug("Ingress is not ready for the channel. Going to requeue.", zap.Any("proberAddressable", proberAddressable))
		statusConditionManager.ProbesStatusNotReady(status)
		return nil // Object will get re-queued once probe status changes.
	}

	statusConditionManager.ProbesStatusReady()
	channel.Status.Address = addressableStatus.Address
	channel.Status.Addresses = addressableStatus.Addresses
	channel.GetConditionSet().Manage(channel.GetStatus()).MarkTrue(base.ConditionAddressable)

	return nil
}

func (r *Reconciler) reconcileChannelService(ctx context.Context, channel *messagingv1beta1.KafkaChannel) (*corev1.Service, error) {
	expected, err := resources.MakeK8sService(channel, resources.ExternalService(r.DataPlaneNamespace, channelreconciler.NewChannelIngressServiceName))
	if err != nil {
		return expected, fmt.Errorf("failed to create the channel service object: %w", err)
	}

	svc, err := r.ServiceLister.Services(channel.Namespace).Get(resources.MakeChannelServiceName(channel.Name))
	if err != nil {
		if apierrors.IsNotFound(err) {
			_, err = r.KubeClient.CoreV1().Services(channel.Namespace).Create(ctx, expected, metav1.CreateOptions{})
			if err != nil {
				return expected, fmt.Errorf("failed to create the channel service object: %w", err)
			}
			return expected, nil
		}
		return expected, err
	} else if !equality.Semantic.DeepEqual(svc.Spec, expected.Spec) {
		svc = svc.DeepCopy()
		svc.Spec = expected.Spec

		_, err = r.KubeClient.CoreV1().Services(channel.Namespace).Update(ctx, svc, metav1.UpdateOptions{})
		if err != nil {
			return expected, fmt.Errorf("failed to update the channel service: %w", err)
		}
	}
	// Check to make sure that the KafkaChannel owns this service and if not, complain.
	if !metav1.IsControlledBy(svc, channel) {
		return expected, fmt.Errorf("kafkachannel: %s/%s does not own Service: %q", channel.Namespace, channel.Name, svc.Name)
	}
	return expected, nil
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

	// We update receiver annotation regardless of our contract changed or not due to the fact
	// that in a previous reconciliation we might have failed to update one of our data plane pod annotation, so we want
	// to update anyway remaining annotations with the contract generation that was saved in the CM.
	// Note: if there aren't changes to be done at the pod annotation level, we just skip the update.

	// Update volume generation annotation of receiver pods
	if err := r.UpdateReceiverPodsAnnotation(ctx, logger, ct.Generation); err != nil {
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
	secret, err := security.Secret(ctx, &security.MTConfigMapSecretLocator{ConfigMap: channelConfigMap, UseNamespaceInConfigmap: true}, r.SecretProviderFunc())
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

	authContext, err := security.ResolveAuthContextFromLegacySecret(secret)
	if err != nil {
		return fmt.Errorf("failed to resolve auth context: %w", err)
	}

	// get security option for Sarama with secret info in it
	saramaSecurityOption := security.NewSaramaSecurityOptionFromSecret(authContext.VirtualSecret)

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

	topicName, err := r.KafkaFeatureFlags.ExecuteChannelsTopicTemplate(channel.ObjectMeta)
	if err != nil {
		return err
	}
	topic, err := kafka.DeleteTopic(kafkaClusterAdminClient, topicName)
	if err != nil {
		return err
	}

	logger.Debug("Topic deleted", zap.String("topic", topic))

	return nil
}

func (r *Reconciler) reconcileSubscribers(ctx context.Context, channel *messagingv1beta1.KafkaChannel, topicName string, bootstrapServers []string, secret *corev1.Secret) (bool, error) {
	logger := kafkalogging.CreateReconcileMethodLogger(ctx, channel)

	channel.Status.Subscribers = make([]v1.SubscriberStatus, 0)
	var globalErr error
	currentCgs := make(map[string]*internalscg.ConsumerGroup, len(channel.Spec.Subscribers))
	allReady := true
	for i := range channel.Spec.Subscribers {
		s := &channel.Spec.Subscribers[i]
		logger = logger.With(zap.Any("subscriber", s))
		cg, err := r.reconcileConsumerGroup(ctx, channel, s, topicName, bootstrapServers, secret)
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

func (r Reconciler) reconcileConsumerGroup(ctx context.Context, channel *messagingv1beta1.KafkaChannel, s *v1.SubscriberSpec, topicName string, bootstrapServers []string, secret *corev1.Secret) (*internalscg.ConsumerGroup, error) {

	expectedCg := &internalscg.ConsumerGroup{
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

	// TODO: make keda annotation values configurable and maybe unexposed
	subscriptionAnnotations, err := r.getSubscriptionAnnotations(channel, s)
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("failed to extract subscription annotations for subscriber %v: %w", s, err)
	}
	expectedCg.Annotations = kedafunc.SetAutoscalingAnnotations(subscriptionAnnotations)

	if secret != nil {
		expectedCg.Spec.Template.Spec.Auth = &internalscg.Auth{
			SecretSpec: &internalscg.SecretSpec{
				Ref: &internalscg.SecretReference{
					Name:      secret.Name,
					Namespace: secret.Namespace,
				},
			},
		}
	}

	cg, err := r.ConsumerGroupLister.ConsumerGroups(channel.GetNamespace()).Get(string(s.UID)) // Get by consumer group id
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, err
	}
	if apierrors.IsNotFound(err) {
		cg, err = r.InternalsClient.InternalV1alpha1().ConsumerGroups(expectedCg.GetNamespace()).Create(ctx, expectedCg, metav1.CreateOptions{})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("failed to create consumer group %s/%s: %w", expectedCg.GetNamespace(), expectedCg.GetName(), err)
		}
		return cg, nil
	}

	if equality.Semantic.DeepDerivative(expectedCg.Spec, cg.Spec) && equality.Semantic.DeepDerivative(expectedCg.Annotations, cg.Annotations) {
		return cg, nil
	}

	newCg := &internalscg.ConsumerGroup{
		TypeMeta:   cg.TypeMeta,
		ObjectMeta: cg.ObjectMeta,
		Spec:       expectedCg.Spec,
		Status:     cg.Status,
	}
	newCg.Annotations = expectedCg.Annotations

	if cg, err = r.InternalsClient.InternalV1alpha1().ConsumerGroups(cg.GetNamespace()).Update(ctx, newCg, metav1.UpdateOptions{}); err != nil {
		return nil, fmt.Errorf("failed to update consumer group %s/%s: %w", newCg.GetNamespace(), newCg.GetName(), err)
	}

	return cg, nil
}

func (r *Reconciler) getChannelContractResource(ctx context.Context, topic string, channel *messagingv1beta1.KafkaChannel, auth *security.NetSpecAuthContext, config *kafka.TopicConfig) (*contract.Resource, error) {
	resource := &contract.Resource{
		Uid:    string(channel.UID),
		Topics: []string{topic},
		Ingress: &contract.Ingress{
			Host: receiver.Host(channel.GetNamespace(), channel.GetName()),
		},
		BootstrapServers: config.GetBootstrapServers(),
		Reference: &contract.Reference{
			Uuid:      string(channel.GetUID()),
			Namespace: channel.GetNamespace(),
			Name:      channel.GetName(),
		},
	}

	if auth != nil && auth.MultiSecretReference != nil {
		resource.Auth = &contract.Resource_MultiAuthSecret{
			MultiAuthSecret: auth.MultiSecretReference,
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
func consumerGroup(channel *messagingv1beta1.KafkaChannel, s *v1.SubscriberSpec) string {
	return fmt.Sprintf("kafka.%s.%s.%s", channel.Namespace, channel.Name, string(s.UID))
}

func (r *Reconciler) channelConfigMap() (*corev1.ConfigMap, error) {
	// TODO: do we want to support namespaced channels? they're not supported at the moment.

	namespace := r.DataPlaneNamespace
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
		retentionDuration = messagingv1beta1.DefaultRetentionDuration
	}
	retentionMillisString := strconv.FormatInt(retentionDuration.Milliseconds(), 10)

	return &kafka.TopicConfig{
		TopicDetail: sarama.TopicDetail{
			NumPartitions:     channel.Spec.NumPartitions,
			ReplicationFactor: channel.Spec.ReplicationFactor,
			ConfigEntries: map[string]*string{
				messagingv1beta1.KafkaTopicConfigRetentionMs: &retentionMillisString,
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

func (r *Reconciler) getSubscriptionAnnotations(channel *messagingv1beta1.KafkaChannel, subscriber *v1.SubscriberSpec) (map[string]string, error) {
	subscriptions, err := r.SubscriptionLister.Subscriptions(channel.GetNamespace()).List(labels.Everything())
	if err != nil {
		return nil, fmt.Errorf("failed to list subscriptions in namespace %s: %w", channel.GetNamespace(), err)
	}

	for _, s := range subscriptions {
		if s.UID == subscriber.UID {
			return s.Annotations, nil
		}
	}

	return nil, apierrors.NewNotFound(messaging.SchemeGroupVersion.WithResource("subscriptions").GroupResource(), string(subscriber.UID))
}

func (r *Reconciler) getCaCerts() (string, error) {
	secret, err := r.SecretLister.Secrets(system.Namespace()).Get(kafkaChannelTLSSecretName)
	if err != nil {
		return "", fmt.Errorf("failed to get CA certs from %s/%s: %w", system.Namespace(), kafkaChannelTLSSecretName, err)
	}
	caCerts, ok := secret.Data[caCertsSecretKey]
	if !ok {
		return "", fmt.Errorf("failed to get CA certs from %s/%s: missing %s key", system.Namespace(), kafkaChannelTLSSecretName, caCertsSecretKey)
	}
	return string(caCerts), nil
}
