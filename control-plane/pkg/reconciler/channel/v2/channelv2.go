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
	"fmt"
	"strings"

	"go.uber.org/multierr"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corelisters "k8s.io/client-go/listers/core/v1"

	v1 "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/pkg/apis"
	"knative.dev/pkg/kmeta"
	"knative.dev/pkg/reconciler"
	"knative.dev/pkg/system"

	messagingv1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/receiver"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka"

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

	KafkaChannelConditionConsumerGroup    apis.ConditionType = "ConsumerGroup" //condition is registered by controller
	KafkaChannelConditionConfigMapUpdated apis.ConditionType = "ConfigMapUpdated"
)

var (
	conditionSet = apis.NewLivingConditionSet(
		KafkaChannelConditionConsumerGroup,
		KafkaChannelConditionConfigMapUpdated,
	)
)

type Reconciler struct {
	*base.Reconciler
	*config.Env
	ConfigMapLister     corelisters.ConfigMapLister
	ConsumerGroupLister internalslst.ConsumerGroupLister
	InternalsClient     internalsclient.Interface
}

func (r *Reconciler) ReconcileKind(ctx context.Context, channel *messagingv1beta1.KafkaChannel) reconciler.Event {

	logger := kafkalogging.CreateReconcileMethodLogger(ctx, channel)

	topicName := kafka.ChannelTopic(TopicPrefix, channel)

	// Get the channel configmap
	channelConfigMap, err := r.channelConfigMap()
	if err != nil {
		return err
	}
	logger.Debug("configmap read", zap.Any("configmap", channelConfigMap))

	bootstrapServers, err := kafka.BootstrapServersFromConfigMap(logger, channelConfigMap)
	if err != nil {
		return fmt.Errorf("unable to get bootstrapServers from configmap: %w - ConfigMap data: %v", err, channelConfigMap.Data)
	}

	// Get contract configmap
	cm, err := r.GetOrCreateDataPlaneConfigMap(ctx)
	if err != nil {
		return fmt.Errorf("failed to get or create data plane ConfigMap: %w", err)
	}
	logger.Debug("Got contract config map")

	// Get contract data
	ct, err := r.GetDataPlaneConfigMapData(logger, cm)
	if err != nil && ct == nil {
		return fmt.Errorf("failed to get contract data from ConfigMap: %w", err)
	}
	if ct == nil {
		ct = &contract.Contract{}
	}
	logger.Debug("Got contract data from config map", zap.Any(base.ContractLogKey, ct))

	// Get channel configuration
	channelConfig := &contract.Resource{
		Uid:    string(channel.UID),
		Topics: []string{topicName},
		Ingress: &contract.Ingress{
			IngressType: &contract.Ingress_Path{
				Path: receiver.PathFromObject(channel),
			},
		},
		BootstrapServers: strings.Join(bootstrapServers, ","),
		Reference: &contract.Reference{
			Uuid:      string(channel.GetUID()),
			Namespace: channel.GetNamespace(),
			Name:      channel.GetName(),
		},
	}

	err = r.reconcileConsumerGroup(ctx, channel, topicName, bootstrapServers)
	if err != nil {
		channel.GetConditionSet().Manage(&channel.Status).MarkFalse(KafkaChannelConditionConsumerGroup, "failed to reconcile consumer group", err.Error())
		return err
	}
	channel.GetConditionSet().Manage(&channel.Status).MarkTrue(KafkaChannelConditionConsumerGroup)

	// Update contract data with the new contract configuration (add/update channel resource)
	channelIndex := coreconfig.FindResource(ct, channel.UID)
	changed := coreconfig.AddOrUpdateResourceConfig(ct, channelConfig, channelIndex, logger)
	logger.Debug("Change detector", zap.Int("changed", changed))

	if changed == coreconfig.ResourceChanged {
		logger.Debug("Contract changed", zap.Int("changed", changed))

		//ct.IncrementGeneration()  //From PR#1601

		// Update the configuration map with the new contract data.
		if err := r.UpdateDataPlaneConfigMap(ctx, ct, cm); err != nil {
			channel.GetConditionSet().Manage(&channel.Status).MarkFalse(KafkaChannelConditionConfigMapUpdated, "failed to update ConfigMap", err.Error())
			return err
		}
		logger.Debug("Contract config map updated")
	}
	channel.GetConditionSet().Manage(&channel.Status).MarkTrue(KafkaChannelConditionConfigMapUpdated)

	return nil
}

func (r Reconciler) reconcileConsumerGroup(ctx context.Context, channel *messagingv1beta1.KafkaChannel, topicName string, bootstrapServers []string) error {

	var globalErr error

	for i := range channel.Spec.Subscribers {
		s := &channel.Spec.Subscribers[i]

		newcg := &internalscg.ConsumerGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      string(s.UID),
				Namespace: channel.Namespace,
				OwnerReferences: []metav1.OwnerReference{
					*kmeta.NewControllerRef(channel),
				},
			},
			Spec: internalscg.ConsumerGroupSpec{
				Template: internalscg.ConsumerTemplateSpec{
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
					},
				},
			},
		}

		cg, err := r.ConsumerGroupLister.ConsumerGroups(channel.GetNamespace()).Get(string(s.UID)) //Get by consumer group id
		if err != nil && !apierrors.IsNotFound(err) {
			channel.Status.Subscribers = append(channel.Status.Subscribers, subscriberStatus(s, err))
			globalErr = multierr.Append(globalErr, err)
			continue
		}

		if apierrors.IsNotFound(err) {
			_, err = r.InternalsClient.InternalV1alpha1().ConsumerGroups(newcg.GetNamespace()).Create(ctx, newcg, metav1.CreateOptions{})
			if err != nil && !apierrors.IsAlreadyExists(err) {
				channel.Status.Subscribers = append(channel.Status.Subscribers, subscriberStatus(s, err))
				globalErr = multierr.Append(globalErr, fmt.Errorf("failed to create consumer group %s/%s: %w", newcg.GetNamespace(), newcg.GetName(), err))
			} else {
				channel.Status.Subscribers = append(channel.Status.Subscribers, subscriberStatus(s, nil))
			}
			continue
		}

		if !equality.Semantic.DeepDerivative(newcg.Spec, cg.Spec) {
			newCg := &internalscg.ConsumerGroup{
				TypeMeta:   cg.TypeMeta,
				ObjectMeta: cg.ObjectMeta,
				Spec:       newcg.Spec,
				Status:     cg.Status,
			}
			if _, err = r.InternalsClient.InternalV1alpha1().ConsumerGroups(cg.GetNamespace()).Update(ctx, newCg, metav1.UpdateOptions{}); err != nil {
				channel.Status.Subscribers = append(channel.Status.Subscribers, subscriberStatus(s, err))
				globalErr = multierr.Append(globalErr, fmt.Errorf("failed to update consumer group %s/%s: %w", newCg.GetNamespace(), newCg.GetName(), err))
			} else {
				channel.Status.Subscribers = append(channel.Status.Subscribers, subscriberStatus(s, nil))
			}
		}
	}

	return globalErr
}

func subscriberStatus(s *v1.SubscriberSpec, err error) v1.SubscriberStatus {
	if err != nil {
		return v1.SubscriberStatus{
			UID:                s.UID,
			ObservedGeneration: s.Generation,
			Ready:              corev1.ConditionFalse,
			Message:            fmt.Sprint("Subscription not ready", err),
		}
	}

	return v1.SubscriberStatus{
		UID:                s.UID,
		ObservedGeneration: s.Generation,
		Ready:              corev1.ConditionTrue,
	}
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
