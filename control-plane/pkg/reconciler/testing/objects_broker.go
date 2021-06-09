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

package testing

import (
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgotesting "k8s.io/client-go/testing"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1"
	reconcilertesting "knative.dev/eventing/pkg/reconciler/testing/v1"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/network"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	. "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/broker"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/security"
)

const (
	BrokerUUID      = "e7185016-5d98-4b54-84e8-3b1cd4acc6b4"
	BrokerNamespace = "test-namespace"
	BrokerName      = "test-broker"

	ReceiverIP = "127.0.0.1"
)

func BrokerTopic() string {
	broker := NewBroker().(metav1.Object)
	return kafka.Topic(TopicPrefix, broker)
}

// NewBroker creates a new Broker with broker class equals to kafka.BrokerClass.
func NewBroker(options ...reconcilertesting.BrokerOption) runtime.Object {
	return reconcilertesting.NewBroker(
		BrokerName,
		BrokerNamespace,
		append(
			[]reconcilertesting.BrokerOption{
				reconcilertesting.WithBrokerClass(kafka.BrokerClass),
				func(broker *eventing.Broker) {
					broker.UID = BrokerUUID
				},
			},
			options...,
		)...,
	)
}

func NewDeletedBroker(options ...reconcilertesting.BrokerOption) runtime.Object {
	return NewBroker(
		append(
			options,
			func(broker *eventing.Broker) {
				broker.DeletionTimestamp = &metav1.Time{Time: time.Now()}
			},
		)...,
	)
}

func WithDelivery() func(*eventing.Broker) {
	service := NewService()

	return func(broker *eventing.Broker) {
		if broker.Spec.Delivery == nil {
			broker.Spec.Delivery = &eventingduck.DeliverySpec{}
		}
		broker.Spec.Delivery.DeadLetterSink = &duckv1.Destination{
			Ref: &duckv1.KReference{
				Kind:       service.Kind,
				Namespace:  service.Namespace,
				Name:       service.Name,
				APIVersion: service.APIVersion,
			},
		}
	}
}

func WithRetry(retry *int32, policy *eventingduck.BackoffPolicyType, delay *string) func(*eventing.Broker) {
	return func(broker *eventing.Broker) {
		if broker.Spec.Delivery == nil {
			broker.Spec.Delivery = &eventingduck.DeliverySpec{}
		}
		broker.Spec.Delivery.Retry = retry
		broker.Spec.Delivery.BackoffPolicy = policy
		broker.Spec.Delivery.BackoffDelay = delay
	}
}

func WithBrokerConfig(reference *duckv1.KReference) func(*eventing.Broker) {
	return func(broker *eventing.Broker) {
		broker.Spec.Config = reference
	}
}

type CMOption func(cm *corev1.ConfigMap)

func BrokerConfig(bootstrapServers string, numPartitions, replicationFactor int, options ...CMOption) *corev1.ConfigMap {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ConfigMapNamespace,
			Name:      ConfigMapName,
		},
		Data: map[string]string{
			BootstrapServersConfigMapKey:              bootstrapServers,
			DefaultTopicReplicationFactorConfigMapKey: fmt.Sprintf("%d", replicationFactor),
			DefaultTopicNumPartitionConfigMapKey:      fmt.Sprintf("%d", numPartitions),
		},
	}
	for _, opt := range options {
		opt(cm)
	}
	return cm
}

func BrokerAuthConfig(name string) CMOption {
	return func(cm *corev1.ConfigMap) {
		if cm.Data == nil {
			cm.Data = make(map[string]string, 1)
		}
		cm.Data[security.AuthSecretNameKey] = name
	}
}

func KReference(configMap *corev1.ConfigMap) *duckv1.KReference {
	return &duckv1.KReference{
		Kind:       "ConfigMap",
		Namespace:  configMap.Namespace,
		Name:       configMap.Name,
		APIVersion: configMap.APIVersion,
	}
}

func BrokerReady(broker *eventing.Broker) {
	broker.Status.Conditions = duckv1.Conditions{
		{
			Type:   apis.ConditionReady,
			Status: corev1.ConditionTrue,
		},
	}
}

func BrokerConfigMapUpdatedReady(configs *Configs) func(broker *eventing.Broker) {
	return func(broker *eventing.Broker) {
		broker.GetConditionSet().Manage(broker.GetStatus()).MarkTrueWithReason(
			base.ConditionConfigMapUpdated,
			fmt.Sprintf("Config map %s updated", configs.DataPlaneConfigMapAsString()),
			"",
		)
	}
}

func BrokerTopicReady(broker *eventing.Broker) {
	broker.GetConditionSet().Manage(broker.GetStatus()).MarkTrueWithReason(
		base.ConditionTopicReady,
		fmt.Sprintf("Topic %s created", kafka.Topic(TopicPrefix, broker)),
		"",
	)
}

func BrokerDataPlaneAvailable(broker *eventing.Broker) {
	broker.GetConditionSet().Manage(broker.GetStatus()).MarkTrue(base.ConditionDataPlaneAvailable)
}

func BrokerDataPlaneNotAvailable(broker *eventing.Broker) {
	broker.GetConditionSet().Manage(broker.GetStatus()).MarkFalse(
		base.ConditionDataPlaneAvailable,
		base.ReasonDataPlaneNotAvailable,
		base.MessageDataPlaneNotAvailable,
	)
}

func BrokerConfigParsed(broker *eventing.Broker) {
	broker.GetConditionSet().Manage(broker.GetStatus()).MarkTrue(base.ConditionConfigParsed)
}

func BrokerConfigNotParsed(reason string) func(broker *eventing.Broker) {
	return func(broker *eventing.Broker) {
		broker.GetConditionSet().Manage(broker.GetStatus()).MarkFalse(base.ConditionConfigParsed, reason, "")
	}
}

func BrokerProbeSucceeded(broker *eventing.Broker) {
	broker.GetConditionSet().Manage(broker.GetStatus()).MarkTrue(base.ConditionProbeSucceeded)
}

func BrokerProbeFailed(statusCode int) func(broker *eventing.Broker) {
	return func(broker *eventing.Broker) {
		broker.GetConditionSet().Manage(broker.GetStatus()).MarkFalse(
			base.ConditionProbeSucceeded,
			base.ReasonProbeFailed,
			fmt.Sprintf("probe failed response status code %d from pod knative-eventing/kafka-broker-receiver with ip %s", statusCode, ReceiverIP),
		)
	}
}

func BrokerAddressable(configs *Configs) func(broker *eventing.Broker) {

	return func(broker *eventing.Broker) {

		broker.Status.Address.URL = &apis.URL{
			Scheme: "http",
			Host:   network.GetServiceHostname(configs.IngressName, configs.SystemNamespace),
			Path:   fmt.Sprintf("/%s/%s", broker.Namespace, broker.Name),
		}

		broker.GetConditionSet().Manage(&broker.Status).MarkTrue(base.ConditionAddressable)
	}
}

func BrokerFailedToCreateTopic(broker *eventing.Broker) {

	broker.GetConditionSet().Manage(broker.GetStatus()).MarkFalse(
		base.ConditionTopicReady,
		fmt.Sprintf("Failed to create topic: %s", BrokerTopic()),
		"%v",
		fmt.Errorf("failed to create topic"),
	)

}

func BrokerFailedToGetConfigMap(configs *Configs) func(broker *eventing.Broker) {

	return func(broker *eventing.Broker) {

		broker.GetConditionSet().Manage(broker.GetStatus()).MarkFalse(
			base.ConditionConfigMapUpdated,
			fmt.Sprintf(
				"Failed to get ConfigMap: %s",
				configs.DataPlaneConfigMapAsString(),
			),
			`configmaps "knative-eventing" not found`,
		)
	}

}

func BrokerDispatcherPod(namespace string, annotations map[string]string) runtime.Object {
	return &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        "kafka-broker-dispatcher",
			Namespace:   namespace,
			Annotations: annotations,
			Labels: map[string]string{
				"app": base.BrokerDispatcherLabel,
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}
}

func BrokerReceiverPod(namespace string, annotations map[string]string) runtime.Object {
	return &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        "kafka-broker-receiver",
			Namespace:   namespace,
			Annotations: annotations,
			Labels: map[string]string{
				"app": base.BrokerReceiverLabel,
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: ReceiverIP,
		},
	}
}

func BrokerDispatcherPodUpdate(namespace string, annotations map[string]string) clientgotesting.UpdateActionImpl {
	return clientgotesting.NewUpdateAction(
		schema.GroupVersionResource{
			Group:    "*",
			Version:  "v1",
			Resource: "Pod",
		},
		namespace,
		BrokerDispatcherPod(namespace, annotations),
	)
}

func BrokerReceiverPodUpdate(namespace string, annotations map[string]string) clientgotesting.UpdateActionImpl {
	return clientgotesting.NewUpdateAction(
		schema.GroupVersionResource{
			Group:    "*",
			Version:  "v1",
			Resource: "Pod",
		},
		namespace,
		BrokerReceiverPod(namespace, annotations),
	)
}
