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
	"encoding/json"
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgotesting "k8s.io/client-go/testing"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1beta1"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1beta1"
	"knative.dev/eventing/pkg/reconciler/names"
	reconcilertesting "knative.dev/eventing/pkg/reconciler/testing/v1beta1"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"

	coreconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/core/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	. "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/broker"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka"
)

const (
	BrokerNamespace    = "test-namespace"
	BrokerName         = "test-broker"
	ConfigMapNamespace = "test-namespace-config-map"
	ConfigMapName      = "test-config-cm"

	serviceNamespace = "test-service-namespace"
	serviceName      = "test-service"
	ServiceURL       = "http://test-service.test-service-namespace.svc.cluster.local/"

	BrokerUUID  = "e7185016-5d98-4b54-84e8-3b1cd4acc6b4"
	TriggerUUID = "e7185016-5d98-4b54-84e8-3b1cd4acc6b5"
)

var (
	Formats = []string{base.Protobuf, base.Json}
)

func GetTopic() string {
	return fmt.Sprintf("%s%s-%s", TopicPrefix, BrokerNamespace, BrokerName)
}

func NewDispatcherPod(namespace string, annotations map[string]string) runtime.Object {
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
				"app": base.DispatcherLabel,
			},
		},
	}
}

func NewReceiverPod(namespace string, annotations map[string]string) runtime.Object {
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
				"app": base.ReceiverLabel,
			},
		},
	}
}

func DispatcherPodUpdate(namespace string, annotations map[string]string) clientgotesting.UpdateActionImpl {
	return clientgotesting.NewUpdateAction(
		schema.GroupVersionResource{
			Group:    "*",
			Version:  "v1",
			Resource: "Pod",
		},
		namespace,
		NewDispatcherPod(namespace, annotations),
	)
}

func ReceiverPodUpdate(namespace string, annotations map[string]string) clientgotesting.UpdateActionImpl {
	return clientgotesting.NewUpdateAction(
		schema.GroupVersionResource{
			Group:    "*",
			Version:  "v1",
			Resource: "Pod",
		},
		namespace,
		NewReceiverPod(namespace, annotations),
	)
}

func NewService() *corev1.Service {
	return reconcilertesting.NewService(
		serviceName,
		serviceNamespace,
		func(service *corev1.Service) {
			service.APIVersion = "v1"
			service.Kind = "Service"
		},
	)
}

func NewConfigMap(configs *Configs, data []byte) runtime.Object {
	return reconcilertesting.NewConfigMap(
		configs.DataPlaneConfigMapName,
		configs.DataPlaneConfigMapNamespace,
		func(configMap *corev1.ConfigMap) {
			if configMap.BinaryData == nil {
				configMap.BinaryData = make(map[string][]byte, 1)
			}
			if data == nil {
				data = []byte("")
			}
			configMap.BinaryData[base.ConfigMapDataKey] = data
		},
	)
}

func NewConfigMapFromBrokers(brokers *coreconfig.Brokers, configs *Configs) runtime.Object {
	var data []byte
	var err error
	if configs.DataPlaneConfigFormat == base.Protobuf {
		data, err = proto.Marshal(brokers)
	} else {
		data, err = json.Marshal(brokers)
	}
	if err != nil {
		panic(err)
	}

	return NewConfigMap(configs, data)
}

func ConfigMapUpdate(configs *Configs, brokers *coreconfig.Brokers) clientgotesting.UpdateActionImpl {
	return clientgotesting.NewUpdateAction(
		schema.GroupVersionResource{
			Group:    "*",
			Version:  "v1",
			Resource: "ConfigMap",
		},
		configs.DataPlaneConfigMapNamespace,
		NewConfigMapFromBrokers(brokers, configs),
	)
}

// NewBroker creates a new Broker with broker class equals to kafka.BrokerClass
func NewBroker(options ...reconcilertesting.BrokerOption) runtime.Object {
	return reconcilertesting.NewBroker(
		BrokerName,
		BrokerNamespace,
		append(
			options,
			reconcilertesting.WithBrokerClass(kafka.BrokerClass),
			func(broker *eventing.Broker) {
				broker.UID = BrokerUUID
			},
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
		broker.Spec.Delivery = &eventingduck.DeliverySpec{
			DeadLetterSink: &duckv1.Destination{
				Ref: &duckv1.KReference{
					Kind:       service.Kind,
					Namespace:  service.Namespace,
					Name:       service.Name,
					APIVersion: service.APIVersion,
				},
			},
		}
	}
}

func WithBrokerConfig(reference *duckv1.KReference) func(*eventing.Broker) {
	return func(broker *eventing.Broker) {
		broker.Spec.Config = reference
	}
}

func BrokerConfig(bootstrapServers string, numPartitions, replicationFactor int) *corev1.ConfigMap {
	return &corev1.ConfigMap{
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

func ConfigMapUpdatedReady(configs *Configs) func(broker *eventing.Broker) {
	return func(broker *eventing.Broker) {
		broker.GetConditionSet().Manage(broker.GetStatus()).MarkTrueWithReason(
			ConditionConfigMapUpdated,
			fmt.Sprintf("Config map %s updated", configs.DataPlaneConfigMapAsString()),
			"",
		)
	}
}

func TopicReady(broker *eventing.Broker) {
	broker.GetConditionSet().Manage(broker.GetStatus()).MarkTrueWithReason(
		ConditionTopicReady,
		fmt.Sprintf("Topic %s created", Topic(broker)),
		"",
	)
}

func ConfigParsed(broker *eventing.Broker) {
	broker.GetConditionSet().Manage(broker.GetStatus()).MarkTrue(ConditionConfigParsed)
}

func ConfigNotParsed(reason string) func(broker *eventing.Broker) {
	return func(broker *eventing.Broker) {
		broker.GetConditionSet().Manage(broker.GetStatus()).MarkFalse(ConditionConfigParsed, reason, "")
	}
}

func Addressable(configs *Configs) func(broker *eventing.Broker) {

	return func(broker *eventing.Broker) {

		broker.Status.Address.URL = &apis.URL{
			Scheme: "http",
			Host:   names.ServiceHostName(configs.BrokerIngressName, configs.SystemNamespace),
			Path:   fmt.Sprintf("/%s/%s", broker.Namespace, broker.Name),
		}

		broker.GetConditionSet().Manage(&broker.Status).MarkTrue(ConditionAddressable)
	}
}

func FailedToCreateTopic(broker *eventing.Broker) {

	broker.GetConditionSet().Manage(broker.GetStatus()).MarkFalse(
		ConditionTopicReady,
		fmt.Sprintf("Failed to create topic: %s", GetTopic()),
		"%v",
		fmt.Errorf("failed to create topic"),
	)

}

func FailedToGetConfigMap(configs *Configs) func(broker *eventing.Broker) {

	return func(broker *eventing.Broker) {

		broker.GetConditionSet().Manage(broker.GetStatus()).MarkFalse(
			ConditionConfigMapUpdated,
			fmt.Sprintf(
				"Failed to get ConfigMap: %s",
				configs.DataPlaneConfigMapAsString(),
			),
			`configmaps "knative-eventing" not found`,
		)
	}

}
