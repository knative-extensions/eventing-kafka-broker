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
	"bytes"
	"fmt"
	"strings"
	"text/template"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgotesting "k8s.io/client-go/testing"
	"k8s.io/utils/pointer"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1"
	reconcilertesting "knative.dev/eventing/pkg/reconciler/testing/v1"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/network"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/prober"

	apisconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	. "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/broker"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/security"
)

const (
	BrokerUUID          = "e7185016-5d98-4b54-84e8-3b1cd4acc6b4"
	BrokerNamespaceUUID = "d1234567-8910-1234-5678-901234567890"
	BrokerNamespace     = "test-namespace"
	BrokerName          = "test-broker"
	ExternalTopicName   = "test-topic"

	SecretFinalizerName = "kafka.eventing/" + BrokerUUID

	TriggerName      = "test-trigger"
	TriggerNamespace = "test-namespace"
)

var (
	kafkaFeatureFlags = apisconfig.DefaultFeaturesConfig()
	BrokerTopics      = []string{getKafkaTopic()}
)

func BrokerTopic() string {
	return getKafkaTopic()
}

func CustomBrokerTopic(template *template.Template) string {
	var result bytes.Buffer
	err := template.Execute(&result, metav1.ObjectMeta{Namespace: BrokerNamespace, Name: BrokerName, UID: BrokerUUID})
	if err != nil {
		panic("Failed to create custom topic name")
	}
	return result.String()
}

// NewBroker creates a new Broker with broker class equals to kafka.BrokerClass.
func NewBroker(options ...reconcilertesting.BrokerOption) runtime.Object {
	return doNewBroker(kafka.BrokerClass, options...)
}

// NewNamespacedBroker creates a new Broker with broker class equals to kafka.NamespacedBrokerClass.
func NewNamespacedBroker(options ...reconcilertesting.BrokerOption) runtime.Object {
	return doNewBroker(kafka.NamespacedBrokerClass, options...)
}

func doNewBroker(class string, options ...reconcilertesting.BrokerOption) runtime.Object {
	return reconcilertesting.NewBroker(
		BrokerName,
		BrokerNamespace,
		append(
			[]reconcilertesting.BrokerOption{
				reconcilertesting.WithBrokerClass(class),
				WithBrokerConfig(
					KReference(BrokerConfig("", 20, 5)),
				),
				func(broker *eventing.Broker) {
					broker.UID = BrokerUUID
				},
			},
			options...,
		)...,
	)
}

func BrokerSecretWithFinalizer(ns, name, finalizerName string) *corev1.Secret {
	secret := NewSSLSecret(ns, name)
	secret.Finalizers = append(secret.Finalizers, finalizerName)
	return secret
}

func NewDeletedBroker(options ...reconcilertesting.BrokerOption) runtime.Object {
	return NewBroker(
		append(
			options,
			func(broker *eventing.Broker) {
				WithDeletedTimeStamp(broker)
			},
			BrokerConfigMapAnnotations(),
		)...,
	)
}

func NewDeletedBrokerWithoutConfigMapAnnotations(options ...reconcilertesting.BrokerOption) runtime.Object {
	return NewBroker(
		append(
			options,
			func(broker *eventing.Broker) {
				WithDeletedTimeStamp(broker)
			},
		)...,
	)
}

func WithDelivery(mutations ...func(spec *eventingduck.DeliverySpec)) func(*eventing.Broker) {
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
		for _, mut := range mutations {
			mut(broker.Spec.Delivery)
		}
	}
}

func WithNoDeadLetterSinkNamespace(spec *eventingduck.DeliverySpec) {
	if spec.DeadLetterSink != nil && spec.DeadLetterSink.Ref != nil {
		spec.DeadLetterSink.Ref.Namespace = ""
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

func WithBrokerAddress(address duckv1.Addressable) reconcilertesting.BrokerOption {
	return func(broker *eventing.Broker) {
		broker.Status.Address = &address
	}
}

func WithBrokerAddresses(addresses []duckv1.Addressable) reconcilertesting.BrokerOption {
	return func(broker *eventing.Broker) {
		broker.Status.Addresses = addresses
	}
}

func WithBrokerAddessable() reconcilertesting.BrokerOption {
	return func(broker *eventing.Broker) {
		broker.GetConditionSet().Manage(broker.GetStatus()).MarkTrue(eventing.BrokerConditionAddressable)
	}
}

func WithExternalTopic(topic string) func(*eventing.Broker) {
	return func(broker *eventing.Broker) {
		annotations := broker.GetAnnotations()
		if annotations == nil {
			annotations = make(map[string]string, 1)
		}
		annotations[ExternalTopicAnnotation] = topic
		broker.SetAnnotations(annotations)
		WithTopicStatusAnnotation(topic)(broker)
	}
}

func WithTopicStatusAnnotation(topic string) reconcilertesting.BrokerOption {
	return func(broker *eventing.Broker) {
		if broker.Status.Annotations == nil {
			broker.Status.Annotations = make(map[string]string, 1)
		}
		broker.Status.Annotations[kafka.TopicAnnotation] = topic
	}
}

func WithBootstrapServerStatusAnnotation(servers string) reconcilertesting.BrokerOption {
	return func(broker *eventing.Broker) {
		if broker.Status.Annotations == nil {
			broker.Status.Annotations = make(map[string]string, 1)
		}
		broker.Status.Annotations[kafka.BootstrapServersConfigMapKey] = servers
	}
}

func WithSecretStatusAnnotation(name string) reconcilertesting.BrokerOption {
	return func(broker *eventing.Broker) {
		if broker.Status.Annotations == nil {
			broker.Status.Annotations = make(map[string]string, 10)
		}
		broker.Status.Annotations[security.AuthSecretNameKey] = name
	}
}

func WithAutoscalingAnnotationsTrigger() reconcilertesting.TriggerOption {
	return func(trigger *eventing.Trigger) {
		if trigger.Annotations == nil {
			trigger.Annotations = make(map[string]string)
		}

		for k, v := range ConsumerGroupAnnotations {
			if _, ok := trigger.Annotations[k]; !ok {
				trigger.Annotations[k] = v
			}
		}
	}
}

type CMOption func(cm *corev1.ConfigMap)

func WithConfigMapNamespace(namespace string) CMOption {
	return func(cm *corev1.ConfigMap) {
		cm.ObjectMeta.Namespace = namespace
	}
}

func BrokerConfig(bootstrapServers string, numPartitions, replicationFactor int, options ...CMOption) *corev1.ConfigMap {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ConfigMapNamespace,
			Name:      ConfigMapName,
		},
		Data: map[string]string{
			kafka.BootstrapServersConfigMapKey:              bootstrapServers,
			kafka.DefaultTopicReplicationFactorConfigMapKey: fmt.Sprintf("%d", replicationFactor),
			kafka.DefaultTopicNumPartitionConfigMapKey:      fmt.Sprintf("%d", numPartitions),
		},
	}
	for _, opt := range options {
		opt(cm)
	}
	return cm
}

func BogusBrokerConfig() *corev1.ConfigMap {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ConfigMapNamespace,
			Name:      ConfigMapName,
		},
		Data: map[string]string{
			"foo": "bar",
		},
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

func StatusBrokerConfigMapUpdatedReady(env *config.Env) func(broker *eventing.Broker) {
	return func(broker *eventing.Broker) {
		StatusConfigMapUpdatedReady(env)(broker)
	}
}

func StatusBrokerTopicReady(broker *eventing.Broker) {
	topicName, err := kafkaFeatureFlags.ExecuteBrokersTopicTemplate(broker.ObjectMeta)
	if err != nil {
		panic("Failed to create broker topic name")
	}
	StatusTopicReadyWithName(topicName)(broker)
}

func StatusExternalBrokerTopicReady(topic string) func(broker *eventing.Broker) {
	return func(broker *eventing.Broker) {
		StatusTopicReadyWithName(topic)(broker)
	}
}

func StatusBrokerDataPlaneAvailable(broker *eventing.Broker) {
	StatusDataPlaneAvailable(broker)
}

func StatusBrokerDataPlaneNotAvailable(broker *eventing.Broker) {
	StatusDataPlaneNotAvailable(broker)
}

func StatusBrokerConfigParsed(broker *eventing.Broker) {
	StatusConfigParsed(broker)
}

func StatusBrokerConfigNotParsed(reason string) func(broker *eventing.Broker) {
	return func(broker *eventing.Broker) {
		StatusConfigNotParsed(reason)(broker)
	}
}

func BrokerAddressable(env *config.Env) func(broker *eventing.Broker) {
	return func(broker *eventing.Broker) {
		brokerAddressable(broker, env.IngressName, env.SystemNamespace)
	}
}

func NamespacedBrokerAddressable(env *config.Env) func(broker *eventing.Broker) {
	return func(broker *eventing.Broker) {
		brokerAddressable(broker, env.IngressName, broker.Namespace)
	}
}

func brokerAddressable(broker *eventing.Broker, serviceName, serviceNamespace string) {
	broker.Status.Address = &duckv1.Addressable{
		Name: pointer.String("http"),
		URL: &apis.URL{
			Scheme: "http",
			Host:   network.GetServiceHostname(serviceName, serviceNamespace),
			Path:   fmt.Sprintf("/%s/%s", broker.Namespace, broker.Name),
		},
	}

	broker.GetConditionSet().Manage(&broker.Status).MarkTrue(base.ConditionAddressable)
}

func BrokerReference() *contract.Reference {
	return &contract.Reference{
		Uuid:         BrokerUUID,
		Namespace:    BrokerNamespace,
		Name:         BrokerName,
		Kind:         "Broker",
		GroupVersion: eventing.SchemeGroupVersion.String(),
	}
}

func TriggerReference() *contract.Reference {
	return &contract.Reference{
		Uuid:      TriggerUUID,
		Namespace: TriggerNamespace,
		Name:      TriggerName,
	}
}

func BrokerDLSResolved(uri string) func(broker *eventing.Broker) {
	return func(broker *eventing.Broker) {
		broker.Status.DeadLetterSinkURI, _ = apis.ParseURL(uri)
	}
}

func StatusBrokerFailedToCreateTopic(broker *eventing.Broker) {
	StatusFailedToCreateTopic(BrokerTopic())(broker)
}

func StatusExternalBrokerTopicNotPresentOrInvalid(topicname string) func(broker *eventing.Broker) {
	return func(broker *eventing.Broker) {
		StatusTopicNotPresentOrInvalid(topicname)(broker)
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
				"app":                    base.BrokerDispatcherLabel,
				"app.kubernetes.io/kind": "kafka-dispatcher",
			},
		},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
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
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
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

func StatusBrokerProbeSucceeded(broker *eventing.Broker) {
	StatusProbeSucceeded(broker)
}

func StatusBrokerProbeFailed(status prober.Status) reconcilertesting.BrokerOption {
	return func(broker *eventing.Broker) {
		StatusProbeFailed(status)(broker)
	}
}

func BrokerConfigMapAnnotations() reconcilertesting.BrokerOption {
	return func(broker *eventing.Broker) {
		if broker.Status.Annotations == nil {
			broker.Status.Annotations = make(map[string]string, 10)
		}
		broker.Status.Annotations[kafka.BootstrapServersConfigMapKey] = strings.Join(bootstrapServers, ",")
		broker.Status.Annotations[kafka.DefaultTopicNumPartitionConfigMapKey] = fmt.Sprintf("%d", DefaultNumPartitions)
		broker.Status.Annotations[kafka.DefaultTopicReplicationFactorConfigMapKey] = fmt.Sprintf("%d", DefaultReplicationFactor)
	}
}

func BrokerConfigMapSecretAnnotation(name string) reconcilertesting.BrokerOption {
	return func(broker *eventing.Broker) {
		if broker.Status.Annotations == nil {
			broker.Status.Annotations = make(map[string]string, 10)
		}
		broker.Status.Annotations[security.AuthSecretNameKey] = name
	}
}

func getKafkaTopic() string {
	topicName, err := kafkaFeatureFlags.ExecuteBrokersTopicTemplate(metav1.ObjectMeta{Namespace: BrokerNamespace, Name: BrokerName})
	if err != nil {
		panic("Failed to create broker topic name")
	}
	return topicName
}
