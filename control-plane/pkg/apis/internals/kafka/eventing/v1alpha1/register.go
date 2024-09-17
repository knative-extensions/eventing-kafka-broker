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

package v1alpha1

import (
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	kafkaeventing "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing"
)

// SchemeGroupVersion is group version used to register these objects.
var SchemeGroupVersion = schema.GroupVersion{Group: kafkaeventing.GroupName, Version: "v1alpha1"}

var ConsumerGroupGroupVersionKind = schema.GroupVersionKind{
	Group:   SchemeGroupVersion.Group,
	Version: SchemeGroupVersion.Version,
	Kind:    "ConsumerGroup",
}

var ConsumerGroupVersionKind = schema.GroupVersionKind{
	Group:   SchemeGroupVersion.Group,
	Version: SchemeGroupVersion.Version,
	Kind:    "Consumer",
}

// Kind takes an unqualified kind and returns back a Group qualified GroupKind.
func Kind(kind string) schema.GroupKind {
	return SchemeGroupVersion.WithKind(kind).GroupKind()
}

// Resource takes an unqualified resource and returns a Group qualified GroupResource.
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

var (
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	AddToScheme   = SchemeBuilder.AddToScheme
)

// Adds the list of known types to Scheme.
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&ConsumerGroup{},
		&Consumer{},
	)
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}

const (
	SourceStatefulSetName  = "kafka-source-dispatcher"
	ChannelStatefulSetName = "kafka-channel-dispatcher"
	BrokerStatefulSetName  = "kafka-broker-dispatcher"
)

func IsKnownStatefulSet(name string) bool {
	return SourceStatefulSetName == name ||
		name == ChannelStatefulSetName ||
		name == BrokerStatefulSetName
}

func GetOwnerKindFromStatefulSetPrefix(name string) (string, bool) {
	if strings.HasPrefix(name, SourceStatefulSetName) {
		return "KafkaSource", true
	}
	if strings.HasPrefix(name, ChannelStatefulSetName) {
		return "KafkaChannel", true
	}
	if strings.HasPrefix(name, BrokerStatefulSetName) {
		return "Trigger", true
	}
	return "", false
}
