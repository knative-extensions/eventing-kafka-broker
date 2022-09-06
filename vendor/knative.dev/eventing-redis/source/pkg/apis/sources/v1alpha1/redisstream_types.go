/*
Copyright 2020 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/kmeta"
)

// +genclient
// +genreconciler
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:defaulter-gen=true

// RedisStreamSource is the Schema for the RedisStream API.
type RedisStreamSource struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RedisStreamSourceSpec   `json:"spec,omitempty"`
	Status RedisStreamSourceStatus `json:"status,omitempty"`
}

// Check the interfaces that RedisStreamSource should be implementing.
var (
	_ runtime.Object     = (*RedisStreamSource)(nil)
	_ kmeta.OwnerRefable = (*RedisStreamSource)(nil)
	//_ apis.Validatable   = (*RedisStreamSource)(nil)
	//_ apis.Defaultable   = (*RedisStreamSource)(nil)
	_ apis.HasSpec    = (*RedisStreamSource)(nil)
	_ duckv1.KRShaped = (*RedisStreamSource)(nil)
)

// RedisStreamSourceSpec defines the desired state of the RedisStreamSource.
type RedisStreamSourceSpec struct {
	// inherits duck/v1 SourceSpec, which currently provides:
	// * Sink - a reference to an object that will resolve to a domain name or
	//   a URI directly to use as the sink.
	// * CloudEventOverrides - defines overrides to control the output format
	//   and modifications of the event sent to the sink.
	duckv1.SourceSpec `json:",inline"`

	// RedisConnection represents the address and options to connect
	// to a Redis instance
	RedisConnection `json:",inline"`

	// Stream is the name of the stream.
	Stream string `json:"stream"`

	// Group is the name of the consumer group associated to this source.
	// When left empty, a group is automatically created for this source and
	// deleted when this source is deleted.
	// +optional
	Group string `json:"group,omitempty"`

	// Number of desired consumers running in the consumer group. Defaults to 1.
	//
	// This is a pointer to distinguish between explicit
	// zero and not specified.
	// +optional
	Consumers *int32 `json:"consumers,omitempty"`
}

// RedisConnection defines the address and options to connect to a Redis instance
type RedisConnection struct {
	// Address is the Redis TCP address
	Address string `json:"address"`

	// Options are the connection options
	// +optional
	Options *RedisConnectionOptions `json:"dialOptions,omitempty"`
}

// RedisConnection defines the desired state of the RedisStreamSource.
type RedisConnectionOptions struct {
	// Password to use for connecting to Redis
	// +optional
	Password corev1.ObjectReference `json:"password,omitempty"`

	// UseTLS indicates whether to use TLS or not
	// +optional
	UseTLS bool `json:"useTLS,omitempty"`

	// SkipVerify indicates whether to skip TLS verification or not
	// +optional
	SkipVerify bool `json:"skipVerify,omitempty"`

	// Cert is the Kubernetes secret containing the client certificate.
	// +optional
	Cert RedisSecretValueFromSource `json:"cert,omitempty"`

	// Key is the Kubernetes secret containing the client key.
	// +optional
	Key RedisSecretValueFromSource `json:"key,omitempty"`

	// CACert is the Kubernetes secret containing the server CA cert.
	// +optional
	CACert RedisSecretValueFromSource `json:"caCert,omitempty"`
}

// RedisSecretValueFromSource represents the source of a secret value
type RedisSecretValueFromSource struct {
	// The Secret key to select from.
	SecretKeyRef *corev1.SecretKeySelector `json:"secretKeyRef,omitempty"`
}

// RedisStreamSourceStatus defines the observed state of RedisStreamSource.
type RedisStreamSourceStatus struct {
	// inherits duck/v1 SourceStatus, which currently provides:
	// * ObservedGeneration - the 'Generation' of the Service that was last
	//   processed by the controller.
	// * Conditions - the latest available observations of a resource's current
	//   state.
	// * SinkURI - the current active sink URI that has been configured for the
	//   Source.
	duckv1.SourceStatus `json:",inline"`

	// Total number of consumers actually running in the consumer group.
	// +optional
	Consumers int32 `json:"consumers,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// RedisStreamSourceList contains a list of RedisStreamSources.
type RedisStreamSourceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RedisStreamSource `json:"items"`
}

// GetStatus retrieves the status of the RedisStreamSource. Implements the KRShaped interface.
func (p *RedisStreamSource) GetStatus() *duckv1.Status {
	return &p.Status.Status
}
