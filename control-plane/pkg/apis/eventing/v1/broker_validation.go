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

package v1

import (
	"context"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"

	eventing "knative.dev/eventing/pkg/apis/eventing/v1"

	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/webhook/resourcesemantics"
)

type BrokerStub struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec eventing.BrokerSpec `json:"spec,omitempty"`

	// +optional
	Status eventing.BrokerStatus `json:"status,omitempty"`
}

var (
	// Check that Broker is resourcesemantics.GenericCRD.
	// We do this to make sure the validating/defaulting webhooks can operate on our stub.
	// If we don't do this stubbing, we will end up doing the defaulting/validating using the upstream Broker type.
	// However, we want to let the upstream webhook to do its job and not us. We only want to default/validate
	// the parts relevant to us.
	_ resourcesemantics.GenericCRD = (*BrokerStub)(nil)
)

type BrokerStubSpec struct {
	// Config is a KReference to the configuration that specifies
	// configuration options for this Broker. For example, this could be
	// a pointer to a ConfigMap.
	// +optional
	Config *duckv1.KReference `json:"config,omitempty"`
}

func (b *BrokerStub) Validate(context.Context) *apis.FieldError {
	// TODO: uncomment the following check when we backport namespaced broker into 1.6
	// if b.Annotations[eventing.BrokerClassAnnotationKey] != kafka.BrokerClass && b.Annotations[eventing.BrokerClassAnnotationKey] != kafka.NamespacedBrokerClass {
	if b.Annotations[eventing.BrokerClassAnnotationKey] != kafka.BrokerClass{
		// validation for the broker of other classes is done by the other webhooks
		return nil
	}

	if b.Spec.Config == nil {
		// validation of the Config requiredness is done by the upstream webhook and we want to let it do its job.
		// otherwise API server will say that our Kafka admission controller refused it, but it is a generic validation.
		return nil
	}

	// we specifically expect our broker class to use ConfigMap as the config type
	if strings.ToLower(b.Spec.Config.Kind) != "configmap" {
		return apis.ErrInvalidValue(b.Spec.Config.Kind, "kind", "Expected ConfigMap").ViaField("config").ViaField("spec")
	}

	// we specifically expect for our broker classes, that there's a namespace in the config
	if b.Spec.Config.Namespace == "" {
		return apis.ErrMissingField(b.Spec.Config.Namespace, "namespace").ViaField("config").ViaField("spec")
	}

	// TODO: uncomment the following check when we backport namespaced broker into 1.6
	// for the namespaced broker, we expect the config to be in the same namespace as the broker
	//if b.Annotations[eventing.BrokerClassAnnotationKey] == kafka.NamespacedBrokerClass && b.Spec.Config.Namespace != b.Namespace {
	//	return apis.ErrInvalidValue(b.Spec.Config.Namespace, "namespace", "Expected ConfigMap in same namespace with broker resource").ViaField("config").ViaField("spec")
	//}

	return nil
}

func (b *BrokerStub) SetDefaults(ctx context.Context) {
	// do nothing, we let the upstream webhook do its job
}

func (b *BrokerStub) DeepCopyObject() runtime.Object {
	if b == nil {
		return nil
	}

	out := &BrokerStub{
		TypeMeta: b.TypeMeta, //simple struct
	}

	b.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	b.Spec.DeepCopyInto(&out.Spec)
	b.Status.DeepCopyInto(&out.Status)

	return out
}
