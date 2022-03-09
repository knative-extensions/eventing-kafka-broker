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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/webhook"
	"knative.dev/pkg/webhook/resourcesemantics/validation"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
)

type BrokerPartial struct {
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`
	// Spec defines the desired state of the Broker.
	Spec BrokerPartialSpec `json:"spec,omitempty"`
}

type BrokerPartialSpec struct {
	// Config is a KReference to the configuration that specifies
	// configuration options for this Broker. For example, this could be
	// a pointer to a ConfigMap.
	// +optional
	Config *duckv1.KReference `json:"config,omitempty"`
}

func (b BrokerPartialSpec) Validate(context.Context) error {
	if b.Config == nil {
		return apis.ErrMissingField("config").ViaField("spec")
	}
	if strings.ToLower(b.Config.Kind) != "configmap" {
		return apis.ErrInvalidValue(b.Config.Kind, "kind", "Expected ConfigMap").ViaField("config").ViaField("spec")
	}
	return nil
}

func validateBrokerFromUnstructured(ctx context.Context, unstructured *unstructured.Unstructured) error {
	broker := BrokerPartial{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(unstructured.UnstructuredContent(), &broker); err != nil {
		return err
	}
	if class, ok := broker.Annotations[eventing.BrokerClassAnnotationKey]; !ok || class != kafka.BrokerClass {
		return nil
	}
	return broker.Spec.Validate(ctx)
}

func BrokerValidationCallback() validation.Callback {
	return validation.NewCallback(validateBrokerFromUnstructured, webhook.Create, webhook.Update)
}
