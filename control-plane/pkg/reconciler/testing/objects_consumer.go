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

package testing

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"
	"knative.dev/eventing-kafka/pkg/apis/bindings/v1beta1"
	v1 "knative.dev/eventing/pkg/apis/eventing/v1"
	duckv1 "knative.dev/pkg/apis/duck/v1"

	internals "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing"
	kafkainternals "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing/v1alpha1"
)

const (
	ConsumerNamePrefix = "test-cg"
	ConsumerNamespace  = "test-cg-ns"
)

type ConsumerOption func(cg *kafkainternals.Consumer)

type ConsumerSpecOption func(c *kafkainternals.ConsumerSpec)

func NewConsumer(ordinal int, opts ...ConsumerOption) *kafkainternals.Consumer {

	c := &kafkainternals.Consumer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%d", ConsumerNamePrefix, ordinal),
			Namespace: ConsumerNamespace,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion:         kafkainternals.SchemeGroupVersion.String(),
					Kind:               kafkainternals.ConsumerGroupGroupVersionKind.Kind,
					Name:               ConsumerGroupName,
					Controller:         pointer.BoolPtr(true),
					BlockOwnerDeletion: pointer.BoolPtr(true),
				},
			},
			Labels: ConsumerLabels,
		},
		Spec: kafkainternals.ConsumerSpec{
			Subscriber: duckv1.Destination{
				Ref: &duckv1.KReference{
					Kind:       "Service",
					Namespace:  ServiceNamespace,
					Name:       ServiceName,
					APIVersion: "v1",
				},
			},
		},
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

func NewConsumerSpec(opts ...ConsumerSpecOption) kafkainternals.ConsumerSpec {
	spec := &kafkainternals.ConsumerSpec{}

	for _, opt := range opts {
		opt(spec)
	}

	return *spec
}

func NewConsumerSpecDelivery(order internals.DeliveryOrdering) *kafkainternals.DeliverySpec {
	return &kafkainternals.DeliverySpec{
		Ordering: order,
	}
}

func NewConsumerSpecAuth() *kafkainternals.Auth {
	return &kafkainternals.Auth{
		NetSpec: &v1beta1.KafkaNetSpec{},
	}
}

func NewConsumerSpecFilters() *kafkainternals.Filters {
	return &kafkainternals.Filters{
		Filter: &v1.TriggerFilter{},
	}
}

func ConsumerSpec(spec kafkainternals.ConsumerSpec) ConsumerOption {
	return func(cg *kafkainternals.Consumer) {
		cg.Spec = spec
	}
}

func ConsumerTopics(topics ...string) ConsumerSpecOption {
	return func(c *kafkainternals.ConsumerSpec) {
		c.Topics = topics
	}
}

func ConsumerPlacement(pb kafkainternals.PodBind) ConsumerSpecOption {
	return func(c *kafkainternals.ConsumerSpec) {
		c.PodBind = &pb
	}
}

type ConsumerConfigsOption func(configs *kafkainternals.ConsumerConfigs)

func ConsumerConfigs(opts ...ConsumerConfigsOption) ConsumerSpecOption {
	return func(c *kafkainternals.ConsumerSpec) {
		configs := &kafkainternals.ConsumerConfigs{Configs: map[string]string{}}

		for _, opt := range opts {
			opt(configs)
		}

		c.Configs = *configs
	}
}

func ConsumerBootstrapServersConfig(s string) ConsumerConfigsOption {
	return func(configs *kafkainternals.ConsumerConfigs) {
		configs.Configs["bootstrap.servers"] = s
	}
}

func ConsumerGroupIdConfig(s string) ConsumerConfigsOption {
	return func(configs *kafkainternals.ConsumerConfigs) {
		configs.Configs["group.id"] = s
	}
}

func ConsumerVReplicas(vreplicas int32) ConsumerSpecOption {
	return func(c *kafkainternals.ConsumerSpec) {
		c.VReplicas = &vreplicas
	}
}

func ConsumerAuth(auth *kafkainternals.Auth) ConsumerSpecOption {
	return func(c *kafkainternals.ConsumerSpec) {
		c.Auth = auth
	}
}

func ConsumerDelivery(delivery *kafkainternals.DeliverySpec) ConsumerSpecOption {
	return func(c *kafkainternals.ConsumerSpec) {
		c.Delivery = delivery
	}
}

func ConsumerSubscriber(dest duckv1.Destination) ConsumerSpecOption {
	return func(c *kafkainternals.ConsumerSpec) {
		c.Subscriber = dest
	}
}

func ConsumerCloudEventOverrides(ce *duckv1.CloudEventOverrides) ConsumerSpecOption {
	return func(c *kafkainternals.ConsumerSpec) {
		c.CloudEventOverrides = ce
	}
}

func ConsumerFilters(filters *kafkainternals.Filters) ConsumerSpecOption {
	return func(c *kafkainternals.ConsumerSpec) {
		c.Filters = filters
	}
}

func ConsumerForTrigger() ConsumerGroupOption {
	return func(c *kafkainternals.ConsumerGroup) {
		c.OwnerReferences = append(c.OwnerReferences, metav1.OwnerReference{
			APIVersion: "eventing.knative.dev/v1",
			Kind:       "Trigger",
			Name:       "test-trigger",
			UID:        TriggerUUID,
		})
	}
}
