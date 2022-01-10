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
	duckv1 "knative.dev/pkg/apis/duck/v1"

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
