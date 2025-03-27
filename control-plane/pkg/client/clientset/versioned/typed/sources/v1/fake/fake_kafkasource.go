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

// Code generated by client-gen. DO NOT EDIT.

package fake

import (
	context "context"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	gentype "k8s.io/client-go/gentype"
	testing "k8s.io/client-go/testing"
	v1 "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/sources/v1"
	sourcesv1 "knative.dev/eventing-kafka-broker/control-plane/pkg/client/clientset/versioned/typed/sources/v1"
)

// fakeKafkaSources implements KafkaSourceInterface
type fakeKafkaSources struct {
	*gentype.FakeClientWithList[*v1.KafkaSource, *v1.KafkaSourceList]
	Fake *FakeSourcesV1
}

func newFakeKafkaSources(fake *FakeSourcesV1, namespace string) sourcesv1.KafkaSourceInterface {
	return &fakeKafkaSources{
		gentype.NewFakeClientWithList[*v1.KafkaSource, *v1.KafkaSourceList](
			fake.Fake,
			namespace,
			v1.SchemeGroupVersion.WithResource("kafkasources"),
			v1.SchemeGroupVersion.WithKind("KafkaSource"),
			func() *v1.KafkaSource { return &v1.KafkaSource{} },
			func() *v1.KafkaSourceList { return &v1.KafkaSourceList{} },
			func(dst, src *v1.KafkaSourceList) { dst.ListMeta = src.ListMeta },
			func(list *v1.KafkaSourceList) []*v1.KafkaSource { return gentype.ToPointerSlice(list.Items) },
			func(list *v1.KafkaSourceList, items []*v1.KafkaSource) { list.Items = gentype.FromPointerSlice(items) },
		),
		fake,
	}
}

// GetScale takes name of the kafkaSource, and returns the corresponding scale object, and an error if there is any.
func (c *fakeKafkaSources) GetScale(ctx context.Context, kafkaSourceName string, options metav1.GetOptions) (result *autoscalingv1.Scale, err error) {
	emptyResult := &autoscalingv1.Scale{}
	obj, err := c.Fake.
		Invokes(testing.NewGetSubresourceActionWithOptions(c.Resource(), c.Namespace(), "scale", kafkaSourceName, options), emptyResult)

	if obj == nil {
		return emptyResult, err
	}
	return obj.(*autoscalingv1.Scale), err
}

// UpdateScale takes the representation of a scale and updates it. Returns the server's representation of the scale, and an error, if there is any.
func (c *fakeKafkaSources) UpdateScale(ctx context.Context, kafkaSourceName string, scale *autoscalingv1.Scale, opts metav1.UpdateOptions) (result *autoscalingv1.Scale, err error) {
	emptyResult := &autoscalingv1.Scale{}
	obj, err := c.Fake.
		Invokes(testing.NewUpdateSubresourceActionWithOptions(c.Resource(), "scale", c.Namespace(), scale, opts), &autoscalingv1.Scale{})

	if obj == nil {
		return emptyResult, err
	}
	return obj.(*autoscalingv1.Scale), err
}
