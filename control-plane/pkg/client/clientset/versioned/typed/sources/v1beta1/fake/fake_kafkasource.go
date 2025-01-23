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
	"context"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	labels "k8s.io/apimachinery/pkg/labels"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	testing "k8s.io/client-go/testing"
	v1beta1 "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/sources/v1beta1"
)

// FakeKafkaSources implements KafkaSourceInterface
type FakeKafkaSources struct {
	Fake *FakeSourcesV1beta1
	ns   string
}

var kafkasourcesResource = v1beta1.SchemeGroupVersion.WithResource("kafkasources")

var kafkasourcesKind = v1beta1.SchemeGroupVersion.WithKind("KafkaSource")

// Get takes name of the kafkaSource, and returns the corresponding kafkaSource object, and an error if there is any.
func (c *FakeKafkaSources) Get(ctx context.Context, name string, options v1.GetOptions) (result *v1beta1.KafkaSource, err error) {
	emptyResult := &v1beta1.KafkaSource{}
	obj, err := c.Fake.
		Invokes(testing.NewGetActionWithOptions(kafkasourcesResource, c.ns, name, options), emptyResult)

	if obj == nil {
		return emptyResult, err
	}
	return obj.(*v1beta1.KafkaSource), err
}

// List takes label and field selectors, and returns the list of KafkaSources that match those selectors.
func (c *FakeKafkaSources) List(ctx context.Context, opts v1.ListOptions) (result *v1beta1.KafkaSourceList, err error) {
	emptyResult := &v1beta1.KafkaSourceList{}
	obj, err := c.Fake.
		Invokes(testing.NewListActionWithOptions(kafkasourcesResource, kafkasourcesKind, c.ns, opts), emptyResult)

	if obj == nil {
		return emptyResult, err
	}

	label, _, _ := testing.ExtractFromListOptions(opts)
	if label == nil {
		label = labels.Everything()
	}
	list := &v1beta1.KafkaSourceList{ListMeta: obj.(*v1beta1.KafkaSourceList).ListMeta}
	for _, item := range obj.(*v1beta1.KafkaSourceList).Items {
		if label.Matches(labels.Set(item.Labels)) {
			list.Items = append(list.Items, item)
		}
	}
	return list, err
}

// Watch returns a watch.Interface that watches the requested kafkaSources.
func (c *FakeKafkaSources) Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error) {
	return c.Fake.
		InvokesWatch(testing.NewWatchActionWithOptions(kafkasourcesResource, c.ns, opts))

}

// Create takes the representation of a kafkaSource and creates it.  Returns the server's representation of the kafkaSource, and an error, if there is any.
func (c *FakeKafkaSources) Create(ctx context.Context, kafkaSource *v1beta1.KafkaSource, opts v1.CreateOptions) (result *v1beta1.KafkaSource, err error) {
	emptyResult := &v1beta1.KafkaSource{}
	obj, err := c.Fake.
		Invokes(testing.NewCreateActionWithOptions(kafkasourcesResource, c.ns, kafkaSource, opts), emptyResult)

	if obj == nil {
		return emptyResult, err
	}
	return obj.(*v1beta1.KafkaSource), err
}

// Update takes the representation of a kafkaSource and updates it. Returns the server's representation of the kafkaSource, and an error, if there is any.
func (c *FakeKafkaSources) Update(ctx context.Context, kafkaSource *v1beta1.KafkaSource, opts v1.UpdateOptions) (result *v1beta1.KafkaSource, err error) {
	emptyResult := &v1beta1.KafkaSource{}
	obj, err := c.Fake.
		Invokes(testing.NewUpdateActionWithOptions(kafkasourcesResource, c.ns, kafkaSource, opts), emptyResult)

	if obj == nil {
		return emptyResult, err
	}
	return obj.(*v1beta1.KafkaSource), err
}

// UpdateStatus was generated because the type contains a Status member.
// Add a +genclient:noStatus comment above the type to avoid generating UpdateStatus().
func (c *FakeKafkaSources) UpdateStatus(ctx context.Context, kafkaSource *v1beta1.KafkaSource, opts v1.UpdateOptions) (result *v1beta1.KafkaSource, err error) {
	emptyResult := &v1beta1.KafkaSource{}
	obj, err := c.Fake.
		Invokes(testing.NewUpdateSubresourceActionWithOptions(kafkasourcesResource, "status", c.ns, kafkaSource, opts), emptyResult)

	if obj == nil {
		return emptyResult, err
	}
	return obj.(*v1beta1.KafkaSource), err
}

// Delete takes name of the kafkaSource and deletes it. Returns an error if one occurs.
func (c *FakeKafkaSources) Delete(ctx context.Context, name string, opts v1.DeleteOptions) error {
	_, err := c.Fake.
		Invokes(testing.NewDeleteActionWithOptions(kafkasourcesResource, c.ns, name, opts), &v1beta1.KafkaSource{})

	return err
}

// DeleteCollection deletes a collection of objects.
func (c *FakeKafkaSources) DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error {
	action := testing.NewDeleteCollectionActionWithOptions(kafkasourcesResource, c.ns, opts, listOpts)

	_, err := c.Fake.Invokes(action, &v1beta1.KafkaSourceList{})
	return err
}

// Patch applies the patch and returns the patched kafkaSource.
func (c *FakeKafkaSources) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *v1beta1.KafkaSource, err error) {
	emptyResult := &v1beta1.KafkaSource{}
	obj, err := c.Fake.
		Invokes(testing.NewPatchSubresourceActionWithOptions(kafkasourcesResource, c.ns, name, pt, data, opts, subresources...), emptyResult)

	if obj == nil {
		return emptyResult, err
	}
	return obj.(*v1beta1.KafkaSource), err
}

// GetScale takes name of the kafkaSource, and returns the corresponding scale object, and an error if there is any.
func (c *FakeKafkaSources) GetScale(ctx context.Context, kafkaSourceName string, options v1.GetOptions) (result *autoscalingv1.Scale, err error) {
	emptyResult := &autoscalingv1.Scale{}
	obj, err := c.Fake.
		Invokes(testing.NewGetSubresourceActionWithOptions(kafkasourcesResource, c.ns, "scale", kafkaSourceName, options), emptyResult)

	if obj == nil {
		return emptyResult, err
	}
	return obj.(*autoscalingv1.Scale), err
}

// UpdateScale takes the representation of a scale and updates it. Returns the server's representation of the scale, and an error, if there is any.
func (c *FakeKafkaSources) UpdateScale(ctx context.Context, kafkaSourceName string, scale *autoscalingv1.Scale, opts v1.UpdateOptions) (result *autoscalingv1.Scale, err error) {
	emptyResult := &autoscalingv1.Scale{}
	obj, err := c.Fake.
		Invokes(testing.NewUpdateSubresourceActionWithOptions(kafkasourcesResource, "scale", c.ns, scale, opts), &autoscalingv1.Scale{})

	if obj == nil {
		return emptyResult, err
	}
	return obj.(*autoscalingv1.Scale), err
}
