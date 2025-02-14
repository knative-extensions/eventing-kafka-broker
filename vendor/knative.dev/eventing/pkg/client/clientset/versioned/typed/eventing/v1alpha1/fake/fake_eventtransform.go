/*
Copyright 2021 The Knative Authors

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

// Code generated by client-gen. DO NOT EDIT.

package fake

import (
	"context"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	labels "k8s.io/apimachinery/pkg/labels"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	testing "k8s.io/client-go/testing"
	v1alpha1 "knative.dev/eventing/pkg/apis/eventing/v1alpha1"
)

// FakeEventTransforms implements EventTransformInterface
type FakeEventTransforms struct {
	Fake *FakeEventingV1alpha1
	ns   string
}

var eventtransformsResource = v1alpha1.SchemeGroupVersion.WithResource("eventtransforms")

var eventtransformsKind = v1alpha1.SchemeGroupVersion.WithKind("EventTransform")

// Get takes name of the eventTransform, and returns the corresponding eventTransform object, and an error if there is any.
func (c *FakeEventTransforms) Get(ctx context.Context, name string, options v1.GetOptions) (result *v1alpha1.EventTransform, err error) {
	emptyResult := &v1alpha1.EventTransform{}
	obj, err := c.Fake.
		Invokes(testing.NewGetActionWithOptions(eventtransformsResource, c.ns, name, options), emptyResult)

	if obj == nil {
		return emptyResult, err
	}
	return obj.(*v1alpha1.EventTransform), err
}

// List takes label and field selectors, and returns the list of EventTransforms that match those selectors.
func (c *FakeEventTransforms) List(ctx context.Context, opts v1.ListOptions) (result *v1alpha1.EventTransformList, err error) {
	emptyResult := &v1alpha1.EventTransformList{}
	obj, err := c.Fake.
		Invokes(testing.NewListActionWithOptions(eventtransformsResource, eventtransformsKind, c.ns, opts), emptyResult)

	if obj == nil {
		return emptyResult, err
	}

	label, _, _ := testing.ExtractFromListOptions(opts)
	if label == nil {
		label = labels.Everything()
	}
	list := &v1alpha1.EventTransformList{ListMeta: obj.(*v1alpha1.EventTransformList).ListMeta}
	for _, item := range obj.(*v1alpha1.EventTransformList).Items {
		if label.Matches(labels.Set(item.Labels)) {
			list.Items = append(list.Items, item)
		}
	}
	return list, err
}

// Watch returns a watch.Interface that watches the requested eventTransforms.
func (c *FakeEventTransforms) Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error) {
	return c.Fake.
		InvokesWatch(testing.NewWatchActionWithOptions(eventtransformsResource, c.ns, opts))

}

// Create takes the representation of a eventTransform and creates it.  Returns the server's representation of the eventTransform, and an error, if there is any.
func (c *FakeEventTransforms) Create(ctx context.Context, eventTransform *v1alpha1.EventTransform, opts v1.CreateOptions) (result *v1alpha1.EventTransform, err error) {
	emptyResult := &v1alpha1.EventTransform{}
	obj, err := c.Fake.
		Invokes(testing.NewCreateActionWithOptions(eventtransformsResource, c.ns, eventTransform, opts), emptyResult)

	if obj == nil {
		return emptyResult, err
	}
	return obj.(*v1alpha1.EventTransform), err
}

// Update takes the representation of a eventTransform and updates it. Returns the server's representation of the eventTransform, and an error, if there is any.
func (c *FakeEventTransforms) Update(ctx context.Context, eventTransform *v1alpha1.EventTransform, opts v1.UpdateOptions) (result *v1alpha1.EventTransform, err error) {
	emptyResult := &v1alpha1.EventTransform{}
	obj, err := c.Fake.
		Invokes(testing.NewUpdateActionWithOptions(eventtransformsResource, c.ns, eventTransform, opts), emptyResult)

	if obj == nil {
		return emptyResult, err
	}
	return obj.(*v1alpha1.EventTransform), err
}

// UpdateStatus was generated because the type contains a Status member.
// Add a +genclient:noStatus comment above the type to avoid generating UpdateStatus().
func (c *FakeEventTransforms) UpdateStatus(ctx context.Context, eventTransform *v1alpha1.EventTransform, opts v1.UpdateOptions) (result *v1alpha1.EventTransform, err error) {
	emptyResult := &v1alpha1.EventTransform{}
	obj, err := c.Fake.
		Invokes(testing.NewUpdateSubresourceActionWithOptions(eventtransformsResource, "status", c.ns, eventTransform, opts), emptyResult)

	if obj == nil {
		return emptyResult, err
	}
	return obj.(*v1alpha1.EventTransform), err
}

// Delete takes name of the eventTransform and deletes it. Returns an error if one occurs.
func (c *FakeEventTransforms) Delete(ctx context.Context, name string, opts v1.DeleteOptions) error {
	_, err := c.Fake.
		Invokes(testing.NewDeleteActionWithOptions(eventtransformsResource, c.ns, name, opts), &v1alpha1.EventTransform{})

	return err
}

// DeleteCollection deletes a collection of objects.
func (c *FakeEventTransforms) DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error {
	action := testing.NewDeleteCollectionActionWithOptions(eventtransformsResource, c.ns, opts, listOpts)

	_, err := c.Fake.Invokes(action, &v1alpha1.EventTransformList{})
	return err
}

// Patch applies the patch and returns the patched eventTransform.
func (c *FakeEventTransforms) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *v1alpha1.EventTransform, err error) {
	emptyResult := &v1alpha1.EventTransform{}
	obj, err := c.Fake.
		Invokes(testing.NewPatchSubresourceActionWithOptions(eventtransformsResource, c.ns, name, pt, data, opts, subresources...), emptyResult)

	if obj == nil {
		return emptyResult, err
	}
	return obj.(*v1alpha1.EventTransform), err
}
