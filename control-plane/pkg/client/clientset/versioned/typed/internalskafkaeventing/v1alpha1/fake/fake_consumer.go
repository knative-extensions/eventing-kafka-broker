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
	gentype "k8s.io/client-go/gentype"
	v1alpha1 "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internalskafkaeventing/v1alpha1"
	internalskafkaeventingv1alpha1 "knative.dev/eventing-kafka-broker/control-plane/pkg/client/clientset/versioned/typed/internalskafkaeventing/v1alpha1"
)

// fakeConsumers implements ConsumerInterface
type fakeConsumers struct {
	*gentype.FakeClientWithList[*v1alpha1.Consumer, *v1alpha1.ConsumerList]
	Fake *FakeInternalV1alpha1
}

func newFakeConsumers(fake *FakeInternalV1alpha1, namespace string) internalskafkaeventingv1alpha1.ConsumerInterface {
	return &fakeConsumers{
		gentype.NewFakeClientWithList[*v1alpha1.Consumer, *v1alpha1.ConsumerList](
			fake.Fake,
			namespace,
			v1alpha1.SchemeGroupVersion.WithResource("consumers"),
			v1alpha1.SchemeGroupVersion.WithKind("Consumer"),
			func() *v1alpha1.Consumer { return &v1alpha1.Consumer{} },
			func() *v1alpha1.ConsumerList { return &v1alpha1.ConsumerList{} },
			func(dst, src *v1alpha1.ConsumerList) { dst.ListMeta = src.ListMeta },
			func(list *v1alpha1.ConsumerList) []*v1alpha1.Consumer { return gentype.ToPointerSlice(list.Items) },
			func(list *v1alpha1.ConsumerList, items []*v1alpha1.Consumer) {
				list.Items = gentype.FromPointerSlice(items)
			},
		),
		fake,
	}
}
