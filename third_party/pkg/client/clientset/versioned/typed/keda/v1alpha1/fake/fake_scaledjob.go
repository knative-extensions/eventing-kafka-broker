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
	v1alpha1 "knative.dev/eventing-kafka-broker/third_party/pkg/apis/keda/v1alpha1"
	kedav1alpha1 "knative.dev/eventing-kafka-broker/third_party/pkg/client/clientset/versioned/typed/keda/v1alpha1"
)

// fakeScaledJobs implements ScaledJobInterface
type fakeScaledJobs struct {
	*gentype.FakeClientWithList[*v1alpha1.ScaledJob, *v1alpha1.ScaledJobList]
	Fake *FakeKedaV1alpha1
}

func newFakeScaledJobs(fake *FakeKedaV1alpha1, namespace string) kedav1alpha1.ScaledJobInterface {
	return &fakeScaledJobs{
		gentype.NewFakeClientWithList[*v1alpha1.ScaledJob, *v1alpha1.ScaledJobList](
			fake.Fake,
			namespace,
			v1alpha1.SchemeGroupVersion.WithResource("scaledjobs"),
			v1alpha1.SchemeGroupVersion.WithKind("ScaledJob"),
			func() *v1alpha1.ScaledJob { return &v1alpha1.ScaledJob{} },
			func() *v1alpha1.ScaledJobList { return &v1alpha1.ScaledJobList{} },
			func(dst, src *v1alpha1.ScaledJobList) { dst.ListMeta = src.ListMeta },
			func(list *v1alpha1.ScaledJobList) []*v1alpha1.ScaledJob { return gentype.ToPointerSlice(list.Items) },
			func(list *v1alpha1.ScaledJobList, items []*v1alpha1.ScaledJob) {
				list.Items = gentype.FromPointerSlice(items)
			},
		),
		fake,
	}
}
