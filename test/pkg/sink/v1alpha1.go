/*
 * Copyright 2020 The Knative Authors
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

package sink

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	testlib "knative.dev/eventing/test/lib"

	eventing "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/eventing/v1alpha1"
	eventingv1alpha1 "knative.dev/eventing-kafka-broker/control-plane/pkg/client/clientset/versioned/typed/eventing/v1alpha1"
	"knative.dev/eventing-kafka-broker/test/pkg/addressable"
)

func CreatorV1Alpha1(
	clientset eventingv1alpha1.EventingV1alpha1Interface,
	tracker *testlib.Tracker,
	spec eventing.KafkaSinkSpec) addressable.Creator {

	return func(namespacedName types.NamespacedName) (addressable.Addressable, error) {

		ctx := context.Background()
		ks := &eventing.KafkaSink{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespacedName.Namespace,
				Name:      namespacedName.Name,
			},
			Spec: spec,
		}

		ks, err := clientset.KafkaSinks(namespacedName.Namespace).Create(ctx, ks, metav1.CreateOptions{})
		if err != nil {
			return addressable.Addressable{}, err
		}
		tracker.AddObj(ks)

		return addressable.Addressable{
			NamespacedName: namespacedName,
			TypeMeta: metav1.TypeMeta{
				Kind:       eventing.Kind("KafkaSink").Kind,
				APIVersion: eventing.SchemeGroupVersion.String(),
			},
		}, nil
	}
}
