/*
 * Copyright 2023 The Knative Authors
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

package resources

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	duckv1alpha1 "knative.dev/pkg/apis/duck/v1alpha1"

	kafkabindingv1beta1 "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/bindings/v1beta1"
	"knative.dev/pkg/tracker"
)

func KafkaBindingV1Beta1(bootstrapServer string, ref *tracker.Reference) *kafkabindingv1beta1.KafkaBinding {
	return &kafkabindingv1beta1.KafkaBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-kafka-binding",
		},
		Spec: kafkabindingv1beta1.KafkaBindingSpec{
			KafkaAuthSpec: kafkabindingv1beta1.KafkaAuthSpec{
				BootstrapServers: []string{bootstrapServer},
			},
			BindingSpec: duckv1alpha1.BindingSpec{
				Subject: *ref,
			},
		},
	}
}
