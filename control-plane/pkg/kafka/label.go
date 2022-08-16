/*
 * Copyright 2022 The Knative Authors
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

package kafka

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	NamespacedBrokerDataplaneLabelKey   = "eventing.knative.dev/namespaced"
	NamespacedBrokerDataplaneLabelValue = "true"
)

func NamespacedDataplaneLabelConfigmapOption(cm *corev1.ConfigMap) {
	if len(cm.Labels) == 0 {
		cm.Labels = make(map[string]string, 1)
	}
	cm.Labels[NamespacedBrokerDataplaneLabelKey] = NamespacedBrokerDataplaneLabelValue
}

func FilterAny(funcs ...func(obj interface{}) bool) func(obj interface{}) bool {
	return func(obj interface{}) bool {
		for _, f := range funcs {
			if ok := f(obj); ok {
				return true
			}
		}
		return false
	}
}

func FilterWithLabel(key, value string) func(obj interface{}) bool {
	return func(obj interface{}) bool {
		if object, ok := obj.(metav1.Object); ok {
			labels := object.GetLabels()
			v, exists := labels[key]
			if !exists {
				return false
			}
			return v == value
		}
		return false

	}
}
