/*
Copyright 2020 The Knative Authors

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

package v1beta1

import (
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/eventing-kafka/pkg/apis/duck/v1alpha1"
)

func (k *KafkaSource) GetKey() types.NamespacedName {
	return types.NamespacedName{
		Namespace: k.Namespace,
		Name:      k.Name,
	}
}

func (k *KafkaSource) GetVReplicas() int32 {
	if k.Spec.Consumers == nil {
		return 1
	}
	if k.Status.MaxAllowedVReplicas != nil {
		if *k.Spec.Consumers > *k.Status.MaxAllowedVReplicas {
			return *k.Status.MaxAllowedVReplicas
		}
	}
	return *k.Spec.Consumers
}

func (k *KafkaSource) GetPlacements() []v1alpha1.Placement {
	if k.Status.Placeable.Placements == nil {
		return nil
	}
	return k.Status.Placeable.Placements
}

func (k *KafkaSource) GetResourceVersion() string {
	return k.ObjectMeta.ResourceVersion
}
