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
	"knative.dev/eventing/pkg/apis/duck/v1alpha1"
)

func (ks *KafkaSource) GetKey() types.NamespacedName {
	return types.NamespacedName{
		Namespace: ks.Namespace,
		Name:      ks.Name,
	}
}

func (ks *KafkaSource) GetVReplicas() int32 {
	if ks.Spec.Consumers == nil {
		return 1
	}
	if ks.Status.MaxAllowedVReplicas != nil {
		if *ks.Spec.Consumers > *ks.Status.MaxAllowedVReplicas {
			return *ks.Status.MaxAllowedVReplicas
		}
	}
	return *ks.Spec.Consumers
}

func (ks *KafkaSource) GetPlacements() []v1alpha1.Placement {
	if ks.Status.Placements == nil {
		return nil
	}
	return ks.Status.Placements
}

func (ks *KafkaSource) GetResourceVersion() string {
	return ks.ResourceVersion
}
