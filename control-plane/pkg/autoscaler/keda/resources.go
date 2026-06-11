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

package keda

import (
	"fmt"
	"strconv"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/autoscaler"
	kedav1alpha1 "knative.dev/eventing-kafka-broker/third_party/pkg/apis/keda/v1alpha1"
)

var (
	KedaSchemeGroupVersion = schema.GroupVersion{Group: "keda.sh", Version: "v1alpha1"}
)

func GenerateScaledObject(obj metav1.Object, gvk schema.GroupVersionKind, scaleTarget *kedav1alpha1.ScaleTarget, triggers []kedav1alpha1.ScaleTriggers, aconfig autoscaler.AutoscalerConfig) (*kedav1alpha1.ScaledObject, error) {

	cooldownPeriod, err := GetInt32ValueFromMap(obj.GetAnnotations(), autoscaler.AutoscalingCooldownPeriodAnnotation, aconfig.AutoscalerDefaults[autoscaler.AutoscalingCooldownPeriodAnnotation])
	if err != nil {
		return nil, err
	}
	pollingInterval, err := GetInt32ValueFromMap(obj.GetAnnotations(), autoscaler.AutoscalingPollingIntervalAnnotation, aconfig.AutoscalerDefaults[autoscaler.AutoscalingPollingIntervalAnnotation])
	if err != nil {
		return nil, err
	}
	minReplicaCount, err := GetInt32ValueFromMap(obj.GetAnnotations(), autoscaler.AutoscalingMinScaleAnnotation, aconfig.AutoscalerDefaults[autoscaler.AutoscalingMinScaleAnnotation])
	if err != nil {
		return nil, err
	}
	maxReplicaCount, err := GetInt32ValueFromMap(obj.GetAnnotations(), autoscaler.AutoscalingMaxScaleAnnotation, aconfig.AutoscalerDefaults[autoscaler.AutoscalingMaxScaleAnnotation])
	if err != nil {
		return nil, err
	}

	return &kedav1alpha1.ScaledObject{
		ObjectMeta: metav1.ObjectMeta{
			Name:      GenerateScaledObjectName(obj),
			Namespace: obj.GetNamespace(),
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(obj, gvk),
			},
		},
		Spec: kedav1alpha1.ScaledObjectSpec{
			PollingInterval: pollingInterval,
			CooldownPeriod:  cooldownPeriod,
			MinReplicaCount: minReplicaCount,
			MaxReplicaCount: maxReplicaCount,
			ScaleTargetRef:  scaleTarget,
			Triggers:        triggers,
		},
	}, nil
}

func GenerateScaledObjectName(obj metav1.Object) string {
	return fmt.Sprintf("so-%s", string(obj.GetUID()))
}

func GetInt32ValueFromMap(dict map[string]string, key string, defaultValue int32) (*int32, error) {
	val, ok := dict[key]
	if !ok {
		return &defaultValue, nil
	}
	i, err := strconv.ParseInt(val, 10, 32)
	if err != nil {
		return nil, fmt.Errorf("Expected value for annotation: "+key+" should be integer but got "+val, err)
	}
	i32 := int32(i)
	return &i32, nil
}

func GetStringValueFromMap(dict map[string]string, key string, defaultValue string) (*string, error) {
	val, ok := dict[key]
	if !ok {
		return &defaultValue, nil
	}
	if val == "" {
		return nil, nil
	}
	return &val, nil
}
