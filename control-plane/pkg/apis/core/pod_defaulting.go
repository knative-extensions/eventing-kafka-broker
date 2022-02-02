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

package core

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"knative.dev/pkg/system"
	"knative.dev/pkg/webhook"
	"knative.dev/pkg/webhook/resourcesemantics/defaulting"

	kafkainternals "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing/v1alpha1"
)

// DispatcherPodsDefaulting returns the defaulting Callback for dispatcher pods
func DispatcherPodsDefaulting() defaulting.Callback {
	return defaulting.NewCallback(
		podDefaultingCallback(func(u *unstructured.Unstructured) uuid.UUID { return uuid.New() }),
		webhook.Create,
	)
}

// generatorFunc generates uuid to set as ConfigMap names.
type generatorFunc func(*unstructured.Unstructured) uuid.UUID

func podDefaultingCallback(generator generatorFunc) defaulting.CallbackFunc {
	namespace := system.Namespace()
	return func(ctx context.Context, unstructured *unstructured.Unstructured) error {
		if unstructured.GetNamespace() != namespace {
			return nil
		}

		var cmName string
		if unstructured.GetName() == "" {
			cmName = generator(unstructured).String()
		} else {
			cmName = unstructured.GetName()
		}

		volume := &corev1.Volume{
			Name: kafkainternals.DispatcherVolumeName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: cmName},
				},
			},
		}

		unstrVolume, err := runtime.DefaultUnstructuredConverter.ToUnstructured(volume)
		if err != nil {
			return fmt.Errorf("failed to convert volume struct to unstructured object: %w", err)
		}

		pod := unstructured.Object
		spec := pod["spec"].(map[string]interface{})

		volumes, ok := spec["volumes"].([]interface{})
		if !ok {
			volumes = []interface{}{}
			spec["volumes"] = volumes
		}

		found := false
		for k, v := range volumes {
			vt := v.(map[string]interface{})
			if vt["name"].(string) == "contract-resources" {
				found = true
				volumes[k] = unstrVolume
			}
		}

		if !found {
			spec["volumes"] = append(volumes, unstrVolume)
		}

		unstructured.Object = pod

		return nil
	}
}
