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

package internalskafkaeventing

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
)

const (
	// ConfigMapVolumeName is the volume name of the data plane ConfigMap
	ConfigMapVolumeName = "kafka-resources"

	DispatcherVolumeName = "contract-resources"

	DataPlanePodKindLabelKey    = "app.kubernetes.io/kind"
	DispatcherPodKindLabelValue = "kafka-dispatcher"

	DispatcherLabelSelectorStr = DataPlanePodKindLabelKey + "=" + DispatcherPodKindLabelValue
)

func ConfigMapNameFromPod(p *corev1.Pod) (string, error) {
	var vDp *corev1.Volume
	for i, v := range p.Spec.Volumes {
		if v.Name == DispatcherVolumeName {
			vDp = &p.Spec.Volumes[i]
			break
		}
	}
	if vDp == nil {
		return "", fmt.Errorf("failed to get data plane volume %s in pod %s/%s", ConfigMapVolumeName, p.GetNamespace(), p.GetName())
	}

	// If the volume has a ConfigMap reference, use that name
	if vDp.ConfigMap != nil && vDp.ConfigMap.Name != "" {
		return vDp.ConfigMap.Name, nil
	}

	// Fallback: If the volume exists but doesn't have a ConfigMap reference yet
	// (e.g., it's still emptyDir because the webhook hasn't processed it yet),
	// use the pod name as the ConfigMap name. This matches the webhook's behavior
	// which sets the ConfigMap name to the pod name.
	return p.GetName(), nil
}
